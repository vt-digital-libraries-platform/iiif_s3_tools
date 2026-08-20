package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func strp(s string) *string { return &s }

func TestFetchDDBItem_GetItem(t *testing.T) {
	origRunner := cliRunner
	defer func() { cliRunner = origRunner }()

	var gotArgs []string
	cliRunner = func(name string, args ...string) ([]byte, error) {
		gotArgs = args
		return []byte(`{"Item":{"id":{"S":"abc123"},"archiveOptions":{"S":"{\"assets\":{\"x3d_config\":\"https://example.com/model.x3d\"}}"}}}`), nil
	}

	cfg := &Config{
		LookupMode:       "get_item",
		PartitionKeyAttr: "id",
		TableName:        "my-table",
		Region:           "us-east-1",
	}
	item, err := fetchDDBItem(cfg, "abc123")
	if err != nil {
		t.Fatalf("fetchDDBItem: %v", err)
	}
	if item["id"].S == nil || *item["id"].S != "abc123" {
		t.Errorf("unexpected id attr: %+v", item["id"])
	}

	joined := strings.Join(gotArgs, " ")
	if !strings.Contains(joined, "get-item") {
		t.Errorf("expected get-item subcommand, got args: %v", gotArgs)
	}
	if !strings.Contains(joined, `"id":{"S":"abc123"}`) {
		t.Errorf("expected key JSON to reference id=abc123, got args: %v", gotArgs)
	}
}

func TestFetchDDBItem_GetItem_NotFound(t *testing.T) {
	origRunner := cliRunner
	defer func() { cliRunner = origRunner }()
	cliRunner = func(name string, args ...string) ([]byte, error) {
		return []byte(`{}`), nil // no "Item" key: not found
	}

	cfg := &Config{LookupMode: "get_item", PartitionKeyAttr: "id"}
	_, err := fetchDDBItem(cfg, "missing")
	if err == nil {
		t.Fatal("expected error for missing item, got nil")
	}
}

func TestFetchDDBItem_Scan(t *testing.T) {
	origRunner := cliRunner
	defer func() { cliRunner = origRunner }()

	var gotArgs []string
	cliRunner = func(name string, args ...string) ([]byte, error) {
		gotArgs = args
		return []byte(`{"Items":[{"identifier":{"S":"368a8114"},"archiveOptions":{"S":"{}"}}]}`), nil
	}

	cfg := &Config{
		LookupMode:     "scan",
		IdentifierAttr: "identifier",
		TableName:      "my-table",
		Region:         "us-east-1",
	}
	item, err := fetchDDBItem(cfg, "368a8114")
	if err != nil {
		t.Fatalf("fetchDDBItem: %v", err)
	}
	if item["identifier"].S == nil || *item["identifier"].S != "368a8114" {
		t.Errorf("unexpected identifier attr: %+v", item["identifier"])
	}
	joined := strings.Join(gotArgs, " ")
	if !strings.Contains(joined, "scan") {
		t.Errorf("expected scan subcommand, got args: %v", gotArgs)
	}
}

func TestFetchDDBItem_IdentifierPrefix(t *testing.T) {
	origRunner := cliRunner
	defer func() { cliRunner = origRunner }()

	var gotArgs []string
	cliRunner = func(name string, args ...string) ([]byte, error) {
		gotArgs = args
		return []byte(`{"Items":[{"custom_key":{"S":"ark:/53696/368a8114"}}]}`), nil
	}

	cfg := &Config{
		LookupMode:       "scan",
		IdentifierAttr:   "custom_key",
		IdentifierPrefix: "ark:/53696/",
	}
	if _, err := fetchDDBItem(cfg, "368a8114"); err != nil {
		t.Fatalf("fetchDDBItem: %v", err)
	}
	joined := strings.Join(gotArgs, " ")
	if !strings.Contains(joined, `ark:/53696/368a8114`) {
		t.Errorf("expected prefixed identifier in args, got: %v", gotArgs)
	}
}

func TestFetchDDBItem_InvalidLookupMode(t *testing.T) {
	cfg := &Config{LookupMode: "bogus"}
	if _, err := fetchDDBItem(cfg, "x"); err == nil {
		t.Fatal("expected error for invalid lookup_mode, got nil")
	}
}

func TestExtractX3DConfigURL(t *testing.T) {
	item := ddbItem{
		"archiveOptions": ddbAttr{S: strp(`{"assets":{"x3d_config":"https://cdn.example.com/models/foo.x3d","other":"ignored"}}`)},
	}
	url, err := extractX3DConfigURL(item, "archiveOptions")
	if err != nil {
		t.Fatalf("extractX3DConfigURL: %v", err)
	}
	if url != "https://cdn.example.com/models/foo.x3d" {
		t.Errorf("got %q", url)
	}
}

func TestExtractX3DConfigURL_MissingField(t *testing.T) {
	item := ddbItem{}
	if _, err := extractX3DConfigURL(item, "archiveOptions"); err == nil {
		t.Fatal("expected error for missing archiveOptions attribute, got nil")
	}
}

func TestExtractX3DConfigURL_MissingX3DConfig(t *testing.T) {
	item := ddbItem{
		"archiveOptions": ddbAttr{S: strp(`{"assets":{"env_config":"https://example.com/env.x3d"}}`)},
	}
	if _, err := extractX3DConfigURL(item, "archiveOptions"); err == nil {
		t.Fatal("expected error for missing assets.x3d_config, got nil")
	}
}

func TestExtractX3DConfigURL_InvalidJSON(t *testing.T) {
	item := ddbItem{
		"archiveOptions": ddbAttr{S: strp(`not json`)},
	}
	if _, err := extractX3DConfigURL(item, "archiveOptions"); err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestResolveRelativeURL(t *testing.T) {
	got, err := resolveRelativeURL(
		"https://cdn.example.com/federated/testss/VTEC1/3d/LowRes_VTEC1.x3d",
		"LowRes_VTEC1.png",
	)
	if err != nil {
		t.Fatalf("resolveRelativeURL: %v", err)
	}
	want := "https://cdn.example.com/federated/testss/VTEC1/3d/LowRes_VTEC1.png"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestDownloadFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "file-contents")
	}))
	defer srv.Close()

	dir := t.TempDir()
	dest := filepath.Join(dir, "sub", "out.txt")

	if err := downloadFile(srv.Client(), srv.URL, dest); err != nil {
		t.Fatalf("downloadFile: %v", err)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("reading downloaded file: %v", err)
	}
	if string(got) != "file-contents" {
		t.Errorf("got %q", got)
	}
}

func TestDownloadFile_SkipsExisting(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		fmt.Fprint(w, "should-not-be-fetched")
	}))
	defer srv.Close()

	dir := t.TempDir()
	dest := filepath.Join(dir, "out.txt")
	if err := os.WriteFile(dest, []byte("already-there"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := downloadFile(srv.Client(), srv.URL, dest); err != nil {
		t.Fatalf("downloadFile: %v", err)
	}
	if calls != 0 {
		t.Errorf("expected no HTTP call for existing file, got %d", calls)
	}
	got, _ := os.ReadFile(dest)
	if string(got) != "already-there" {
		t.Errorf("existing file was overwritten: %q", got)
	}
}

func TestDownloadFile_NonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}))
	defer srv.Close()

	dir := t.TempDir()
	err := downloadFile(srv.Client(), srv.URL, filepath.Join(dir, "out.txt"))
	if err == nil {
		t.Fatal("expected error for 404 response, got nil")
	}
}

func TestFetchModel_EndToEnd(t *testing.T) {
	origRunner := cliRunner
	defer func() { cliRunner = origRunner }()

	x3dContent := readTestdata(t, "sample.x3d") // references ImageTexture url="sample.png"

	var assetSrv *httptest.Server
	assetSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/models/sample.x3d":
			fmt.Fprint(w, x3dContent)
		case "/models/sample.png":
			fmt.Fprint(w, "fake-png-bytes")
		default:
			http.NotFound(w, r)
		}
	}))
	defer assetSrv.Close()

	cliRunner = func(name string, args ...string) ([]byte, error) {
		archiveOptions := fmt.Sprintf(`{"assets":{"x3d_config":%q}}`, assetSrv.URL+"/models/sample.x3d")
		out := fmt.Sprintf(`{"Item":{"id":{"S":"abc"},"archiveOptions":{"S":%q}}}`, archiveOptions)
		return []byte(out), nil
	}

	dir := t.TempDir()
	cfg := &Config{
		InputDir:            dir,
		LookupMode:          "get_item",
		PartitionKeyAttr:    "id",
		ArchiveOptionsField: "archiveOptions",
	}

	name, err := fetchModel(cfg, assetSrv.Client(), "abc")
	if err != nil {
		t.Fatalf("fetchModel: %v", err)
	}
	if name != "sample.x3d" {
		t.Errorf("got model name %q, want sample.x3d", name)
	}

	x3dBytes, err := os.ReadFile(filepath.Join(dir, "sample.x3d"))
	if err != nil {
		t.Fatalf("model was not downloaded: %v", err)
	}
	if string(x3dBytes) != x3dContent {
		t.Errorf("downloaded model content mismatch")
	}

	pngBytes, err := os.ReadFile(filepath.Join(dir, "sample.png"))
	if err != nil {
		t.Fatalf("texture was not downloaded: %v", err)
	}
	if string(pngBytes) != "fake-png-bytes" {
		t.Errorf("downloaded texture content mismatch: %q", pngBytes)
	}
}

func TestLoadIdentifiers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ids.json")
	if err := os.WriteFile(path, []byte(`["a", "b", "c"]`), 0o644); err != nil {
		t.Fatal(err)
	}
	ids, err := loadIdentifiers(path)
	if err != nil {
		t.Fatalf("loadIdentifiers: %v", err)
	}
	want := []string{"a", "b", "c"}
	if len(ids) != len(want) {
		t.Fatalf("got %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Errorf("ids[%d] = %q, want %q", i, ids[i], want[i])
		}
	}
}

func TestLoadIdentifiers_NotAnArray(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ids.json")
	if err := os.WriteFile(path, []byte(`{"not": "an array"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadIdentifiers(path); err == nil {
		t.Fatal("expected error for non-array JSON, got nil")
	}
}
