package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// fakeDDB is a minimal ddbAPI implementation for tests: it never touches
// AWS, and each method just delegates to a configurable func field.
type fakeDDB struct {
	getItem func(ctx context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	scan    func(ctx context.Context, in *dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
}

func (f *fakeDDB) GetItem(ctx context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return f.getItem(ctx, in)
}

func (f *fakeDDB) Scan(ctx context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	return f.scan(ctx, in)
}

func strAttr(s string) types.AttributeValue { return &types.AttributeValueMemberS{Value: s} }

func TestFetchDDBItem_GetItem(t *testing.T) {
	var gotKey map[string]types.AttributeValue
	var gotTable string
	client := &fakeDDB{
		getItem: func(ctx context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			gotKey = in.Key
			gotTable = *in.TableName
			return &dynamodb.GetItemOutput{
				Item: map[string]types.AttributeValue{
					"id":             strAttr("abc123"),
					"archiveOptions": strAttr(`{"assets":{"x3d_config":"https://example.com/model.x3d"}}`),
				},
			}, nil
		},
	}

	cfg := &Config{
		LookupMode:       "get_item",
		PartitionKeyAttr: "id",
		TableName:        "my-table",
		Region:           "us-east-1",
	}
	item, err := fetchDDBItem(context.Background(), client, cfg, "abc123")
	if err != nil {
		t.Fatalf("fetchDDBItem: %v", err)
	}
	got, ok := item["id"].(*types.AttributeValueMemberS)
	if !ok || got.Value != "abc123" {
		t.Errorf("unexpected id attr: %+v", item["id"])
	}
	if gotTable != "my-table" {
		t.Errorf("got table %q, want my-table", gotTable)
	}
	keyAttr, ok := gotKey["id"].(*types.AttributeValueMemberS)
	if !ok || keyAttr.Value != "abc123" {
		t.Errorf("expected GetItem key id=abc123, got: %+v", gotKey)
	}
}

func TestFetchDDBItem_GetItem_NotFound(t *testing.T) {
	client := &fakeDDB{
		getItem: func(ctx context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{}, nil // no Item: not found
		},
	}
	cfg := &Config{LookupMode: "get_item", PartitionKeyAttr: "id"}
	if _, err := fetchDDBItem(context.Background(), client, cfg, "missing"); err == nil {
		t.Fatal("expected error for missing item, got nil")
	}
}

func TestFetchDDBItem_GetItem_Error(t *testing.T) {
	client := &fakeDDB{
		getItem: func(ctx context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			return nil, fmt.Errorf("boom")
		},
	}
	cfg := &Config{LookupMode: "get_item", PartitionKeyAttr: "id"}
	if _, err := fetchDDBItem(context.Background(), client, cfg, "x"); err == nil {
		t.Fatal("expected error to propagate, got nil")
	}
}

func TestFetchDDBItem_Scan(t *testing.T) {
	var gotFilter string
	client := &fakeDDB{
		scan: func(ctx context.Context, in *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
			gotFilter = *in.FilterExpression
			return &dynamodb.ScanOutput{
				Items: []map[string]types.AttributeValue{
					{"identifier": strAttr("368a8114"), "archiveOptions": strAttr("{}")},
				},
			}, nil
		},
	}
	cfg := &Config{
		LookupMode:     "scan",
		IdentifierAttr: "identifier",
		TableName:      "my-table",
		Region:         "us-east-1",
	}
	item, err := fetchDDBItem(context.Background(), client, cfg, "368a8114")
	if err != nil {
		t.Fatalf("fetchDDBItem: %v", err)
	}
	got, ok := item["identifier"].(*types.AttributeValueMemberS)
	if !ok || got.Value != "368a8114" {
		t.Errorf("unexpected identifier attr: %+v", item["identifier"])
	}
	if gotFilter != "#a = :v" {
		t.Errorf("got filter expression %q", gotFilter)
	}
}

func TestFetchDDBItem_Scan_PaginatesUntilMatch(t *testing.T) {
	calls := 0
	client := &fakeDDB{
		scan: func(ctx context.Context, in *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
			calls++
			if calls == 1 {
				// First page: no match, but more pages exist.
				return &dynamodb.ScanOutput{
					Items:            nil,
					LastEvaluatedKey: map[string]types.AttributeValue{"id": strAttr("page1-end")},
				}, nil
			}
			// Second page: the match. Also verify ExclusiveStartKey was
			// carried over from the previous page's LastEvaluatedKey.
			if in.ExclusiveStartKey == nil {
				t.Errorf("expected ExclusiveStartKey to be set on page 2")
			}
			return &dynamodb.ScanOutput{
				Items: []map[string]types.AttributeValue{
					{"identifier": strAttr("found-me")},
				},
			}, nil
		},
	}
	cfg := &Config{LookupMode: "scan", IdentifierAttr: "identifier"}
	item, err := fetchDDBItem(context.Background(), client, cfg, "found-me")
	if err != nil {
		t.Fatalf("fetchDDBItem: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 Scan calls (pagination), got %d", calls)
	}
	got := item["identifier"].(*types.AttributeValueMemberS).Value
	if got != "found-me" {
		t.Errorf("got %q", got)
	}
}

func TestFetchDDBItem_Scan_NotFound(t *testing.T) {
	client := &fakeDDB{
		scan: func(ctx context.Context, in *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
			return &dynamodb.ScanOutput{Items: nil, LastEvaluatedKey: nil}, nil
		},
	}
	cfg := &Config{LookupMode: "scan", IdentifierAttr: "identifier"}
	if _, err := fetchDDBItem(context.Background(), client, cfg, "nope"); err == nil {
		t.Fatal("expected error when scan exhausts all pages with no match, got nil")
	}
}

func TestFetchDDBItem_IdentifierPrefix(t *testing.T) {
	var gotValue string
	client := &fakeDDB{
		scan: func(ctx context.Context, in *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
			v := in.ExpressionAttributeValues[":v"].(*types.AttributeValueMemberS)
			gotValue = v.Value
			return &dynamodb.ScanOutput{Items: []map[string]types.AttributeValue{{"x": strAttr("y")}}}, nil
		},
	}
	cfg := &Config{
		LookupMode:       "scan",
		IdentifierAttr:   "custom_key",
		IdentifierPrefix: "ark:/53696/",
	}
	if _, err := fetchDDBItem(context.Background(), client, cfg, "368a8114"); err != nil {
		t.Fatalf("fetchDDBItem: %v", err)
	}
	if gotValue != "ark:/53696/368a8114" {
		t.Errorf("got filter value %q, want prefixed identifier", gotValue)
	}
}

func TestFetchDDBItem_InvalidLookupMode(t *testing.T) {
	cfg := &Config{LookupMode: "bogus"}
	if _, err := fetchDDBItem(context.Background(), &fakeDDB{}, cfg, "x"); err == nil {
		t.Fatal("expected error for invalid lookup_mode, got nil")
	}
}

func TestExtractX3DConfigURL_StringJSON(t *testing.T) {
	item := map[string]types.AttributeValue{
		"archiveOptions": strAttr(`{"assets":{"x3d_config":"https://cdn.example.com/models/foo.x3d","other":"ignored"}}`),
	}
	url, err := extractX3DConfigURL(item, "archiveOptions")
	if err != nil {
		t.Fatalf("extractX3DConfigURL: %v", err)
	}
	if url != "https://cdn.example.com/models/foo.x3d" {
		t.Errorf("got %q", url)
	}
}

// TestExtractX3DConfigURL_Map covers the shape actually observed in
// production: archiveOptions (an AppSync AWSJSON-typed field) stored as a
// native DynamoDB Map (M) of nested M/L/S/N/BOOL attributes, not as a
// JSON-encoded String — confirmed by fetching a real item and inspecting
// its raw attribute type (see fetch.go's extractX3DConfigURL doc comment).
func TestExtractX3DConfigURL_Map(t *testing.T) {
	item := map[string]types.AttributeValue{
		"archiveOptions": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"assets": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"x3d_config": &types.AttributeValueMemberS{Value: "https://cdn.example.com/models/foo.x3d"},
				"media_type": &types.AttributeValueMemberS{Value: "3d-model/x3d"},
			}},
			"config": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"_3d": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
					"scale_factor": &types.AttributeValueMemberS{Value: ""},
				}},
			}},
		}},
	}
	url, err := extractX3DConfigURL(item, "archiveOptions")
	if err != nil {
		t.Fatalf("extractX3DConfigURL: %v", err)
	}
	if url != "https://cdn.example.com/models/foo.x3d" {
		t.Errorf("got %q", url)
	}
}

// TestExtractX3DConfigURL_Map_GLTFOnly mirrors a real record found in the
// table this tool targets: a glTF-only item (assets.gltf_config present,
// no assets.x3d_config), stored as a Map. This must produce the
// "no assets.x3d_config" error, not a type error.
func TestExtractX3DConfigURL_Map_GLTFOnly(t *testing.T) {
	item := map[string]types.AttributeValue{
		"archiveOptions": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"assets": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"gltf_config": &types.AttributeValueMemberS{Value: "https://cdn.example.com/models/foo.glb"},
				"media_type":  &types.AttributeValueMemberS{Value: "3d-model/gltf"},
			}},
		}},
	}
	_, err := extractX3DConfigURL(item, "archiveOptions")
	if err == nil {
		t.Fatal("expected error for glTF-only item with no assets.x3d_config, got nil")
	}
}

func TestExtractX3DConfigURL_MissingField(t *testing.T) {
	item := map[string]types.AttributeValue{}
	if _, err := extractX3DConfigURL(item, "archiveOptions"); err == nil {
		t.Fatal("expected error for missing archiveOptions attribute, got nil")
	}
}

func TestExtractX3DConfigURL_UnsupportedType(t *testing.T) {
	item := map[string]types.AttributeValue{
		"archiveOptions": &types.AttributeValueMemberN{Value: "42"},
	}
	if _, err := extractX3DConfigURL(item, "archiveOptions"); err == nil {
		t.Fatal("expected error for attribute that is neither Map nor String, got nil")
	}
}

func TestExtractX3DConfigURL_MissingX3DConfig(t *testing.T) {
	item := map[string]types.AttributeValue{
		"archiveOptions": strAttr(`{"assets":{"env_config":"https://example.com/env.x3d"}}`),
	}
	if _, err := extractX3DConfigURL(item, "archiveOptions"); err == nil {
		t.Fatal("expected error for missing assets.x3d_config, got nil")
	}
}

func TestExtractX3DConfigURL_InvalidJSON(t *testing.T) {
	item := map[string]types.AttributeValue{
		"archiveOptions": strAttr(`not json`),
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

	client := &fakeDDB{
		getItem: func(ctx context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			archiveOptions := fmt.Sprintf(`{"assets":{"x3d_config":%q}}`, assetSrv.URL+"/models/sample.x3d")
			return &dynamodb.GetItemOutput{
				Item: map[string]types.AttributeValue{
					"id":             strAttr("abc"),
					"archiveOptions": strAttr(archiveOptions),
				},
			}, nil
		},
	}

	dir := t.TempDir()
	cfg := &Config{
		InputDir:            dir,
		LookupMode:          "get_item",
		PartitionKeyAttr:    "id",
		ArchiveOptionsField: "archiveOptions",
	}

	name, err := fetchModel(context.Background(), client, assetSrv.Client(), cfg, "abc")
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
