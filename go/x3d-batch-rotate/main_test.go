package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func readTestdata(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("reading testdata/%s: %v", name, err)
	}
	return string(data)
}

func TestWrapSceneInTransform(t *testing.T) {
	src := readTestdata(t, "sample.x3d")

	got, err := wrapSceneInTransform(src, "1 0 0", -75)
	if err != nil {
		t.Fatalf("wrapSceneInTransform: %v", err)
	}

	if !strings.Contains(got, `<Transform rotation="1 0 0 -1.308997">`) {
		t.Errorf("expected rotation attribute not found in output:\n%s", got)
	}

	// The Shape (and everything else that was between <Scene> and
	// </Scene>) must now be nested inside the new Transform, not a
	// sibling of it.
	openIdx := strings.Index(got, "<Transform")
	shapeIdx := strings.Index(got, "<Shape>")
	closeTransformIdx := strings.Index(got, "</Transform>")
	closeSceneIdx := strings.Index(got, "</Scene>")
	if !(openIdx < shapeIdx && shapeIdx < closeTransformIdx && closeTransformIdx < closeSceneIdx) {
		t.Errorf("Shape not nested inside Transform inside Scene; got order Transform=%d Shape=%d /Transform=%d /Scene=%d",
			openIdx, shapeIdx, closeTransformIdx, closeSceneIdx)
	}

	// Original document structure (aside from the new wrapper) must be
	// byte-for-byte preserved, in particular the huge geometry attributes
	// this tool never needs to parse or understand.
	innerWant := src[strings.Index(src, "<Shape>") : strings.Index(src, "</Shape>")+len("</Shape>")]
	if !strings.Contains(got, innerWant) {
		t.Errorf("original Shape content was not preserved verbatim")
	}

	// It must still be well-formed enough to find exactly one Scene/Transform pair.
	if n := strings.Count(got, "<Scene>"); n != 1 {
		t.Errorf("expected exactly one <Scene>, got %d", n)
	}
	if n := strings.Count(got, "<Transform"); n != 1 {
		t.Errorf("expected exactly one <Transform>, got %d", n)
	}
}

func TestWrapSceneInTransform_PositiveAndNegativeAngles(t *testing.T) {
	src := readTestdata(t, "sample.x3d")

	pos, err := wrapSceneInTransform(src, "1 0 0", 75)
	if err != nil {
		t.Fatalf("wrapSceneInTransform(+75): %v", err)
	}
	neg, err := wrapSceneInTransform(src, "1 0 0", -75)
	if err != nil {
		t.Fatalf("wrapSceneInTransform(-75): %v", err)
	}

	angleRe := regexp.MustCompile(`rotation="1 0 0 (-?[0-9.]+)"`)
	pm := angleRe.FindStringSubmatch(pos)
	nm := angleRe.FindStringSubmatch(neg)
	if pm == nil || nm == nil {
		t.Fatalf("could not extract rotation angle from output; pos=%q neg=%q", pos, neg)
	}
	if pm[1] != "1.308997" {
		t.Errorf("expected +75deg -> 1.308997 rad, got %s", pm[1])
	}
	if nm[1] != "-1.308997" {
		t.Errorf("expected -75deg -> -1.308997 rad, got %s", nm[1])
	}
}

func TestWrapSceneInTransform_NoScene(t *testing.T) {
	_, err := wrapSceneInTransform("<X3D><NotAScene></NotAScene></X3D>", "1 0 0", 75)
	if err == nil {
		t.Fatal("expected error for document with no <Scene>, got nil")
	}
}

func TestExtractTextureURLs(t *testing.T) {
	src := readTestdata(t, "sample.x3d")
	urls := extractTextureURLs(src)
	if len(urls) != 1 || urls[0] != "sample.png" {
		t.Errorf("expected [\"sample.png\"], got %v", urls)
	}
}

func TestExtractTextureURLs_Dedup(t *testing.T) {
	src := `<Scene>
    <Shape><Appearance><ImageTexture url="tex.png"/></Appearance></Shape>
    <Shape><Appearance><ImageTexture url="tex.png"/></Appearance></Shape>
    <Shape><Appearance><ImageTexture url="other.png"/></Appearance></Shape>
  </Scene>`
	urls := extractTextureURLs(src)
	if len(urls) != 2 || urls[0] != "tex.png" || urls[1] != "other.png" {
		t.Errorf("expected [tex.png other.png], got %v", urls)
	}
}

func TestIsRelativeAssetURL(t *testing.T) {
	cases := map[string]bool{
		"texture.png":                    true,
		"sub/dir/texture.png":            true,
		"http://example.com/texture.png": false,
		"https://example.com/tex.png":    false,
		"data:image/png;base64,AAAA":     false,
		"/abs/texture.png":               false,
	}
	for u, want := range cases {
		if got := isRelativeAssetURL(u); got != want {
			t.Errorf("isRelativeAssetURL(%q) = %v, want %v", u, got, want)
		}
	}
}

func TestFindModels(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.x3d", "b.X3D", "rotated_a.x3d", "notes.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "subdir.x3d"), 0o755); err != nil {
		t.Fatal(err)
	}

	got, err := findModels(dir, "rotated_")
	if err != nil {
		t.Fatalf("findModels: %v", err)
	}
	want := []string{"a.x3d", "b.X3D"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("got[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestCopyFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	if err := os.WriteFile(src, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	dst := filepath.Join(dir, "nested", "dst.txt")
	if err := copyFile(src, dst); err != nil {
		t.Fatalf("copyFile: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("reading copied file: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("got %q, want %q", got, "hello")
	}

	// Existing destination is left untouched (no-op), even if src changes.
	if err := os.WriteFile(src, []byte("changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(src, dst); err != nil {
		t.Fatalf("copyFile (no-op case): %v", err)
	}
	got, _ = os.ReadFile(dst)
	if string(got) != "hello" {
		t.Errorf("expected existing destination to be left alone, got %q", got)
	}

	// Same src/dst path is a no-op, not an error.
	if err := copyFile(src, src); err != nil {
		t.Errorf("copyFile(src, src) should be a no-op, got error: %v", err)
	}
}
