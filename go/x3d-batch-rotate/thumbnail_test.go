package main

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"testing"
)

func encodePNG(t *testing.T, img image.Image) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encoding test PNG: %v", err)
	}
	return buf.Bytes()
}

func decodePNG(t *testing.T, data []byte) image.Image {
	t.Helper()
	img, err := png.Decode(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("decoding PNG: %v", err)
	}
	return img
}

// solidImage returns an opaque image of the given size filled with c.
func solidImage(w, h int, c color.Color) *image.RGBA {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.Set(x, y, c)
		}
	}
	return img
}

func TestSquareThumbnail_AlreadyCorrectSize_Unchanged(t *testing.T) {
	src := encodePNG(t, solidImage(250, 250, color.RGBA{255, 0, 0, 255}))

	got, err := squareThumbnail(src, 250)
	if err != nil {
		t.Fatalf("squareThumbnail: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Error("expected already-correct-size PNG to be returned unchanged (same bytes), got different bytes")
	}
}

func TestSquareThumbnail_WideImage_PaddedAndResized(t *testing.T) {
	// A 100x50 red image should be padded to a 100x100 white square
	// (red centered, 25px white top/bottom) and then scaled to 40x40.
	src := encodePNG(t, solidImage(100, 50, color.RGBA{255, 0, 0, 255}))

	got, err := squareThumbnail(src, 40)
	if err != nil {
		t.Fatalf("squareThumbnail: %v", err)
	}

	img := decodePNG(t, got)
	b := img.Bounds()
	if b.Dx() != 40 || b.Dy() != 40 {
		t.Fatalf("got size %dx%d, want 40x40", b.Dx(), b.Dy())
	}

	// Corner pixels come from the padded white margin -> should be white.
	r, g, bl, _ := img.At(0, 0).RGBA()
	if r>>8 != 255 || g>>8 != 255 || bl>>8 != 255 {
		t.Errorf("expected white corner pixel, got RGB(%d,%d,%d)", r>>8, g>>8, bl>>8)
	}

	// Center pixel comes from the original red content -> should be
	// predominantly red (allow for CatmullRom interpolation fuzz).
	r, g, bl, _ = img.At(20, 20).RGBA()
	if r>>8 < 200 || g>>8 > 60 || bl>>8 > 60 {
		t.Errorf("expected reddish center pixel, got RGB(%d,%d,%d)", r>>8, g>>8, bl>>8)
	}
}

func TestSquareThumbnail_TallImage_Padded(t *testing.T) {
	src := encodePNG(t, solidImage(50, 100, color.RGBA{0, 0, 255, 255}))

	got, err := squareThumbnail(src, 50)
	if err != nil {
		t.Fatalf("squareThumbnail: %v", err)
	}
	img := decodePNG(t, got)
	b := img.Bounds()
	if b.Dx() != 50 || b.Dy() != 50 {
		t.Fatalf("got size %dx%d, want 50x50", b.Dx(), b.Dy())
	}
}

func TestSquareThumbnail_AlreadySquare_WrongSize_Resized(t *testing.T) {
	src := encodePNG(t, solidImage(500, 500, color.RGBA{0, 255, 0, 255}))

	got, err := squareThumbnail(src, 250)
	if err != nil {
		t.Fatalf("squareThumbnail: %v", err)
	}
	img := decodePNG(t, got)
	b := img.Bounds()
	if b.Dx() != 250 || b.Dy() != 250 {
		t.Fatalf("got size %dx%d, want 250x250", b.Dx(), b.Dy())
	}
}

func TestSquareThumbnail_InvalidPNG(t *testing.T) {
	if _, err := squareThumbnail([]byte("not a png"), 250); err == nil {
		t.Fatal("expected error for invalid PNG input, got nil")
	}
}
