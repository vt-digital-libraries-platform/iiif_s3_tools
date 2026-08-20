package main

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"

	xdraw "golang.org/x/image/draw"
)

// squareThumbnail decodes a rendered PNG and returns a size x size PNG.
// If the source isn't already square, it is first centered on a white
// square canvas sized to its longer side (no cropping, no stretching —
// the source image itself is copied in unmodified), which is then scaled
// to size x size. If the source is already exactly size x size, it is
// returned unchanged rather than being re-encoded.
func squareThumbnail(pngBytes []byte, size int) ([]byte, error) {
	img, err := png.Decode(bytes.NewReader(pngBytes))
	if err != nil {
		return nil, fmt.Errorf("decoding rendered PNG: %w", err)
	}

	b := img.Bounds()
	w, h := b.Dx(), b.Dy()
	if w == size && h == size {
		return pngBytes, nil
	}

	side := w
	if h > side {
		side = h
	}

	square := image.NewRGBA(image.Rect(0, 0, side, side))
	draw.Draw(square, square.Bounds(), image.NewUniform(color.White), image.Point{}, draw.Src)
	offsetX := (side - w) / 2
	offsetY := (side - h) / 2
	dstRect := image.Rect(offsetX, offsetY, offsetX+w, offsetY+h)
	draw.Draw(square, dstRect, img, b.Min, draw.Src)

	dst := image.NewRGBA(image.Rect(0, 0, size, size))
	xdraw.CatmullRom.Scale(dst, dst.Bounds(), square, square.Bounds(), xdraw.Src, nil)

	var buf bytes.Buffer
	if err := png.Encode(&buf, dst); err != nil {
		return nil, fmt.Errorf("encoding thumbnail PNG: %w", err)
	}
	return buf.Bytes(), nil
}
