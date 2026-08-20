package main

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

var (
	sceneOpenRe       = regexp.MustCompile(`<Scene\b[^>]*>`)
	sceneCloseRe      = regexp.MustCompile(`</Scene\s*>`)
	imageTextureURLRe = regexp.MustCompile(`<ImageTexture\b[^>]*\burl\s*=\s*"([^"]*)"`)
)

// wrapSceneInTransform rewrites the <Scene>...</Scene> body of an X3D
// document so every existing child node is nested inside one new
// <Transform rotation="axis angleRad"> node, rotating the whole model by
// angleDeg degrees about axis (an X3D SFVec3f string, e.g. "1 0 0").
//
// This is deliberately structure-agnostic: it does not need to know how
// many Shape/Group nodes the scene contains, since it wraps everything
// between <Scene> and </Scene> in a single new parent.
func wrapSceneInTransform(content, axis string, angleDeg float64) (string, error) {
	openLoc := sceneOpenRe.FindStringIndex(content)
	if openLoc == nil {
		return "", fmt.Errorf("no <Scene> element found")
	}
	closeLoc := sceneCloseRe.FindStringIndex(content)
	if closeLoc == nil {
		return "", fmt.Errorf("no </Scene> element found")
	}
	if closeLoc[0] < openLoc[1] {
		return "", fmt.Errorf("</Scene> occurs before <Scene> content")
	}

	angleRad := angleDeg * math.Pi / 180.0

	var b strings.Builder
	b.WriteString(content[:openLoc[1]])
	b.WriteString("\n    <Transform rotation=\"")
	b.WriteString(axis)
	b.WriteString(" ")
	b.WriteString(strconv.FormatFloat(angleRad, 'f', 6, 64))
	b.WriteString("\">\n")
	b.WriteString(content[openLoc[1]:closeLoc[0]])
	b.WriteString("\n    </Transform>\n  ")
	b.WriteString(content[closeLoc[0]:])
	return b.String(), nil
}

// extractTextureURLs returns the de-duplicated "url" attribute values of
// every <ImageTexture> element in an X3D document, in document order.
func extractTextureURLs(content string) []string {
	matches := imageTextureURLRe.FindAllStringSubmatch(content, -1)
	seen := make(map[string]bool, len(matches))
	urls := make([]string, 0, len(matches))
	for _, m := range matches {
		u := strings.TrimSpace(m[1])
		if u == "" || seen[u] {
			continue
		}
		seen[u] = true
		urls = append(urls, u)
	}
	return urls
}

// isRelativeAssetURL reports whether u is a plain relative path (as opposed
// to an absolute http(s) or data URL), i.e. a reference to a sibling file
// that a batch run would need to make available alongside the model.
func isRelativeAssetURL(u string) bool {
	lower := strings.ToLower(u)
	return !strings.HasPrefix(lower, "http://") &&
		!strings.HasPrefix(lower, "https://") &&
		!strings.HasPrefix(lower, "data:") &&
		!strings.HasPrefix(u, "/")
}
