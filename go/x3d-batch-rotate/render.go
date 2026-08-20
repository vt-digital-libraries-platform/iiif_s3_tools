package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"text/template"
	"time"

	"github.com/chromedp/chromedp"
)

// renderPageTmpl is served for every render, parameterized with the model
// URL to load. Custom X3DOM element tags MUST use explicit closing tags
// (<viewpoint>...</viewpoint>), not self-closing "/>" syntax: HTML parsing
// does not treat unknown/custom elements as void, so a self-closed
// <viewpoint .../> silently swallows every following sibling as its child,
// corrupting the scene graph (the model still "loads" with no error, but
// nothing ends up in the renderable scene).
var renderPageTmpl = template.Must(template.New("render").Parse(`<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Render</title>
  <link rel="stylesheet" type="text/css" href="{{.X3domCSSURL}}">
  <script src="{{.X3domJSURL}}"></script>
  <style>
    html, body { margin:0; padding:0; background:#ffffff; }
    x3d { border:none; width:{{.Size}}px; height:{{.Size}}px; }
  </style>
</head>
<body>
  <x3d id="x3dElement" is="x3d" width="{{.Size}}px" height="{{.Size}}px">
    <scene is="x3d">
      <viewpoint id="mainViewpoint" position="0 0 4"></viewpoint>
      <navigationInfo type="examine" id="navType"></navigationInfo>
      <inline
        id="x3dInline"
        DEF="x3dInline"
        nameSpaceName="model"
        is="x3d"
        mapDEFToID="true"
        url="{{.ModelURL}}"
      ></inline>
    </scene>
  </x3d>
</body>
</html>
`))

type renderPageData struct {
	X3domJSURL  string
	X3domCSSURL string
	Size        int
	ModelURL    string
}

// fileServer is a local HTTP server rooted at a directory, used so that
// relative <ImageTexture url="..."> references inside an X3D file resolve
// exactly as they would for a real hosted model, and so the render page
// itself can be served alongside the model.
type fileServer struct {
	listener net.Listener
	server   *http.Server
	mux      *http.ServeMux
	BaseURL  string
}

func startFileServer(rootDir string) (*fileServer, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir(rootDir)))
	srv := &http.Server{Handler: mux}
	fs := &fileServer{
		listener: ln,
		server:   srv,
		mux:      mux,
		BaseURL:  fmt.Sprintf("http://%s", ln.Addr().String()),
	}
	go srv.Serve(ln)
	return fs, nil
}

func (f *fileServer) Close() error {
	return f.server.Close()
}

// handleRenderPage registers a one-off handler serving the render HTML for
// a specific model at a random path, and returns the full URL to it plus a
// cleanup func that unregisters the handler.
func (f *fileServer) handleRenderPage(cfg *Config, modelRelPath string) (string, func(), error) {
	var buf []byte
	data := renderPageData{
		X3domJSURL:  cfg.X3domJSURL,
		X3domCSSURL: cfg.X3domCSSURL,
		Size:        cfg.ImageSize,
		ModelURL:    modelRelPath,
	}

	w := &bufWriter{}
	if err := renderPageTmpl.Execute(w, data); err != nil {
		return "", nil, err
	}
	buf = w.b

	path := fmt.Sprintf("/_render_%d_%s", time.Now().UnixNano(), modelRelPath)
	f.mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(buf)
	})

	cleanup := func() {
		// net/http's ServeMux has no Unregister; harmless one-off paths
		// are left registered for the lifetime of the short-lived server.
	}
	return f.BaseURL + path, cleanup, nil
}

type bufWriter struct{ b []byte }

func (w *bufWriter) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}

// newBrowserAllocator starts a headless Chrome instance (as an allocator,
// not yet a tab/page) configured to support WebGL under software rendering
// (SwiftShader), which x3dom requires to draw anything at all. Each render
// opens its own tab from this allocator via chromedp.NewContext — see the
// comment on renderModelPNG for why a shared tab is not reused across
// renders.
func newBrowserAllocator() (context.Context, context.CancelFunc) {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", true),
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("use-gl", "swiftshader"),
		chromedp.Flag("enable-webgl", true),
		chromedp.Flag("ignore-gpu-blocklist", true),
		chromedp.Flag("enable-unsafe-swiftshader", true),
	)
	return chromedp.NewExecAllocator(context.Background(), opts...)
}

// sceneReadyExpr polls the x3dom runtime until the model's geometry has
// actually been linked into the renderable scene graph (a non-empty,
// valid bounding volume), rather than just relying on the <inline>
// element's load="true" attribute, which can be set before the scene
// graph finishes linking.
const sceneReadyExpr = `(() => {
  const el = document.getElementById('x3dElement');
  if (!el || !el.runtime || !el.runtime.canvas || !el.runtime.canvas.doc) return false;
  const scene = el.runtime.canvas.doc._scene;
  if (!scene || typeof scene.getVolume !== 'function') return false;
  const vol = scene.getVolume();
  return !!(vol && vol.isValid());
})()`

// renderModelPNG renders modelRelPath (a path relative to srv's root,
// e.g. "rotated_foo.x3d") to a PNG screenshot of exactly the X3D canvas
// element, at cfg.ImageSize x cfg.ImageSize pixels.
//
// allocCtx must be an allocator context from newBrowserAllocator, not a
// tab context: a fresh tab (chromedp.NewContext) is opened per call and
// closed when the call returns. Reusing one tab context across multiple
// sequential chromedp.Run calls, each wrapped in its own
// context.WithTimeout, was observed to make every render after the first
// fail instantly with "context canceled" — canceling the WithTimeout
// child unexpectedly tore down the shared tab context too. A fresh tab
// per render sidesteps that entirely and also isolates x3dom's global
// window state between models.
func renderModelPNG(allocCtx context.Context, srv *fileServer, cfg *Config, modelRelPath string) ([]byte, error) {
	pageURL, cleanup, err := srv.handleRenderPage(cfg, modelRelPath)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	tabCtx, tabCancel := chromedp.NewContext(allocCtx)
	defer tabCancel()

	runCtx, cancel := context.WithTimeout(tabCtx, time.Duration(cfg.RenderTimeoutSeconds)*time.Second)
	defer cancel()

	// A node-targeted chromedp.Screenshot("canvas", ...) can hang
	// indefinitely against x3dom's WebGL canvas (observed: navigate and
	// poll both complete in ~1s, then the screenshot action never
	// returns). Since the render page has no margin/padding and the x3d
	// element is sized to exactly cfg.ImageSize, a full-viewport
	// screenshot at that same size is pixel-identical to a crop of the
	// canvas and avoids the hang entirely.
	var buf []byte
	var ready bool
	err = chromedp.Run(runCtx,
		chromedp.EmulateViewport(int64(cfg.ImageSize), int64(cfg.ImageSize)),
		chromedp.Navigate(pageURL),
		chromedp.Poll(sceneReadyExpr, &ready, chromedp.WithPollingTimeout(time.Duration(cfg.RenderTimeoutSeconds)*time.Second)),
		chromedp.Sleep(time.Duration(cfg.RenderWaitSeconds)*time.Second),
		chromedp.CaptureScreenshot(&buf),
	)
	if err != nil {
		return nil, fmt.Errorf("rendering %s: %w", modelRelPath, err)
	}
	if !ready {
		return nil, fmt.Errorf("rendering %s: scene never became ready", modelRelPath)
	}
	return buf, nil
}
