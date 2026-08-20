// Command x3d-batch-rotate takes a directory of X3D models, writes a
// rotated copy of each one (the whole scene wrapped in a single new
// <Transform rotation="..."> node), and — unless rendering is disabled —
// uses headless Chrome (via chromedp) to render an "initial" PNG snapshot
// of each original model and a "rotated" PNG snapshot of each rotated
// model, the same way an X3D viewer in a browser would show them.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	InputDir  string `yaml:"input_dir"`
	OutputDir string `yaml:"output_dir"`

	// Identifier-driven fetch phase: when IdentifiersFile is set, models
	// are downloaded into InputDir from DynamoDB/S3-or-CloudFront before
	// the usual rotate/render phase runs over InputDir. See fetch.go.
	IdentifiersFile     string `yaml:"identifiers_file"`
	Region              string `yaml:"region"`
	TableName           string `yaml:"table_name"`
	LookupMode          string `yaml:"lookup_mode"`
	PartitionKeyAttr    string `yaml:"partition_key_attr"`
	IdentifierAttr      string `yaml:"identifier_attr"`
	IdentifierPrefix    string `yaml:"identifier_prefix"`
	ArchiveOptionsField string `yaml:"archive_options_field"`

	Degrees       float64 `yaml:"degrees"`
	Axis          string  `yaml:"axis"`
	RotatedPrefix string  `yaml:"rotated_prefix"`

	Render               bool `yaml:"render"`
	SkipInitialRender    bool `yaml:"skip_initial_render"`
	ImageSize            int  `yaml:"image_size"`
	RenderWaitSeconds    int  `yaml:"render_wait_seconds"`
	RenderTimeoutSeconds int  `yaml:"render_timeout_seconds"`

	X3domJSURL  string `yaml:"x3dom_js_url"`
	X3domCSSURL string `yaml:"x3dom_css_url"`

	InitialSuffix string `yaml:"initial_suffix"`
	RotatedSuffix string `yaml:"rotated_suffix"`

	DryRun bool `yaml:"dry_run"`
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	if cfg.OutputDir == "" {
		cfg.OutputDir = cfg.InputDir
	}
	if cfg.Axis == "" {
		cfg.Axis = "1 0 0"
	}
	if cfg.RotatedPrefix == "" {
		cfg.RotatedPrefix = "rotated_"
	}
	if cfg.ImageSize == 0 {
		cfg.ImageSize = 1000
	}
	if cfg.RenderWaitSeconds == 0 {
		cfg.RenderWaitSeconds = 4
	}
	if cfg.RenderTimeoutSeconds == 0 {
		cfg.RenderTimeoutSeconds = 60
	}
	if cfg.X3domJSURL == "" {
		cfg.X3domJSURL = "https://img.cloud.lib.vt.edu/scripts/x3dom_1.8.4-dev.js"
	}
	if cfg.X3domCSSURL == "" {
		cfg.X3domCSSURL = "https://img.cloud.lib.vt.edu/scripts/x3dom.css"
	}
	if cfg.InitialSuffix == "" {
		cfg.InitialSuffix = "_initial.PNG"
	}
	if cfg.RotatedSuffix == "" {
		cfg.RotatedSuffix = "_rotated.PNG"
	}
	if cfg.LookupMode == "" {
		cfg.LookupMode = "get_item"
	}
	if cfg.PartitionKeyAttr == "" {
		cfg.PartitionKeyAttr = "id"
	}
	if cfg.IdentifierAttr == "" {
		cfg.IdentifierAttr = "identifier"
	}
	if cfg.ArchiveOptionsField == "" {
		cfg.ArchiveOptionsField = "archiveOptions"
	}

	var missing []string
	if cfg.InputDir == "" {
		missing = append(missing, "input_dir")
	}
	if cfg.IdentifiersFile != "" {
		if cfg.Region == "" {
			missing = append(missing, "region (required when identifiers_file is set)")
		}
		if cfg.TableName == "" {
			missing = append(missing, "table_name (required when identifiers_file is set)")
		}
		if cfg.LookupMode != "get_item" && cfg.LookupMode != "scan" {
			missing = append(missing, `lookup_mode (must be "get_item" or "scan")`)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("config missing required field(s): %s", strings.Join(missing, ", "))
	}

	return &cfg, nil
}

// findModels returns the .x3d files directly inside dir, sorted, excluding
// any that already carry the rotated-output prefix (so re-running the tool
// over its own output directory doesn't try to re-rotate its own results).
func findModels(dir, rotatedPrefix string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading input dir: %w", err)
	}

	var models []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.EqualFold(filepath.Ext(name), ".x3d") {
			continue
		}
		if strings.HasPrefix(name, rotatedPrefix) {
			continue
		}
		models = append(models, name)
	}
	sort.Strings(models)
	return models, nil
}

// copyFile copies src to dst, creating dst's parent directory if needed.
// It is a no-op if dst already exists (assets are treated as immutable
// once staged for a batch run).
func copyFile(src, dst string) error {
	if src == dst {
		return nil
	}
	if _, err := os.Stat(dst); err == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.CreateTemp(filepath.Dir(dst), ".tmp-copy-*")
	if err != nil {
		return err
	}
	tmpName := out.Name()
	defer os.Remove(tmpName)

	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, dst)
}

type result struct {
	model          string
	rotated        bool
	renderedInit   bool
	renderedRotate bool
	err            error
}

func run(cfg *Config) ([]result, error) {
	models, err := findModels(cfg.InputDir, cfg.RotatedPrefix)
	if err != nil {
		return nil, err
	}

	if cfg.Render && !cfg.DryRun {
		if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
			return nil, fmt.Errorf("creating output dir: %w", err)
		}
	}

	var (
		results []result
		alloc   context.Context
		cancel  context.CancelFunc
		srv     *fileServer
	)

	if cfg.Render && !cfg.DryRun && len(models) > 0 {
		var err error
		srv, err = startFileServer(cfg.OutputDir)
		if err != nil {
			return nil, fmt.Errorf("starting local file server: %w", err)
		}
		defer srv.Close()

		alloc, cancel = newBrowserAllocator()
		defer cancel()
	}

	for _, name := range models {
		res := result{model: name}
		if err := processModel(cfg, alloc, srv, name, &res); err != nil {
			res.err = err
		}
		results = append(results, res)
	}

	return results, nil
}

func processModel(cfg *Config, browserCtx context.Context, srv *fileServer, name string, res *result) error {
	stem := strings.TrimSuffix(name, filepath.Ext(name))
	srcPath := filepath.Join(cfg.InputDir, name)
	rotatedName := cfg.RotatedPrefix + name
	rotatedPath := filepath.Join(cfg.OutputDir, rotatedName)

	content, err := os.ReadFile(srcPath)
	if err != nil {
		return fmt.Errorf("reading %s: %w", name, err)
	}

	rotatedContent, err := wrapSceneInTransform(string(content), cfg.Axis, cfg.Degrees)
	if err != nil {
		return fmt.Errorf("rotating %s: %w", name, err)
	}

	if cfg.DryRun {
		log.Printf("[dry-run] would write %s", rotatedPath)
	} else {
		if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
			return fmt.Errorf("creating output dir: %w", err)
		}
		if err := os.WriteFile(rotatedPath, []byte(rotatedContent), 0o644); err != nil {
			return fmt.Errorf("writing %s: %w", rotatedPath, err)
		}
	}
	res.rotated = true
	log.Printf("rotated: %s -> %s (%.2f deg about %s)", name, rotatedName, cfg.Degrees, cfg.Axis)

	if !cfg.Render {
		return nil
	}

	// Make sure the model file(s) and any relatively-referenced textures
	// they need are all reachable from the directory being served, so
	// relative <ImageTexture url="..."> references resolve in-browser
	// regardless of whether output_dir differs from input_dir.
	for _, tex := range extractTextureURLs(string(content)) {
		if !isRelativeAssetURL(tex) {
			continue
		}
		if cfg.DryRun {
			continue
		}
		if err := copyFile(filepath.Join(cfg.InputDir, tex), filepath.Join(cfg.OutputDir, tex)); err != nil {
			return fmt.Errorf("staging texture %s for %s: %w", tex, name, err)
		}
	}

	if cfg.DryRun {
		if !cfg.SkipInitialRender {
			log.Printf("[dry-run] would render %s", filepath.Join(cfg.OutputDir, stem+cfg.InitialSuffix))
		}
		log.Printf("[dry-run] would render %s", filepath.Join(cfg.OutputDir, stem+cfg.RotatedSuffix))
		return nil
	}

	if !cfg.SkipInitialRender {
		// The original model must also be reachable under the served
		// output directory for its relative texture reference to resolve.
		if err := copyFile(srcPath, filepath.Join(cfg.OutputDir, name)); err != nil {
			return fmt.Errorf("staging %s for render: %w", name, err)
		}
		initPath := filepath.Join(cfg.OutputDir, stem+cfg.InitialSuffix)
		png, err := renderModelPNG(browserCtx, srv, cfg, name)
		if err != nil {
			return fmt.Errorf("rendering initial view of %s: %w", name, err)
		}
		if err := os.WriteFile(initPath, png, 0o644); err != nil {
			return fmt.Errorf("writing %s: %w", initPath, err)
		}
		res.renderedInit = true
		log.Printf("rendered: %s", initPath)
	}

	rotPath := filepath.Join(cfg.OutputDir, stem+cfg.RotatedSuffix)
	png, err := renderModelPNG(browserCtx, srv, cfg, rotatedName)
	if err != nil {
		return fmt.Errorf("rendering rotated view of %s: %w", name, err)
	}
	if err := os.WriteFile(rotPath, png, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", rotPath, err)
	}
	res.renderedRotate = true
	log.Printf("rendered: %s", rotPath)

	return nil
}

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML config file")
	dryRun := flag.Bool("dry-run", false, "report planned actions without writing or rendering anything")
	noRender := flag.Bool("no-render", false, "skip rendering PNGs; only write rotated .x3d files")
	inputDir := flag.String("input", "", "override input_dir from the config file")
	outputDir := flag.String("output", "", "override output_dir from the config file")
	degrees := flag.Float64("degrees", 0, "override degrees from the config file (0 means: use config value)")
	identifiersFile := flag.String("identifiers", "", "override identifiers_file from the config file; fetch models from DynamoDB before rotating/rendering")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("config error: %v", err)
	}
	if *dryRun {
		cfg.DryRun = true
	}
	if *noRender {
		cfg.Render = false
	}
	if *inputDir != "" {
		cfg.InputDir = *inputDir
	}
	if *outputDir != "" {
		cfg.OutputDir = *outputDir
	}
	if *degrees != 0 {
		cfg.Degrees = *degrees
	}
	if *identifiersFile != "" {
		cfg.IdentifiersFile = *identifiersFile
	}

	start := time.Now()

	var fetchedN, fetchFailedN int
	if cfg.IdentifiersFile != "" {
		fetchedN, fetchFailedN, err = fetchAll(cfg)
		if err != nil {
			log.Fatalf("fatal: %v", err)
		}
	}

	results, err := run(cfg)
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}

	var rotatedN, initN, rotN, failedN int
	for _, r := range results {
		if r.err != nil {
			failedN++
			log.Printf("FAILED: %s: %v", r.model, r.err)
			continue
		}
		if r.rotated {
			rotatedN++
		}
		if r.renderedInit {
			initN++
		}
		if r.renderedRotate {
			rotN++
		}
	}

	log.Printf("done: fetched=%d fetch_failed=%d found=%d rotated=%d rendered_initial=%d rendered_rotated=%d failed=%d elapsed=%s",
		fetchedN, fetchFailedN, len(results), rotatedN, initN, rotN, failedN, time.Since(start).Round(time.Second))

	if failedN > 0 || fetchFailedN > 0 {
		os.Exit(1)
	}
}
