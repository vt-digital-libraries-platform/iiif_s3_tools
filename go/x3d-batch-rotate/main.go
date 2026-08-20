// Command x3d-batch-rotate takes a directory of X3D models (or downloads
// them from DynamoDB by identifier — see fetch.go) and, for each one,
// renders a 250x250 PNG thumbnail of the model rotated by a configured
// angle, using headless Chrome to run the same X3DOM viewer the DLP
// archive site uses. The rotation is applied in memory purely to produce
// the thumbnail; no rotated .x3d file or unrotated ("initial") render is
// written anywhere.
package main

import (
	"context"
	"flag"
	"fmt"
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
	// the usual rotate/render phase runs, and each identifier (not the
	// model's own filename) is used to name that model's thumbnail. See
	// fetch.go.
	IdentifiersFile     string `yaml:"identifiers_file"`
	Region              string `yaml:"region"`
	TableName           string `yaml:"table_name"`
	LookupMode          string `yaml:"lookup_mode"`
	PartitionKeyAttr    string `yaml:"partition_key_attr"`
	IdentifierAttr      string `yaml:"identifier_attr"`
	IdentifierPrefix    string `yaml:"identifier_prefix"`
	ArchiveOptionsField string `yaml:"archive_options_field"`

	Degrees float64 `yaml:"degrees"`
	Axis    string  `yaml:"axis"`

	Render               bool `yaml:"render"`
	ThumbnailSize        int  `yaml:"thumbnail_size"`
	RenderWaitSeconds    int  `yaml:"render_wait_seconds"`
	RenderTimeoutSeconds int  `yaml:"render_timeout_seconds"`

	X3domJSURL  string `yaml:"x3dom_js_url"`
	X3domCSSURL string `yaml:"x3dom_css_url"`

	ThumbnailSuffix string `yaml:"thumbnail_suffix"`

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
	if cfg.ThumbnailSize == 0 {
		cfg.ThumbnailSize = 250
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
	if cfg.ThumbnailSuffix == "" {
		cfg.ThumbnailSuffix = "_thumbnail.png"
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

// job pairs a model file sitting in InputDir with the identifier its
// thumbnail should be named after.
type job struct {
	identifier string
	modelName  string
}

// findModels returns the .x3d files directly inside dir, sorted.
func findModels(dir string) ([]string, error) {
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
		models = append(models, name)
	}
	sort.Strings(models)
	return models, nil
}

// localJobs builds jobs from whatever .x3d files are already sitting in
// input_dir (the no-identifiers_file case), using each file's basename
// (without extension) as its "identifier" for thumbnail naming.
func localJobs(inputDir string) ([]job, error) {
	models, err := findModels(inputDir)
	if err != nil {
		return nil, err
	}
	jobs := make([]job, len(models))
	for i, name := range models {
		jobs[i] = job{
			identifier: strings.TrimSuffix(name, filepath.Ext(name)),
			modelName:  name,
		}
	}
	return jobs, nil
}

type result struct {
	job      job
	rendered bool
	err      error
}

func run(cfg *Config, jobs []job) []result {
	var (
		results []result
		alloc   context.Context
		cancel  context.CancelFunc
		srv     *fileServer
	)

	if cfg.Render && !cfg.DryRun && len(jobs) > 0 {
		var err error
		srv, err = startFileServer(cfg.InputDir)
		if err != nil {
			return []result{{err: fmt.Errorf("starting local file server: %w", err)}}
		}
		defer srv.Close()

		alloc, cancel = newBrowserAllocator()
		defer cancel()
	}

	for _, j := range jobs {
		res := result{job: j}
		if err := processJob(cfg, alloc, srv, j); err != nil {
			res.err = err
		} else {
			res.rendered = cfg.Render && !cfg.DryRun
		}
		results = append(results, res)
	}

	return results
}

// processJob rotates j's model in memory, renders a thumbnail of the
// rotated model, and writes only that thumbnail to disk. The rotated X3D
// content is written to a scratch file inside input_dir just long enough
// for the headless browser to load it (relative texture references need
// a real served file to resolve against), then removed; it is never
// written to output_dir or left behind in input_dir.
func processJob(cfg *Config, browserCtx context.Context, srv *fileServer, j job) error {
	thumbPath := filepath.Join(cfg.OutputDir, j.identifier+cfg.ThumbnailSuffix)

	if !cfg.Render {
		log.Printf("skipped (render=false): %s", j.identifier)
		return nil
	}

	if cfg.DryRun {
		log.Printf("[dry-run] would rotate %s (%.2f deg about %s) and render %s", j.modelName, cfg.Degrees, cfg.Axis, thumbPath)
		return nil
	}

	srcPath := filepath.Join(cfg.InputDir, j.modelName)
	content, err := os.ReadFile(srcPath)
	if err != nil {
		return fmt.Errorf("reading %s: %w", j.modelName, err)
	}

	rotatedContent, err := wrapSceneInTransform(string(content), cfg.Axis, cfg.Degrees)
	if err != nil {
		return fmt.Errorf("rotating %s: %w", j.modelName, err)
	}

	scratchName := ".tmp_rotated_" + j.modelName
	scratchPath := filepath.Join(cfg.InputDir, scratchName)
	if err := os.WriteFile(scratchPath, []byte(rotatedContent), 0o644); err != nil {
		return fmt.Errorf("writing scratch rotated model for %s: %w", j.modelName, err)
	}
	defer os.Remove(scratchPath)

	rendered, err := renderModelPNG(browserCtx, srv, cfg, scratchName)
	if err != nil {
		return fmt.Errorf("rendering rotated view of %s: %w", j.modelName, err)
	}
	png, err := squareThumbnail(rendered, cfg.ThumbnailSize)
	if err != nil {
		return fmt.Errorf("preparing thumbnail for %s: %w", j.modelName, err)
	}

	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return fmt.Errorf("creating output dir: %w", err)
	}
	if err := os.WriteFile(thumbPath, png, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", thumbPath, err)
	}
	log.Printf("rendered: %s", thumbPath)

	return nil
}

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML config file")
	dryRun := flag.Bool("dry-run", false, "report planned actions without writing or rendering anything")
	noRender := flag.Bool("no-render", false, "skip rotating/rendering; only run the fetch phase, if any")
	inputDir := flag.String("input", "", "override input_dir from the config file")
	outputDir := flag.String("output", "", "override output_dir from the config file")
	degrees := flag.Float64("degrees", 0, "override degrees from the config file (0 means: use config value)")
	identifiersFile := flag.String("identifiers", "", "override identifiers_file from the config file; fetch models from DynamoDB before rendering")
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

	var jobs []job
	var fetchedN, fetchFailedN int
	if cfg.IdentifiersFile != "" {
		jobs, fetchFailedN, err = fetchAll(cfg)
		if err != nil {
			log.Fatalf("fatal: %v", err)
		}
		fetchedN = len(jobs)
	} else {
		jobs, err = localJobs(cfg.InputDir)
		if err != nil {
			log.Fatalf("fatal: %v", err)
		}
	}

	results := run(cfg, jobs)

	var renderedN, failedN int
	for _, r := range results {
		if r.err != nil {
			failedN++
			log.Printf("FAILED: %s: %v", r.job.identifier, r.err)
			continue
		}
		if r.rendered {
			renderedN++
		}
	}

	log.Printf("done: fetched=%d fetch_failed=%d found=%d rendered=%d failed=%d elapsed=%s",
		fetchedN, fetchFailedN, len(jobs), renderedN, failedN, time.Since(start).Round(time.Second))

	if failedN > 0 || fetchFailedN > 0 {
		os.Exit(1)
	}
}
