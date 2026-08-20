package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// ddbAttr is a DynamoDB low-level (AttributeValue) JSON representation, as
// produced by `aws dynamodb get-item`/`scan` output — only the subset of
// types this tool needs to read (String) is modeled.
type ddbAttr struct {
	S *string `json:"S,omitempty"`
}

type ddbItem map[string]ddbAttr

type ddbGetItemOutput struct {
	Item ddbItem `json:"Item"`
}

type ddbScanOutput struct {
	Items []ddbItem `json:"Items"`
}

// cliRunner runs an external command (normally the "aws" CLI) and returns
// its stdout. It is a variable, not a hardcoded exec.Command call, so tests
// can substitute a fake implementation without invoking a real process or
// needing AWS credentials.
var cliRunner = func(name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return nil, fmt.Errorf("%s %s: %s", name, strings.Join(args, " "), msg)
	}
	return stdout.Bytes(), nil
}

// loadIdentifiers reads a JSON file containing an array of identifier
// strings, e.g. ["368a8114", "abcd1234"].
func loadIdentifiers(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading identifiers file: %w", err)
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return nil, fmt.Errorf("parsing identifiers file as a JSON array of strings: %w", err)
	}
	return ids, nil
}

// fetchDDBItem looks up a single DynamoDB item for identifier, via the AWS
// CLI, using either a direct GetItem (identifier is the table's partition
// key value) or a Scan with a filter expression (identifier is the value
// of some other attribute), per cfg.LookupMode.
func fetchDDBItem(cfg *Config, identifier string) (ddbItem, error) {
	key := cfg.IdentifierPrefix + identifier

	switch cfg.LookupMode {
	case "get_item":
		keyJSON, err := json.Marshal(map[string]ddbAttr{
			cfg.PartitionKeyAttr: {S: &key},
		})
		if err != nil {
			return nil, err
		}
		out, err := cliRunner("aws", "dynamodb", "get-item",
			"--table-name", cfg.TableName,
			"--region", cfg.Region,
			"--key", string(keyJSON),
		)
		if err != nil {
			return nil, fmt.Errorf("aws dynamodb get-item: %w", err)
		}
		var res ddbGetItemOutput
		if err := json.Unmarshal(out, &res); err != nil {
			return nil, fmt.Errorf("parsing get-item output: %w", err)
		}
		if res.Item == nil {
			return nil, fmt.Errorf("no item found with %s=%q", cfg.PartitionKeyAttr, key)
		}
		return res.Item, nil

	case "scan":
		namesJSON, err := json.Marshal(map[string]string{"#a": cfg.IdentifierAttr})
		if err != nil {
			return nil, err
		}
		valuesJSON, err := json.Marshal(map[string]ddbAttr{":v": {S: &key}})
		if err != nil {
			return nil, err
		}
		out, err := cliRunner("aws", "dynamodb", "scan",
			"--table-name", cfg.TableName,
			"--region", cfg.Region,
			"--filter-expression", "#a = :v",
			"--expression-attribute-names", string(namesJSON),
			"--expression-attribute-values", string(valuesJSON),
		)
		if err != nil {
			return nil, fmt.Errorf("aws dynamodb scan: %w", err)
		}
		var res ddbScanOutput
		if err := json.Unmarshal(out, &res); err != nil {
			return nil, fmt.Errorf("parsing scan output: %w", err)
		}
		if len(res.Items) == 0 {
			return nil, fmt.Errorf("no item found with %s=%q", cfg.IdentifierAttr, key)
		}
		return res.Items[0], nil

	default:
		return nil, fmt.Errorf("unknown lookup_mode %q (must be \"get_item\" or \"scan\")", cfg.LookupMode)
	}
}

// extractX3DConfigURL parses a DynamoDB item's archiveOptions attribute
// (itself a JSON string, e.g. an AppSync AWSJSON field) and returns the
// download URL at assets.x3d_config inside it.
func extractX3DConfigURL(item ddbItem, archiveOptionsField string) (string, error) {
	attr, ok := item[archiveOptionsField]
	if !ok || attr.S == nil {
		return "", fmt.Errorf("item has no string attribute %q", archiveOptionsField)
	}

	var parsed struct {
		Assets struct {
			X3DConfig string `json:"x3d_config"`
		} `json:"assets"`
	}
	if err := json.Unmarshal([]byte(*attr.S), &parsed); err != nil {
		return "", fmt.Errorf("parsing %s as JSON: %w", archiveOptionsField, err)
	}
	if parsed.Assets.X3DConfig == "" {
		return "", fmt.Errorf("%s JSON has no assets.x3d_config", archiveOptionsField)
	}
	return parsed.Assets.X3DConfig, nil
}

// resolveRelativeURL resolves ref (which may be a bare relative filename,
// e.g. an X3D <ImageTexture url="...">) against baseURL (the URL the
// containing document was itself fetched from).
func resolveRelativeURL(baseURL, ref string) (string, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parsing base URL %q: %w", baseURL, err)
	}
	rel, err := url.Parse(ref)
	if err != nil {
		return "", fmt.Errorf("parsing reference URL %q: %w", ref, err)
	}
	return base.ResolveReference(rel).String(), nil
}

// downloadFile GETs srcURL and writes it to destPath, atomically (via a
// temp file + rename) and without clobbering an existing destination.
func downloadFile(client *http.Client, srcURL, destPath string) error {
	if _, err := os.Stat(destPath); err == nil {
		return nil
	}

	resp, err := client.Get(srcURL)
	if err != nil {
		return fmt.Errorf("GET %s: %w", srcURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: unexpected status %s", srcURL, resp.Status)
	}

	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return err
	}
	out, err := os.CreateTemp(filepath.Dir(destPath), ".tmp-download-*")
	if err != nil {
		return err
	}
	tmpName := out.Name()
	defer os.Remove(tmpName)

	if _, err := io.Copy(out, resp.Body); err != nil {
		out.Close()
		return fmt.Errorf("writing %s: %w", destPath, err)
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, destPath)
}

// fetchModel looks up identifier in DynamoDB, resolves its X3D model URL
// (and, via the model's own <ImageTexture> reference, its texture URL),
// and downloads both into cfg.InputDir. It returns the downloaded model's
// filename (for logging) on success.
func fetchModel(cfg *Config, httpClient *http.Client, identifier string) (string, error) {
	item, err := fetchDDBItem(cfg, identifier)
	if err != nil {
		return "", err
	}

	x3dURL, err := extractX3DConfigURL(item, cfg.ArchiveOptionsField)
	if err != nil {
		return "", err
	}

	x3dName := filepath.Base(x3dURL)
	x3dPath := filepath.Join(cfg.InputDir, x3dName)
	if err := downloadFile(httpClient, x3dURL, x3dPath); err != nil {
		return "", fmt.Errorf("downloading model: %w", err)
	}

	content, err := os.ReadFile(x3dPath)
	if err != nil {
		return "", fmt.Errorf("reading downloaded model: %w", err)
	}
	for _, tex := range extractTextureURLs(string(content)) {
		if !isRelativeAssetURL(tex) {
			continue
		}
		texURL, err := resolveRelativeURL(x3dURL, tex)
		if err != nil {
			return "", fmt.Errorf("resolving texture URL %q: %w", tex, err)
		}
		texPath := filepath.Join(cfg.InputDir, filepath.Base(tex))
		if err := downloadFile(httpClient, texURL, texPath); err != nil {
			return "", fmt.Errorf("downloading texture %s: %w", tex, err)
		}
	}

	return x3dName, nil
}

// fetchAll downloads a model (and its texture) into cfg.InputDir for every
// identifier listed in cfg.IdentifiersFile. It is best-effort: a failure
// for one identifier is logged and does not stop the others, since the
// rotate/render phase that follows only ever looks at whatever .x3d files
// actually ended up in InputDir.
func fetchAll(cfg *Config) (fetched, failed int, err error) {
	ids, err := loadIdentifiers(cfg.IdentifiersFile)
	if err != nil {
		return 0, 0, err
	}

	if cfg.DryRun {
		for _, id := range ids {
			log.Printf("[dry-run] would fetch identifier %q via DynamoDB (%s) and download its model+texture into %s", id, cfg.LookupMode, cfg.InputDir)
		}
		return 0, 0, nil
	}

	if err := os.MkdirAll(cfg.InputDir, 0o755); err != nil {
		return 0, 0, fmt.Errorf("creating input dir: %w", err)
	}

	httpClient := &http.Client{Timeout: 2 * time.Minute}

	for _, id := range ids {
		name, err := fetchModel(cfg, httpClient, id)
		if err != nil {
			failed++
			log.Printf("FAILED to fetch %q: %v", id, err)
			continue
		}
		fetched++
		log.Printf("fetched: %s -> %s", id, name)
	}

	return fetched, failed, nil
}
