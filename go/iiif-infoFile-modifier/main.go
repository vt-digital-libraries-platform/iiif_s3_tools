// Command iiif-infoFile-modifier finds every "info.json" object at the
// default per-item tile layout under a configured S3 collection prefix
// (<collection_prefix>/<tiles_dir_name>/<item_identifier>-<index>/info.json),
// writes a "backup_info.json" copy of it at the same key location, then
// rewrites the info.json object's JSON content to match a target format and
// writes it back to its original key.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Region           string `yaml:"region"`
	Bucket           string `yaml:"bucket"`
	CollectionPrefix string `yaml:"collection_prefix"`
	InfoFileName     string `yaml:"info_file_name"`
	BackupFileName   string `yaml:"backup_file_name"`
	TilesDirName     string `yaml:"tiles_dir_name"`
	DryRun           bool   `yaml:"dry_run"`
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

	if cfg.InfoFileName == "" {
		cfg.InfoFileName = "info.json"
	}
	if cfg.BackupFileName == "" {
		cfg.BackupFileName = "backup_info.json"
	}
	if cfg.TilesDirName == "" {
		cfg.TilesDirName = "tiles"
	}

	var missing []string
	if cfg.Region == "" {
		missing = append(missing, "region")
	}
	if cfg.Bucket == "" {
		missing = append(missing, "bucket")
	}
	if cfg.CollectionPrefix == "" {
		missing = append(missing, "collection_prefix")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("config file is missing required field(s): %s", strings.Join(missing, ", "))
	}

	if !strings.HasSuffix(cfg.CollectionPrefix, "/") {
		cfg.CollectionPrefix += "/"
	}

	return &cfg, nil
}

// requiredContext, requiredProtocol, and requiredProfile are fixed values
// every corrected "tiles/" info.json declares, regardless of what the input
// file had; the tiler behind these files always speaks IIIF Image API 2
// level0, serving JPEG tiles at default quality.
const requiredContext = "http://iiif.io/api/image/2/context.json"
const requiredProtocol = "http://iiif.io/api/image"

var requiredProfile = []interface{}{
	"http://iiif.io/api/image/2/level0.json",
	profileExtra{
		Formats:   []string{"jpg"},
		Qualities: []string{"default"},
		Supports:  []string{"cors", "baseUriRedirect"},
	},
}

type profileExtra struct {
	Formats   []string `json:"formats"`
	Qualities []string `json:"qualities"`
	Supports  []string `json:"supports"`
}

// inputInfo is the shape of a "tiles/" info.json as originally written; only
// @id, width, height, and tiles are read from it, everything else is
// replaced with fixed corrected values.
type inputInfo struct {
	ID     string      `json:"@id"`
	Width  int         `json:"width"`
	Height int         `json:"height"`
	Tiles  []inputTile `json:"tiles"`
}

type inputTile struct {
	Width        int   `json:"width"`
	ScaleFactors []int `json:"scaleFactors"`
}

// outputInfo is the corrected shape: no "sizes" array, fixed
// @context/protocol/profile, and tiles[] entries reordered to
// scaleFactors, then width.
type outputInfo struct {
	Context  string        `json:"@context"`
	ID       string        `json:"@id"`
	Profile  []interface{} `json:"profile"`
	Protocol string        `json:"protocol"`
	Tiles    []outputTile  `json:"tiles"`
	Width    int           `json:"width"`
	Height   int           `json:"height"`
}

type outputTile struct {
	ScaleFactors []int `json:"scaleFactors"`
	Width        int   `json:"width"`
}

// transformInfoJSON converts a "tiles/" info.json object's raw bytes from
// its original format to the corrected target format: @context, protocol,
// and profile are set to fixed values, the "sizes" array is dropped, and
// each tiles[] entry is reordered to scaleFactors, then width. @id, width,
// height, and tile values are preserved from the input.
func transformInfoJSON(data []byte) ([]byte, error) {
	var in inputInfo
	if err := json.Unmarshal(data, &in); err != nil {
		return nil, fmt.Errorf("parsing info.json: %w", err)
	}

	tiles := make([]outputTile, 0, len(in.Tiles))
	for _, t := range in.Tiles {
		tiles = append(tiles, outputTile{
			ScaleFactors: t.ScaleFactors,
			Width:        t.Width,
		})
	}

	out := outputInfo{
		Context:  requiredContext,
		ID:       in.ID,
		Profile:  requiredProfile,
		Protocol: requiredProtocol,
		Tiles:    tiles,
		Width:    in.Width,
		Height:   in.Height,
	}

	newData, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("re-marshaling info.json: %w", err)
	}
	return append(newData, '\n'), nil
}

// findInfoObjects lists every object under tilesPrefix in bucket whose key
// matches the default per-item layout tilesPrefix + "<item_identifier>-<index>/" +
// infoFileName, i.e. exactly one directory segment (the item's tile
// directory) between tilesPrefix and the filename. The IIIF spec places
// other, differently-formatted info.json files outside of tilesPrefix (e.g.
// presentation manifests); those are intentionally left alone.
func findInfoObjects(ctx context.Context, client *s3.Client, bucket, tilesPrefix, infoFileName string) ([]string, error) {
	var keys []string

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(tilesPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing objects under %q: %w", tilesPrefix, err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			rest := strings.TrimPrefix(key, tilesPrefix)
			segs := strings.Split(rest, "/")
			if len(segs) != 2 || segs[0] == "" || segs[1] != infoFileName {
				continue
			}
			keys = append(keys, key)
		}
	}

	return keys, nil
}

// backupKeyFor returns the backup object key that sits alongside infoKey at
// the same S3 "directory" (key prefix).
func backupKeyFor(infoKey, infoFileName, backupFileName string) string {
	dir := strings.TrimSuffix(infoKey, infoFileName)
	return dir + backupFileName
}

// backupObject creates backupKey as a server-side copy of srcKey within the
// same bucket.
func backupObject(ctx context.Context, client *s3.Client, bucket, srcKey, backupKey string) error {
	copySource := url.PathEscape(bucket + "/" + srcKey)
	_, err := client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(backupKey),
		CopySource: aws.String(copySource),
	})
	if err != nil {
		return fmt.Errorf("copying %q to %q: %w", srcKey, backupKey, err)
	}
	return nil
}

// downloadObject fetches the full contents of key in bucket.
func downloadObject(ctx context.Context, client *s3.Client, bucket, key string) ([]byte, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("getting %q: %w", key, err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("reading %q: %w", key, err)
	}
	return data, nil
}

// uploadObject writes data to key in bucket, overwriting any existing
// object at that key.
func uploadObject(ctx context.Context, client *s3.Client, bucket, key string, data []byte) error {
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("putting %q: %w", key, err)
	}
	return nil
}

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML config file")
	dryRunFlag := flag.Bool("dry-run", false, "force dry-run mode (scan and report only, no writes); overrides dry_run: false in the config file")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	dryRun := cfg.DryRun || *dryRunFlag

	ctx := context.Background()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		log.Fatalf("loading AWS config: %v", err)
	}
	client := s3.NewFromConfig(awsCfg)

	mode := "LIVE (objects will be written)"
	if dryRun {
		mode = "DRY RUN (no objects will be written)"
	}
	tilesPrefix := cfg.CollectionPrefix + cfg.TilesDirName + "/"

	log.Printf("bucket=%s tiles_prefix=%s info_file_name=%s backup_file_name=%s mode=%s",
		cfg.Bucket, tilesPrefix, cfg.InfoFileName, cfg.BackupFileName, mode)

	infoKeys, err := findInfoObjects(ctx, client, cfg.Bucket, tilesPrefix, cfg.InfoFileName)
	if err != nil {
		log.Fatalf("%v", err)
	}
	log.Printf("found %d %s object(s) under %s<item_identifier>-<index>/", len(infoKeys), cfg.InfoFileName, tilesPrefix)

	var backedUp, modified, failed int

	for _, infoKey := range infoKeys {
		backupKey := backupKeyFor(infoKey, cfg.InfoFileName, cfg.BackupFileName)

		if dryRun {
			log.Printf("[DRY RUN] would back up %q -> %q", infoKey, backupKey)
		} else {
			if err := backupObject(ctx, client, cfg.Bucket, infoKey, backupKey); err != nil {
				failed++
				log.Printf("ERROR backing up %q: %v", infoKey, err)
				continue
			}
			backedUp++
			log.Printf("backed up %q -> %q", infoKey, backupKey)
		}
	}

	if failed > 0 {
		log.Printf("aborting before modification step: %d backup(s) failed", failed)
		os.Exit(1)
	}

	for _, infoKey := range infoKeys {
		data, err := downloadObject(ctx, client, cfg.Bucket, infoKey)
		if err != nil {
			failed++
			log.Printf("ERROR downloading %q: %v", infoKey, err)
			continue
		}

		newData, err := transformInfoJSON(data)
		if err != nil {
			failed++
			log.Printf("ERROR transforming %q: %v", infoKey, err)
			continue
		}

		if dryRun {
			log.Printf("[DRY RUN] would write modified %q (%d bytes -> %d bytes)", infoKey, len(data), len(newData))
			continue
		}

		if err := uploadObject(ctx, client, cfg.Bucket, infoKey, newData); err != nil {
			failed++
			log.Printf("ERROR uploading %q: %v", infoKey, err)
			continue
		}

		modified++
		log.Printf("modified %q", infoKey)
	}

	if dryRun {
		log.Printf("done: found=%d (dry run, nothing written)", len(infoKeys))
	} else {
		log.Printf("done: found=%d backed_up=%d modified=%d failed=%d", len(infoKeys), backedUp, modified, failed)
	}

	if failed > 0 {
		os.Exit(1)
	}
}
