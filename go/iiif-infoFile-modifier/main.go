// Command iiif-infoFile-modifier finds every "info.json" object belonging to
// archives in a DynamoDB-tracked collection. It looks up the Collection
// record (by identifier) in a configured DynamoDB table to get the
// collection's id, queries the Archive table for every record whose
// collection matches that id, and for each archive identifier lists
// the "info.json" object(s) under the collection's default per-item tile
// layout (<collection_prefix>/<collection_identifier>/<tiles_dir_name>/
// <archive_identifier>-<index>/info.json). For each one found, it writes a
// "backup_info.json" copy at the same key location, then rewrites the
// info.json object's JSON content to match a target format and writes it
// back to its original key.
//
// Run with -rollback to reverse that: each discovered info.json (assumed to
// already be in the corrected/output format) is converted back to the
// original pre-transform ("input") format and written back to its key, and
// the corresponding backup_info.json is then deleted.
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
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Region               string `yaml:"region"`
	Bucket               string `yaml:"bucket"`
	CollectionPrefix     string `yaml:"collection_prefix"`
	InfoFileName         string `yaml:"info_file_name"`
	BackupFileName       string `yaml:"backup_file_name"`
	TilesDirName         string `yaml:"tiles_dir_name"`
	CollectionTable      string `yaml:"collection_table"`
	ArchiveTable         string `yaml:"archive_table"`
	CollectionIdentifier string `yaml:"collection_identifier"`
	DryRun               bool   `yaml:"dry_run"`
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
	if cfg.CollectionTable == "" {
		missing = append(missing, "collection_table")
	}
	if cfg.ArchiveTable == "" {
		missing = append(missing, "archive_table")
	}
	if cfg.CollectionIdentifier == "" {
		missing = append(missing, "collection_identifier")
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

// isAlreadyTransformed reports whether data's info.json content already
// matches the corrected/output format, so callers can avoid re-backing-up
// and re-transforming it: specifically, whether its profile's second
// (feature) element declares a "formats" key, which requiredProfile always
// adds and the original pre-transform format never has. Running the tool
// twice against the same collection without this check would back up an
// already-corrected file over top of the real original, destroying data
// (e.g. the "sizes" array) that can't be recovered afterward.
func isAlreadyTransformed(data []byte) (bool, error) {
	var probe struct {
		Profile []json.RawMessage `json:"profile"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return false, fmt.Errorf("parsing info.json: %w", err)
	}
	if len(probe.Profile) < 2 {
		return false, nil
	}

	var extra map[string]json.RawMessage
	if err := json.Unmarshal(probe.Profile[1], &extra); err != nil {
		return false, nil
	}

	_, hasFormats := extra["formats"]
	return hasFormats, nil
}

// rollbackProfileExtra is the profile "extra" object shape used by the
// original pre-transform info.json format: only "supports" (no
// "formats"/"qualities", unlike the corrected format's profile).
type rollbackProfileExtra struct {
	Supports []string `json:"supports"`
}

// rollbackProfile is the fixed pre-transform profile value, restoring
// "sizeByWhListed" to supports (dropped by the forward transform).
var rollbackProfile = []interface{}{
	"http://iiif.io/api/image/2/level0.json",
	rollbackProfileExtra{
		Supports: []string{"cors", "sizeByWhListed", "baseUriRedirect"},
	},
}

// rollbackSize is one entry of the pre-transform "sizes" array.
type rollbackSize struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

// backupSizes extracts just the "sizes" array from a backup_info.json
// file; the forward transform drops "sizes" and it can't be recovered from
// the corrected info.json alone, so a rollback must read it back out of the
// pre-transform backup instead.
type backupSizes struct {
	Sizes []rollbackSize `json:"sizes"`
}

// rollbackTile mirrors the pre-transform tiles[] entry field order (width
// before scaleFactors).
type rollbackTile struct {
	Width        int   `json:"width"`
	ScaleFactors []int `json:"scaleFactors"`
}

// rollbackInfo is the reconstructed pre-transform ("input format")
// info.json shape, mirroring the field order of the original tiler output.
type rollbackInfo struct {
	Context  string         `json:"@context"`
	ID       string         `json:"@id"`
	Protocol string         `json:"protocol"`
	Width    int            `json:"width"`
	Height   int            `json:"height"`
	Sizes    []rollbackSize `json:"sizes"`
	Profile  []interface{}  `json:"profile"`
	Tiles    []rollbackTile `json:"tiles"`
}

// rollbackInfoJSON converts an info.json's current (corrected/output
// format) bytes back to the tool's pre-transform ("input format") shape.
// @id, width, height, and tiles are read from currentData. "sizes" cannot
// be recovered from the corrected format (the forward transform drops it),
// so it's read from backupData — the pre-transform backup written before
// the original transform ran — instead. @context, protocol, and profile
// are restored to their fixed pre-transform values.
func rollbackInfoJSON(currentData, backupData []byte) ([]byte, error) {
	var current outputInfo
	if err := json.Unmarshal(currentData, &current); err != nil {
		return nil, fmt.Errorf("parsing current info.json: %w", err)
	}

	var backup backupSizes
	if err := json.Unmarshal(backupData, &backup); err != nil {
		return nil, fmt.Errorf("parsing backup_info.json: %w", err)
	}

	tiles := make([]rollbackTile, 0, len(current.Tiles))
	for _, t := range current.Tiles {
		tiles = append(tiles, rollbackTile{
			Width:        t.Width,
			ScaleFactors: t.ScaleFactors,
		})
	}

	out := rollbackInfo{
		Context:  requiredContext,
		ID:       current.ID,
		Protocol: requiredProtocol,
		Width:    current.Width,
		Height:   current.Height,
		Sizes:    backup.Sizes,
		Profile:  rollbackProfile,
		Tiles:    tiles,
	}

	newData, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("re-marshaling rolled-back info.json: %w", err)
	}
	return append(newData, '\n'), nil
}

// findCollectionID scans collectionTable for the item whose "identifier"
// attribute equals collectionIdentifier and returns that item's "id"
// attribute value. Returns an error if zero or more than one match is
// found, or if the matching record has no string "id" attribute.
func findCollectionID(ctx context.Context, client *dynamodb.Client, collectionTable, collectionIdentifier string) (string, error) {
	var found []map[string]types.AttributeValue

	paginator := dynamodb.NewScanPaginator(client, &dynamodb.ScanInput{
		TableName:        aws.String(collectionTable),
		FilterExpression: aws.String("identifier = :identifier"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":identifier": &types.AttributeValueMemberS{Value: collectionIdentifier},
		},
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return "", fmt.Errorf("scanning collection table %q: %w", collectionTable, err)
		}
		found = append(found, page.Items...)
	}

	if len(found) == 0 {
		return "", fmt.Errorf("no collection found in table %q with identifier %q", collectionTable, collectionIdentifier)
	}
	if len(found) > 1 {
		return "", fmt.Errorf("multiple (%d) collections found in table %q with identifier %q", len(found), collectionTable, collectionIdentifier)
	}

	idAttr, ok := found[0]["id"].(*types.AttributeValueMemberS)
	if !ok || idAttr.Value == "" {
		return "", fmt.Errorf("collection record (identifier=%q) in table %q is missing a string \"id\" field", collectionIdentifier, collectionTable)
	}

	return idAttr.Value, nil
}

// findArchiveIdentifiers scans archiveTable for every item whose
// "collection" attribute equals collectionID and returns the deduplicated
// set of matching items' "identifier" attribute values. Items missing a
// valid string "identifier" are skipped with a logged warning rather than
// aborting the whole run.
func findArchiveIdentifiers(ctx context.Context, client *dynamodb.Client, archiveTable, collectionID string) ([]string, error) {
	seen := make(map[string]struct{})
	var identifiers []string

	paginator := dynamodb.NewScanPaginator(client, &dynamodb.ScanInput{
		TableName:        aws.String(archiveTable),
		FilterExpression: aws.String("#collection = :collection"),
		ExpressionAttributeNames: map[string]string{
			"#collection": "collection",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":collection": &types.AttributeValueMemberS{Value: collectionID},
		},
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("scanning archive table %q: %w", archiveTable, err)
		}
		for _, item := range page.Items {
			idAttr, ok := item["identifier"].(*types.AttributeValueMemberS)
			if !ok || idAttr.Value == "" {
				log.Printf("WARNING: archive record in table %q (collection=%q) missing a string \"identifier\" field, skipping", archiveTable, collectionID)
				continue
			}
			if _, dup := seen[idAttr.Value]; dup {
				continue
			}
			seen[idAttr.Value] = struct{}{}
			identifiers = append(identifiers, idAttr.Value)
		}
	}

	return identifiers, nil
}

// findInfoObjects lists, for each archive identifier, every object under
// tilesPrefix in bucket whose key matches the default per-item layout
// tilesPrefix + "<archive_identifier>-<index>/" + infoFileName, i.e. exactly
// one directory segment (the archive's tile directory) between tilesPrefix
// and the filename. Each archive identifier may have more than one matching
// "-<index>" subdirectory, or none at all (e.g. tiles not yet generated).
// The IIIF spec places other, differently-formatted info.json files outside
// of tilesPrefix (e.g. presentation manifests); those are intentionally left
// alone.
func findInfoObjects(ctx context.Context, client *s3.Client, bucket, tilesPrefix, infoFileName string, archiveIdentifiers []string) ([]string, error) {
	var keys []string

	for _, identifier := range archiveIdentifiers {
		identifierPrefix := tilesPrefix + identifier + "-"

		paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			Prefix: aws.String(identifierPrefix),
		})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return nil, fmt.Errorf("listing objects under %q: %w", identifierPrefix, err)
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

// deleteObject removes key from bucket.
func deleteObject(ctx context.Context, client *s3.Client, bucket, key string) error {
	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("deleting %q: %w", key, err)
	}
	return nil
}

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML config file")
	dryRunFlag := flag.Bool("dry-run", false, "force dry-run mode (scan and report only, no writes); overrides dry_run: false in the config file")
	rollback := flag.Bool("rollback", false, "reverse mode: convert each discovered info.json from the corrected/output format back to the original input format, then delete its backup_info.json")
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
	ddbClient := dynamodb.NewFromConfig(awsCfg)

	mode := "LIVE (objects will be written)"
	if dryRun {
		mode = "DRY RUN (no objects will be written)"
	}
	if *rollback {
		mode = "ROLLBACK " + mode
	}
	collectionRootPrefix := cfg.CollectionPrefix + cfg.CollectionIdentifier + "/"
	tilesPrefix := collectionRootPrefix + cfg.TilesDirName + "/"

	log.Printf("bucket=%s tiles_prefix=%s info_file_name=%s backup_file_name=%s mode=%s",
		cfg.Bucket, tilesPrefix, cfg.InfoFileName, cfg.BackupFileName, mode)

	collectionID, err := findCollectionID(ctx, ddbClient, cfg.CollectionTable, cfg.CollectionIdentifier)
	if err != nil {
		log.Fatalf("%v", err)
	}
	log.Printf("found collection id=%s for collection_identifier=%s in table %s", collectionID, cfg.CollectionIdentifier, cfg.CollectionTable)

	archiveIdentifiers, err := findArchiveIdentifiers(ctx, ddbClient, cfg.ArchiveTable, collectionID)
	if err != nil {
		log.Fatalf("%v", err)
	}
	log.Printf("found %d archive identifier(s) in table %s for collection id=%s", len(archiveIdentifiers), cfg.ArchiveTable, collectionID)

	infoKeys, err := findInfoObjects(ctx, client, cfg.Bucket, tilesPrefix, cfg.InfoFileName, archiveIdentifiers)
	if err != nil {
		log.Fatalf("%v", err)
	}
	log.Printf("found %d %s object(s) under %s<archive_identifier>-<index>/", len(infoKeys), cfg.InfoFileName, tilesPrefix)

	if *rollback {
		runRollback(ctx, client, cfg, infoKeys, dryRun)
		return
	}
	runTransform(ctx, client, cfg, infoKeys, dryRun)
}

// pendingTransform is an info.json object confirmed (by isAlreadyTransformed)
// to still be in the original pre-transform format, and therefore safe to
// back up and rewrite.
type pendingTransform struct {
	key       string
	backupKey string
	data      []byte
}

// runTransform backs up, then transforms and writes back, each object in
// infoKeys that isn't already in the corrected/output format (the normal,
// forward mode of operation). Objects already in the corrected format are
// skipped entirely — neither backed up nor rewritten — so that running the
// tool again over an already-processed collection can't overwrite a
// backup_info.json with already-corrected content.
func runTransform(ctx context.Context, client *s3.Client, cfg *Config, infoKeys []string, dryRun bool) {
	var toProcess []pendingTransform
	var skipped, failed int

	for _, infoKey := range infoKeys {
		data, err := downloadObject(ctx, client, cfg.Bucket, infoKey)
		if err != nil {
			failed++
			log.Printf("ERROR downloading %q: %v", infoKey, err)
			continue
		}

		alreadyTransformed, err := isAlreadyTransformed(data)
		if err != nil {
			failed++
			log.Printf("ERROR inspecting %q: %v", infoKey, err)
			continue
		}
		if alreadyTransformed {
			skipped++
			log.Printf("skipping %q: already in corrected format, leaving its backup untouched", infoKey)
			continue
		}

		toProcess = append(toProcess, pendingTransform{
			key:       infoKey,
			backupKey: backupKeyFor(infoKey, cfg.InfoFileName, cfg.BackupFileName),
			data:      data,
		})
	}

	var backedUp, backupFailed int
	for _, p := range toProcess {
		if dryRun {
			log.Printf("[DRY RUN] would back up %q -> %q", p.key, p.backupKey)
			continue
		}
		if err := backupObject(ctx, client, cfg.Bucket, p.key, p.backupKey); err != nil {
			backupFailed++
			log.Printf("ERROR backing up %q: %v", p.key, err)
			continue
		}
		backedUp++
		log.Printf("backed up %q -> %q", p.key, p.backupKey)
	}
	failed += backupFailed

	if backupFailed > 0 {
		log.Printf("aborting before modification step: %d backup(s) failed", backupFailed)
		os.Exit(1)
	}

	var modified int
	for _, p := range toProcess {
		newData, err := transformInfoJSON(p.data)
		if err != nil {
			failed++
			log.Printf("ERROR transforming %q: %v", p.key, err)
			continue
		}

		if dryRun {
			log.Printf("[DRY RUN] would write modified %q (%d bytes -> %d bytes)", p.key, len(p.data), len(newData))
			continue
		}

		if err := uploadObject(ctx, client, cfg.Bucket, p.key, newData); err != nil {
			failed++
			log.Printf("ERROR uploading %q: %v", p.key, err)
			continue
		}

		modified++
		log.Printf("modified %q", p.key)
	}

	if dryRun {
		log.Printf("done: found=%d skipped=%d (dry run, nothing written)", len(infoKeys), skipped)
	} else {
		log.Printf("done: found=%d skipped=%d backed_up=%d modified=%d failed=%d", len(infoKeys), skipped, backedUp, modified, failed)
	}

	if failed > 0 {
		os.Exit(1)
	}
}

// runRollback converts each object in infoKeys from the corrected/output
// format back to the original input format (using its backup_info.json to
// recover the "sizes" field, which the forward transform drops), writes it
// back to its key, then deletes that key's backup_info.json.
func runRollback(ctx context.Context, client *s3.Client, cfg *Config, infoKeys []string, dryRun bool) {
	var rolledBack, failed int

	for _, infoKey := range infoKeys {
		backupKey := backupKeyFor(infoKey, cfg.InfoFileName, cfg.BackupFileName)

		currentData, err := downloadObject(ctx, client, cfg.Bucket, infoKey)
		if err != nil {
			failed++
			log.Printf("ERROR downloading %q: %v", infoKey, err)
			continue
		}

		backupData, err := downloadObject(ctx, client, cfg.Bucket, backupKey)
		if err != nil {
			failed++
			log.Printf("ERROR downloading backup %q: %v", backupKey, err)
			continue
		}

		newData, err := rollbackInfoJSON(currentData, backupData)
		if err != nil {
			failed++
			log.Printf("ERROR rolling back %q: %v", infoKey, err)
			continue
		}

		if dryRun {
			log.Printf("[DRY RUN] would roll back %q (%d bytes -> %d bytes) and remove backup %q", infoKey, len(currentData), len(newData), backupKey)
			continue
		}

		if err := uploadObject(ctx, client, cfg.Bucket, infoKey, newData); err != nil {
			failed++
			log.Printf("ERROR uploading rolled-back %q: %v", infoKey, err)
			continue
		}

		if err := deleteObject(ctx, client, cfg.Bucket, backupKey); err != nil {
			failed++
			log.Printf("ERROR removing backup %q: %v", backupKey, err)
			continue
		}

		rolledBack++
		log.Printf("rolled back %q, removed backup %q", infoKey, backupKey)
	}

	if dryRun {
		log.Printf("done: found=%d (dry run, nothing written)", len(infoKeys))
	} else {
		log.Printf("done: found=%d rolled_back=%d failed=%d", len(infoKeys), rolledBack, failed)
	}

	if failed > 0 {
		os.Exit(1)
	}
}
