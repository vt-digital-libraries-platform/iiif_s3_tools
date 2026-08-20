package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ddbAPI is the subset of *dynamodb.Client this tool needs. Depending on
// an interface rather than *dynamodb.Client directly lets tests substitute
// a fake implementation without real AWS credentials or network access.
type ddbAPI interface {
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
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

// fetchDDBItem looks up a single DynamoDB item for identifier, using
// either a direct GetItem (identifier is the table's partition key value)
// or a Scan with a filter expression (identifier is the value of some
// other attribute), per cfg.LookupMode.
func fetchDDBItem(ctx context.Context, client ddbAPI, cfg *Config, identifier string) (map[string]types.AttributeValue, error) {
	key := cfg.IdentifierPrefix + identifier

	switch cfg.LookupMode {
	case "get_item":
		out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(cfg.TableName),
			Key: map[string]types.AttributeValue{
				cfg.PartitionKeyAttr: &types.AttributeValueMemberS{Value: key},
			},
		})
		if err != nil {
			return nil, fmt.Errorf("dynamodb GetItem: %w", err)
		}
		if out.Item == nil {
			return nil, fmt.Errorf("no item found with %s=%q", cfg.PartitionKeyAttr, key)
		}
		return out.Item, nil

	case "scan":
		input := &dynamodb.ScanInput{
			TableName:                 aws.String(cfg.TableName),
			FilterExpression:          aws.String("#a = :v"),
			ExpressionAttributeNames:  map[string]string{"#a": cfg.IdentifierAttr},
			ExpressionAttributeValues: map[string]types.AttributeValue{":v": &types.AttributeValueMemberS{Value: key}},
		}
		for {
			out, err := client.Scan(ctx, input)
			if err != nil {
				return nil, fmt.Errorf("dynamodb Scan: %w", err)
			}
			if len(out.Items) > 0 {
				return out.Items[0], nil
			}
			if out.LastEvaluatedKey == nil {
				break
			}
			input.ExclusiveStartKey = out.LastEvaluatedKey
		}
		return nil, fmt.Errorf("no item found with %s=%q", cfg.IdentifierAttr, key)

	default:
		return nil, fmt.Errorf("unknown lookup_mode %q (must be \"get_item\" or \"scan\")", cfg.LookupMode)
	}
}

// extractX3DConfigURL parses a DynamoDB item's archiveOptions attribute
// (itself a JSON string, e.g. an AppSync AWSJSON field) and returns the
// download URL at assets.x3d_config inside it.
func extractX3DConfigURL(item map[string]types.AttributeValue, archiveOptionsField string) (string, error) {
	attr, ok := item[archiveOptionsField]
	if !ok {
		return "", fmt.Errorf("item has no attribute %q", archiveOptionsField)
	}
	strAttr, ok := attr.(*types.AttributeValueMemberS)
	if !ok {
		return "", fmt.Errorf("attribute %q is not a String", archiveOptionsField)
	}

	var parsed struct {
		Assets struct {
			X3DConfig string `json:"x3d_config"`
		} `json:"assets"`
	}
	if err := json.Unmarshal([]byte(strAttr.Value), &parsed); err != nil {
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
func fetchModel(ctx context.Context, client ddbAPI, httpClient *http.Client, cfg *Config, identifier string) (string, error) {
	item, err := fetchDDBItem(ctx, client, cfg, identifier)
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
// identifier listed in cfg.IdentifiersFile, and returns a job per
// successfully-fetched model (pairing the identifier with the model's
// downloaded filename, for the render phase that follows) plus a count of
// identifiers that failed. It is best-effort: a failure for one identifier
// is logged and does not stop the others.
func fetchAll(cfg *Config) (jobs []job, failed int, err error) {
	ids, err := loadIdentifiers(cfg.IdentifiersFile)
	if err != nil {
		return nil, 0, err
	}

	if cfg.DryRun {
		for _, id := range ids {
			log.Printf("[dry-run] would fetch identifier %q via DynamoDB (%s) and download its model+texture into %s", id, cfg.LookupMode, cfg.InputDir)
		}
		return nil, 0, nil
	}

	if err := os.MkdirAll(cfg.InputDir, 0o755); err != nil {
		return nil, 0, fmt.Errorf("creating input dir: %w", err)
	}

	ctx := context.Background()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, 0, fmt.Errorf("loading AWS config: %w", err)
	}
	client := dynamodb.NewFromConfig(awsCfg)

	httpClient := &http.Client{Timeout: 2 * time.Minute}

	for _, id := range ids {
		name, err := fetchModel(ctx, client, httpClient, cfg, id)
		if err != nil {
			failed++
			log.Printf("FAILED to fetch %q: %v", id, err)
			continue
		}
		jobs = append(jobs, job{identifier: id, modelName: name})
		log.Printf("fetched: %s -> %s", id, name)
	}

	return jobs, failed, nil
}
