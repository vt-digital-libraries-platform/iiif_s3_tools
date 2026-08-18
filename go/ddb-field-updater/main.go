// Command ddb-field-updater scans a DynamoDB table and, for every item whose
// specified field begins with a configured prefix, rewrites that prefix to a
// new value and writes the item back to the table.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Region      string `yaml:"region"`
	TableName   string `yaml:"table_name"`
	FieldName   string `yaml:"field_name"`
	MatchPrefix string `yaml:"match_prefix"`
	NewValue    string `yaml:"new_value"`
	DryRun      bool   `yaml:"dry_run"`

	// IsJSON indicates that the attribute named by FieldName is a JSON
	// string (e.g. an AppSync AWSJSON field, stored in DynamoDB as a plain
	// String attribute containing JSON text) rather than a plain string.
	// When true, the match/replace is applied to the nested field at
	// JSONFieldPath inside the parsed JSON, and the whole blob is
	// re-serialized back into FieldName.
	IsJSON        bool   `yaml:"is_json"`
	JSONFieldPath string `yaml:"json_field_path"`

	// jsonPathSegments is JSONFieldPath split on "." and is populated by
	// loadConfig; not read from YAML directly.
	jsonPathSegments []string `yaml:"-"`
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

	var missing []string
	if cfg.Region == "" {
		missing = append(missing, "region")
	}
	if cfg.TableName == "" {
		missing = append(missing, "table_name")
	}
	if cfg.FieldName == "" {
		missing = append(missing, "field_name")
	}
	if cfg.MatchPrefix == "" {
		missing = append(missing, "match_prefix")
	}
	if cfg.IsJSON && cfg.JSONFieldPath == "" {
		missing = append(missing, "json_field_path (required when is_json is true)")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("config file is missing required field(s): %s", strings.Join(missing, ", "))
	}

	if cfg.IsJSON {
		cfg.jsonPathSegments = strings.Split(cfg.JSONFieldPath, ".")
	}

	return &cfg, nil
}

// getNestedString walks a path of map keys / slice indices through a value
// produced by json.Unmarshal (map[string]interface{}, []interface{}, or a
// scalar) and returns the string found there, if any.
func getNestedString(v interface{}, path []string) (string, bool) {
	cur := v
	for _, seg := range path {
		switch node := cur.(type) {
		case map[string]interface{}:
			val, ok := node[seg]
			if !ok {
				return "", false
			}
			cur = val
		case []interface{}:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(node) {
				return "", false
			}
			cur = node[idx]
		default:
			return "", false
		}
	}
	str, ok := cur.(string)
	return str, ok
}

// setNestedString walks the same kind of path as getNestedString and
// overwrites the string value at the end of it. Returns false if the path
// doesn't resolve to an existing location.
func setNestedString(v interface{}, path []string, newVal string) bool {
	if len(path) == 0 {
		return false
	}
	cur := v
	for _, seg := range path[:len(path)-1] {
		switch node := cur.(type) {
		case map[string]interface{}:
			val, ok := node[seg]
			if !ok {
				return false
			}
			cur = val
		case []interface{}:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(node) {
				return false
			}
			cur = node[idx]
		default:
			return false
		}
	}

	last := path[len(path)-1]
	switch node := cur.(type) {
	case map[string]interface{}:
		if _, ok := node[last]; !ok {
			return false
		}
		node[last] = newVal
		return true
	case []interface{}:
		idx, err := strconv.Atoi(last)
		if err != nil || idx < 0 || idx >= len(node) {
			return false
		}
		node[idx] = newVal
		return true
	default:
		return false
	}
}

// getNestedAttrString walks a path of map keys / list indices through native
// DynamoDB AttributeValue nesting (M and L types) and returns the string
// found at the end of it, if any.
func getNestedAttrString(root map[string]types.AttributeValue, path []string) (string, bool) {
	if len(path) == 0 {
		return "", false
	}
	var cur types.AttributeValue = &types.AttributeValueMemberM{Value: root}
	for _, seg := range path {
		switch node := cur.(type) {
		case *types.AttributeValueMemberM:
			val, ok := node.Value[seg]
			if !ok {
				return "", false
			}
			cur = val
		case *types.AttributeValueMemberL:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(node.Value) {
				return "", false
			}
			cur = node.Value[idx]
		default:
			return "", false
		}
	}
	strAV, ok := cur.(*types.AttributeValueMemberS)
	if !ok {
		return "", false
	}
	return strAV.Value, true
}

// setNestedAttrString walks the same kind of path as getNestedAttrString and
// overwrites the String attribute value at the end of it in place. Returns
// false if the path doesn't resolve to an existing location.
func setNestedAttrString(root map[string]types.AttributeValue, path []string, newVal string) bool {
	if len(path) == 0 {
		return false
	}
	var cur types.AttributeValue = &types.AttributeValueMemberM{Value: root}
	for _, seg := range path[:len(path)-1] {
		switch node := cur.(type) {
		case *types.AttributeValueMemberM:
			val, ok := node.Value[seg]
			if !ok {
				return false
			}
			cur = val
		case *types.AttributeValueMemberL:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(node.Value) {
				return false
			}
			cur = node.Value[idx]
		default:
			return false
		}
	}

	last := path[len(path)-1]
	switch node := cur.(type) {
	case *types.AttributeValueMemberM:
		if _, ok := node.Value[last]; !ok {
			return false
		}
		node.Value[last] = &types.AttributeValueMemberS{Value: newVal}
		return true
	case *types.AttributeValueMemberL:
		idx, err := strconv.Atoi(last)
		if err != nil || idx < 0 || idx >= len(node.Value) {
			return false
		}
		node.Value[idx] = &types.AttributeValueMemberS{Value: newVal}
		return true
	default:
		return false
	}
}

// evaluateItem checks whether item matches cfg's field/prefix rule and, if
// so, mutates item in place with the replacement value. It returns whether a
// match was found along with the old and new string values (the whole field
// for plain-string fields, or just the nested value for AWSJSON fields).
//
// When cfg.IsJSON is true, field_name's attribute can be stored either way
// AppSync maps an AWSJSON scalar into DynamoDB: as a String attribute
// holding literal JSON text, or as a native nested Map/List attribute. Both
// are handled transparently based on the attribute's actual type.
func evaluateItem(cfg *Config, item map[string]types.AttributeValue) (matched bool, oldVal, newVal string, err error) {
	av, ok := item[cfg.FieldName]
	if !ok {
		return false, "", "", nil
	}

	if !cfg.IsJSON {
		strAV, ok := av.(*types.AttributeValueMemberS)
		if !ok {
			return false, "", "", nil
		}
		if !strings.HasPrefix(strAV.Value, cfg.MatchPrefix) {
			return false, "", "", nil
		}
		oldVal = strAV.Value
		newVal = cfg.NewValue + strings.TrimPrefix(oldVal, cfg.MatchPrefix)
		item[cfg.FieldName] = &types.AttributeValueMemberS{Value: newVal}
		return true, oldVal, newVal, nil
	}

	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		// AWSJSON stored as a JSON-encoded string. Parse it, look up the
		// nested field, and if it matches, rewrite it and re-serialize the
		// whole blob back into the attribute.
		var parsed interface{}
		dec := json.NewDecoder(strings.NewReader(v.Value))
		dec.UseNumber() // preserve original numeric literal formatting on re-marshal
		if err := dec.Decode(&parsed); err != nil {
			return false, "", "", fmt.Errorf("field %q is not valid JSON: %w", cfg.FieldName, err)
		}

		nestedVal, ok := getNestedString(parsed, cfg.jsonPathSegments)
		if !ok || !strings.HasPrefix(nestedVal, cfg.MatchPrefix) {
			return false, "", "", nil
		}

		oldVal = nestedVal
		newVal = cfg.NewValue + strings.TrimPrefix(nestedVal, cfg.MatchPrefix)
		if !setNestedString(parsed, cfg.jsonPathSegments, newVal) {
			return false, "", "", fmt.Errorf("failed to set json_field_path %q", cfg.JSONFieldPath)
		}

		newBlob, err := json.Marshal(parsed)
		if err != nil {
			return false, "", "", fmt.Errorf("re-marshaling JSON for field %q: %w", cfg.FieldName, err)
		}
		item[cfg.FieldName] = &types.AttributeValueMemberS{Value: string(newBlob)}
		return true, oldVal, newVal, nil

	case *types.AttributeValueMemberM:
		// AWSJSON stored as a native nested Map attribute. Mutating v.Value
		// mutates the same map item[cfg.FieldName] already points to, so no
		// reassignment into item is needed.
		nestedVal, ok := getNestedAttrString(v.Value, cfg.jsonPathSegments)
		if !ok || !strings.HasPrefix(nestedVal, cfg.MatchPrefix) {
			return false, "", "", nil
		}

		oldVal = nestedVal
		newVal = cfg.NewValue + strings.TrimPrefix(nestedVal, cfg.MatchPrefix)
		if !setNestedAttrString(v.Value, cfg.jsonPathSegments, newVal) {
			return false, "", "", fmt.Errorf("failed to set json_field_path %q", cfg.JSONFieldPath)
		}
		return true, oldVal, newVal, nil

	default:
		return false, "", "", fmt.Errorf("field %q has unsupported attribute type %T for is_json", cfg.FieldName, av)
	}
}

// keyNames returns the table's key attribute names (partition key, and sort
// key if present) so matched/updated items can be logged in a readable way.
func keyNames(ctx context.Context, client *dynamodb.Client, table string) ([]string, error) {
	out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
	if err != nil {
		return nil, fmt.Errorf("describing table %q: %w", table, err)
	}

	names := make([]string, 0, len(out.Table.KeySchema))
	for _, k := range out.Table.KeySchema {
		names = append(names, aws.ToString(k.AttributeName))
	}
	return names, nil
}

func describeItemKey(item map[string]types.AttributeValue, keys []string) string {
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		if av, ok := item[k]; ok {
			parts = append(parts, fmt.Sprintf("%s=%s", k, attrValueString(av)))
		}
	}
	return strings.Join(parts, ", ")
}

func attrValueString(av types.AttributeValue) string {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		return v.Value
	default:
		return fmt.Sprintf("%v", av)
	}
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
	client := dynamodb.NewFromConfig(awsCfg)

	keys, err := keyNames(ctx, client, cfg.TableName)
	if err != nil {
		log.Fatalf("%v", err)
	}

	mode := "LIVE (records will be updated)"
	if dryRun {
		mode = "DRY RUN (no records will be updated)"
	}
	target := cfg.FieldName
	if cfg.IsJSON {
		target = fmt.Sprintf("%s (AWSJSON) -> %s", cfg.FieldName, cfg.JSONFieldPath)
	}
	log.Printf("table=%s field=%s match_prefix=%q new_value=%q mode=%s", cfg.TableName, target, cfg.MatchPrefix, cfg.NewValue, mode)

	var scanned, matched, updated, failed int

	paginator := dynamodb.NewScanPaginator(client, &dynamodb.ScanInput{
		TableName: aws.String(cfg.TableName),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			log.Fatalf("scanning table: %v", err)
		}

		for _, item := range page.Items {
			scanned++
			keyDesc := describeItemKey(item, keys)

			isMatch, oldVal, newVal, err := evaluateItem(cfg, item)
			if err != nil {
				failed++
				log.Printf("ERROR evaluating (%s): %v", keyDesc, err)
				continue
			}
			if !isMatch {
				continue
			}

			matched++

			if dryRun {
				log.Printf("[DRY RUN] would update (%s): %s: %q -> %q", keyDesc, target, oldVal, newVal)
				continue
			}

			_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(cfg.TableName),
				Item:      item,
			})
			if err != nil {
				failed++
				log.Printf("ERROR updating (%s): %v", keyDesc, err)
				continue
			}

			updated++
			log.Printf("updated (%s): %s: %q -> %q", keyDesc, target, oldVal, newVal)
		}
	}

	if dryRun {
		log.Printf("done: scanned=%d matched=%d (dry run, nothing written)", scanned, matched)
	} else {
		log.Printf("done: scanned=%d matched=%d updated=%d failed=%d", scanned, matched, updated, failed)
	}

	if failed > 0 {
		os.Exit(1)
	}
}
