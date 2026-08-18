package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestEvaluateItemPlainString(t *testing.T) {
	cfg := &Config{
		FieldName:   "identifier",
		MatchPrefix: "old-",
		NewValue:    "new-",
	}
	item := map[string]types.AttributeValue{
		"identifier": &types.AttributeValueMemberS{Value: "old-1234"},
	}

	matched, oldVal, newVal, err := evaluateItem(cfg, item)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Fatalf("expected match")
	}
	if oldVal != "old-1234" || newVal != "new-1234" {
		t.Fatalf("got old=%q new=%q", oldVal, newVal)
	}
	got := item["identifier"].(*types.AttributeValueMemberS).Value
	if got != "new-1234" {
		t.Fatalf("item not updated, got %q", got)
	}
}

func TestEvaluateItemNestedJSON(t *testing.T) {
	blob := `{"metadata":{"identifiers":{"primary":"old-abc","other":5},"unrelated":[1,2,3]},"top":"keep-me"}`
	cfg := &Config{
		FieldName:     "payload",
		MatchPrefix:   "old-",
		NewValue:      "new-",
		IsJSON:        true,
		JSONFieldPath: "metadata.identifiers.primary",
	}
	cfg.jsonPathSegments = []string{"metadata", "identifiers", "primary"}

	item := map[string]types.AttributeValue{
		"payload": &types.AttributeValueMemberS{Value: blob},
	}

	matched, oldVal, newVal, err := evaluateItem(cfg, item)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Fatalf("expected match")
	}
	if oldVal != "old-abc" || newVal != "new-abc" {
		t.Fatalf("got old=%q new=%q", oldVal, newVal)
	}

	newBlob := item["payload"].(*types.AttributeValueMemberS).Value
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(newBlob), &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	metadata := parsed["metadata"].(map[string]interface{})
	identifiers := metadata["identifiers"].(map[string]interface{})
	if identifiers["primary"] != "new-abc" {
		t.Fatalf("primary not updated in output: %v", identifiers["primary"])
	}
	// Number formatting should be preserved (no float drift, e.g. "5" not "5.0").
	if identifiers["other"] != float64(5) {
		t.Fatalf("expected number value preserved, got %#v", identifiers["other"])
	}
	if !strings.Contains(newBlob, `"other":5`) {
		t.Fatalf("expected exact literal \"other\":5 preserved in output, got %s", newBlob)
	}
	if parsed["top"] != "keep-me" {
		t.Fatalf("unrelated top-level field was lost")
	}
	unrelated := metadata["unrelated"].([]interface{})
	if len(unrelated) != 3 {
		t.Fatalf("unrelated array corrupted: %v", unrelated)
	}
}

func TestEvaluateItemNestedJSONNoMatch(t *testing.T) {
	blob := `{"metadata":{"identifiers":{"primary":"no-match-here"}}}`
	cfg := &Config{
		FieldName:     "payload",
		MatchPrefix:   "old-",
		NewValue:      "new-",
		IsJSON:        true,
		JSONFieldPath: "metadata.identifiers.primary",
	}
	cfg.jsonPathSegments = []string{"metadata", "identifiers", "primary"}

	item := map[string]types.AttributeValue{
		"payload": &types.AttributeValueMemberS{Value: blob},
	}

	matched, _, _, err := evaluateItem(cfg, item)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Fatalf("expected no match")
	}
	if item["payload"].(*types.AttributeValueMemberS).Value != blob {
		t.Fatalf("item should be unchanged on no-match")
	}
}

func TestEvaluateItemNestedNativeMap(t *testing.T) {
	// Mirrors how AppSync actually stores this AWSJSON field in DynamoDB for
	// the Archive table: a native nested Map attribute, not a JSON string.
	cfg := &Config{
		FieldName:     "archiveOptions",
		MatchPrefix:   "https://<old-distribution-id>.cloudfront.net",
		NewValue:      "https://<new-distribution-id>.cloudfront.net",
		IsJSON:        true,
		JSONFieldPath: "assets.env_config",
	}
	cfg.jsonPathSegments = []string{"assets", "env_config"}

	item := map[string]types.AttributeValue{
		"archiveOptions": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"assets": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"env_config":  &types.AttributeValueMemberS{Value: "https://<old-distribution-id>.cloudfront.net/federated/3d/gltf/studio.env"},
				"gltf_config": &types.AttributeValueMemberS{Value: "https://<new-distribution-id>.cloudfront.net/federated/vtec/VTEC000000374/3d/VTEC000000374.glb"},
				"media_type":  &types.AttributeValueMemberS{Value: "3d-model/gltf"},
			}},
			"config": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"_3d": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
					"scale_factor": &types.AttributeValueMemberS{Value: ""},
				}},
			}},
		}},
	}

	matched, oldVal, newVal, err := evaluateItem(cfg, item)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Fatalf("expected match")
	}
	if oldVal != "https://<old-distribution-id>.cloudfront.net/federated/3d/gltf/studio.env" {
		t.Fatalf("unexpected oldVal: %q", oldVal)
	}
	if newVal != "https://<new-distribution-id>.cloudfront.net/federated/3d/gltf/studio.env" {
		t.Fatalf("unexpected newVal: %q", newVal)
	}

	root := item["archiveOptions"].(*types.AttributeValueMemberM).Value
	assets := root["assets"].(*types.AttributeValueMemberM).Value
	if got := assets["env_config"].(*types.AttributeValueMemberS).Value; got != newVal {
		t.Fatalf("env_config not updated in place, got %q", got)
	}
	// Untouched sibling fields must survive unchanged.
	if got := assets["gltf_config"].(*types.AttributeValueMemberS).Value; got != "https://<new-distribution-id>.cloudfront.net/federated/vtec/VTEC000000374/3d/VTEC000000374.glb" {
		t.Fatalf("gltf_config was unexpectedly modified: %q", got)
	}
	config := root["config"].(*types.AttributeValueMemberM).Value
	if _, ok := config["_3d"]; !ok {
		t.Fatalf("unrelated nested config block was lost")
	}
}

func TestEvaluateItemNestedJSONMissingPath(t *testing.T) {
	blob := `{"metadata":{}}`
	cfg := &Config{
		FieldName:     "payload",
		MatchPrefix:   "old-",
		NewValue:      "new-",
		IsJSON:        true,
		JSONFieldPath: "metadata.identifiers.primary",
	}
	cfg.jsonPathSegments = []string{"metadata", "identifiers", "primary"}

	item := map[string]types.AttributeValue{
		"payload": &types.AttributeValueMemberS{Value: blob},
	}

	matched, _, _, err := evaluateItem(cfg, item)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Fatalf("expected no match when path is missing")
	}
}
