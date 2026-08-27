package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestEvaluateItemPlainString(t *testing.T) {
	cfg := &Config{
		FieldName: "identifier",
		Matches:   Matches{Value: "old-"},
		NewValue:  "new-",
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

func TestEvaluateItemPlainStringFullMatch(t *testing.T) {
	cfg := &Config{
		FieldName: "identifier",
		Matches:   Matches{Value: "old-1234", Type: matchFull},
		NewValue:  "brand-new-value",
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
	if oldVal != "old-1234" || newVal != "brand-new-value" {
		t.Fatalf("got old=%q new=%q", oldVal, newVal)
	}
	got := item["identifier"].(*types.AttributeValueMemberS).Value
	if got != "brand-new-value" {
		t.Fatalf("item not updated, got %q", got)
	}
}

func TestEvaluateItemPlainStringFullMatchPartialNoMatch(t *testing.T) {
	// A "full" match must equal the whole value; a mere prefix match should
	// not trigger a replacement.
	cfg := &Config{
		FieldName: "identifier",
		Matches:   Matches{Value: "old-", Type: matchFull},
		NewValue:  "new-",
	}
	item := map[string]types.AttributeValue{
		"identifier": &types.AttributeValueMemberS{Value: "old-1234"},
	}

	matched, _, _, err := evaluateItem(cfg, item)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Fatalf("expected no match when value is only a prefix, not the full value")
	}
}

func TestEvaluateItemNestedJSON(t *testing.T) {
	blob := `{"metadata":{"identifiers":{"primary":"old-abc","other":5},"unrelated":[1,2,3]},"top":"keep-me"}`
	cfg := &Config{
		FieldName:     "payload",
		Matches:       Matches{Value: "old-"},
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
		Matches:       Matches{Value: "old-"},
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
		Matches:       Matches{Value: "https://<old-distribution-id>.cloudfront.net"},
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

func TestItemMatchesConditionsNoConditions(t *testing.T) {
	cfg := &Config{}
	item := map[string]types.AttributeValue{
		"status": &types.AttributeValueMemberS{Value: "active"},
	}
	if !itemMatchesConditions(cfg, item) {
		t.Fatalf("expected match when no conditions are configured")
	}
}

func TestItemMatchesConditionsEqualsAllMatch(t *testing.T) {
	cfg := &Config{Conditions: []Condition{
		{Field: "status", Operator: condEquals, Value: "active"},
		{Field: "item_count", Value: "5"}, // default operator (equals)
	}}
	item := map[string]types.AttributeValue{
		"status":     &types.AttributeValueMemberS{Value: "active"},
		"item_count": &types.AttributeValueMemberN{Value: "5"},
		"other":      &types.AttributeValueMemberS{Value: "ignored"},
	}
	if !itemMatchesConditions(cfg, item) {
		t.Fatalf("expected match when all conditions are satisfied")
	}
}

func TestItemMatchesConditionsEqualsMismatch(t *testing.T) {
	cfg := &Config{Conditions: []Condition{{Field: "status", Value: "active"}}}
	item := map[string]types.AttributeValue{
		"status": &types.AttributeValueMemberS{Value: "inactive"},
	}
	if itemMatchesConditions(cfg, item) {
		t.Fatalf("expected no match when a condition value differs")
	}
}

func TestItemMatchesConditionsEqualsMissingField(t *testing.T) {
	cfg := &Config{Conditions: []Condition{{Field: "status", Value: "active"}}}
	item := map[string]types.AttributeValue{
		"other": &types.AttributeValueMemberS{Value: "value"},
	}
	if itemMatchesConditions(cfg, item) {
		t.Fatalf("expected no match when the condition field is absent from the item")
	}
}

func TestItemMatchesConditionsContainsStringSet(t *testing.T) {
	cfg := &Config{Conditions: []Condition{{Field: "tags", Operator: condContains, Value: "featured"}}}
	item := map[string]types.AttributeValue{
		"tags": &types.AttributeValueMemberSS{Value: []string{"new", "featured", "sale"}},
	}
	if !itemMatchesConditions(cfg, item) {
		t.Fatalf("expected match when value is a member of the string set")
	}

	item["tags"] = &types.AttributeValueMemberSS{Value: []string{"new", "sale"}}
	if itemMatchesConditions(cfg, item) {
		t.Fatalf("expected no match when value is not a member of the string set")
	}
}

func TestItemMatchesConditionsContainsList(t *testing.T) {
	cfg := &Config{Conditions: []Condition{{Field: "tags", Operator: condContains, Value: "featured"}}}
	item := map[string]types.AttributeValue{
		"tags": &types.AttributeValueMemberL{Value: []types.AttributeValue{
			&types.AttributeValueMemberS{Value: "new"},
			&types.AttributeValueMemberS{Value: "featured"},
		}},
	}
	if !itemMatchesConditions(cfg, item) {
		t.Fatalf("expected match when value is an element of the list")
	}
}

func TestItemMatchesConditionsContainsSubstring(t *testing.T) {
	cfg := &Config{Conditions: []Condition{{Field: "description", Operator: condContains, Value: "gltf"}}}
	item := map[string]types.AttributeValue{
		"description": &types.AttributeValueMemberS{Value: "a 3d-model/gltf asset"},
	}
	if !itemMatchesConditions(cfg, item) {
		t.Fatalf("expected match when value is a substring of the string attribute")
	}
}

func TestItemMatchesConditionsExists(t *testing.T) {
	cfg := &Config{Conditions: []Condition{{Field: "legacy_id", Operator: condExists}}}

	present := map[string]types.AttributeValue{"legacy_id": &types.AttributeValueMemberS{Value: "x"}}
	if !itemMatchesConditions(cfg, present) {
		t.Fatalf("expected match when field is present")
	}

	absent := map[string]types.AttributeValue{"other": &types.AttributeValueMemberS{Value: "x"}}
	if itemMatchesConditions(cfg, absent) {
		t.Fatalf("expected no match when field is absent")
	}
}

func TestItemMatchesConditionsNotExists(t *testing.T) {
	cfg := &Config{Conditions: []Condition{{Field: "migrated", Operator: condNotExists}}}

	absent := map[string]types.AttributeValue{"other": &types.AttributeValueMemberS{Value: "x"}}
	if !itemMatchesConditions(cfg, absent) {
		t.Fatalf("expected match when field is absent")
	}

	present := map[string]types.AttributeValue{"migrated": &types.AttributeValueMemberS{Value: "x"}}
	if itemMatchesConditions(cfg, present) {
		t.Fatalf("expected no match when field is present")
	}
}

func TestLoadConfigConditionValidation(t *testing.T) {
	base := `
region: us-east-1
table_name: t
field_name: f
matches:
  value: p
`
	tmpDir := t.TempDir()

	write := func(name, extra string) string {
		p := tmpDir + "/" + name
		if err := os.WriteFile(p, []byte(base+extra), 0o600); err != nil {
			t.Fatalf("writing test config: %v", err)
		}
		return p
	}

	t.Run("missing value for equals", func(t *testing.T) {
		p := write("missing_value.yaml", "conditions:\n  - field: status\n")
		if _, err := loadConfig(p); err == nil {
			t.Fatalf("expected error for equals condition missing a value")
		}
	})

	t.Run("missing field", func(t *testing.T) {
		p := write("missing_field.yaml", "conditions:\n  - operator: exists\n")
		if _, err := loadConfig(p); err == nil {
			t.Fatalf("expected error for condition missing a field")
		}
	})

	t.Run("unknown operator", func(t *testing.T) {
		p := write("bad_operator.yaml", "conditions:\n  - field: status\n    operator: bogus\n")
		if _, err := loadConfig(p); err == nil {
			t.Fatalf("expected error for unknown operator")
		}
	})

	t.Run("exists needs no value", func(t *testing.T) {
		p := write("exists_ok.yaml", "conditions:\n  - field: status\n    operator: exists\n")
		if _, err := loadConfig(p); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestLoadConfigMatchesValidation(t *testing.T) {
	baseNoMatches := `
region: us-east-1
table_name: t
field_name: f
`
	tmpDir := t.TempDir()

	write := func(name, content string) string {
		p := tmpDir + "/" + name
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatalf("writing test config: %v", err)
		}
		return p
	}

	t.Run("missing matches.value", func(t *testing.T) {
		p := write("missing_matches.yaml", baseNoMatches)
		if _, err := loadConfig(p); err == nil {
			t.Fatalf("expected error when matches.value is not set")
		}
	})

	t.Run("defaults to prefix", func(t *testing.T) {
		p := write("default_type.yaml", baseNoMatches+"matches:\n  value: p\n")
		cfg, err := loadConfig(p)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Matches.Type != matchPrefix {
			t.Fatalf("expected default matches.type %q, got %q", matchPrefix, cfg.Matches.Type)
		}
	})

	t.Run("full type accepted", func(t *testing.T) {
		p := write("full_type.yaml", baseNoMatches+"matches:\n  value: p\n  type: full\n")
		cfg, err := loadConfig(p)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Matches.Type != matchFull {
			t.Fatalf("expected matches.type %q, got %q", matchFull, cfg.Matches.Type)
		}
	})

	t.Run("unknown type rejected", func(t *testing.T) {
		p := write("bad_type.yaml", baseNoMatches+"matches:\n  value: p\n  type: bogus\n")
		if _, err := loadConfig(p); err == nil {
			t.Fatalf("expected error for unknown matches.type")
		}
	})
}

func TestEvaluateItemNestedJSONMissingPath(t *testing.T) {
	blob := `{"metadata":{}}`
	cfg := &Config{
		FieldName:     "payload",
		Matches:       Matches{Value: "old-"},
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
