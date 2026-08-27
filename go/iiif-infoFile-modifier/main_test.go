package main

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestTransformInfoJSON_MatchesCorrectedFixture(t *testing.T) {
	in, err := os.ReadFile("incorrect_info.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	want, err := os.ReadFile("corrected_info.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}

	got, err := transformInfoJSON(in)
	if err != nil {
		t.Fatalf("transformInfoJSON: %v", err)
	}

	var gotVal, wantVal interface{}
	if err := json.Unmarshal(got, &gotVal); err != nil {
		t.Fatalf("unmarshaling transform output: %v", err)
	}
	if err := json.Unmarshal(want, &wantVal); err != nil {
		t.Fatalf("unmarshaling corrected fixture: %v", err)
	}

	if !reflect.DeepEqual(gotVal, wantVal) {
		t.Fatalf("transform output does not match corrected fixture\ngot:  %s\nwant: %s", got, want)
	}
}

func TestRollbackInfoJSON_ReversesTransform(t *testing.T) {
	original, err := os.ReadFile("incorrect_info.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}

	transformed, err := transformInfoJSON(original)
	if err != nil {
		t.Fatalf("transformInfoJSON: %v", err)
	}

	got, err := rollbackInfoJSON(transformed, original)
	if err != nil {
		t.Fatalf("rollbackInfoJSON: %v", err)
	}

	var gotVal, wantVal interface{}
	if err := json.Unmarshal(got, &gotVal); err != nil {
		t.Fatalf("unmarshaling rollback output: %v", err)
	}
	if err := json.Unmarshal(original, &wantVal); err != nil {
		t.Fatalf("unmarshaling original fixture: %v", err)
	}

	if !reflect.DeepEqual(gotVal, wantVal) {
		t.Fatalf("rollback output does not match original fixture\ngot:  %s\nwant: %s", got, original)
	}
}

func TestIsAlreadyTransformed(t *testing.T) {
	original, err := os.ReadFile("incorrect_info.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	corrected, err := os.ReadFile("corrected_info.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}

	if got, err := isAlreadyTransformed(original); err != nil {
		t.Fatalf("isAlreadyTransformed(original): %v", err)
	} else if got {
		t.Errorf("isAlreadyTransformed(original) = true, want false")
	}

	if got, err := isAlreadyTransformed(corrected); err != nil {
		t.Fatalf("isAlreadyTransformed(corrected): %v", err)
	} else if !got {
		t.Errorf("isAlreadyTransformed(corrected) = false, want true")
	}
}

func TestTransformInfoJSON_Idempotent(t *testing.T) {
	in, err := os.ReadFile("corrected_info.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}

	got, err := transformInfoJSON(in)
	if err != nil {
		t.Fatalf("transformInfoJSON: %v", err)
	}

	var gotVal, wantVal interface{}
	if err := json.Unmarshal(got, &gotVal); err != nil {
		t.Fatalf("unmarshaling transform output: %v", err)
	}
	if err := json.Unmarshal(in, &wantVal); err != nil {
		t.Fatalf("unmarshaling input fixture: %v", err)
	}

	if !reflect.DeepEqual(gotVal, wantVal) {
		t.Fatalf("transform is not idempotent on already-corrected input\ngot:  %s\nwant: %s", got, in)
	}
}
