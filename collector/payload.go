package collector

import (
	"bytes"
	"encoding/json"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"

	"TemporalObserver/db"
)

// argKey names the pseudo-field an operator can allowlist to capture an entire positional
// input argument verbatim, for workflows whose input is a bare scalar rather than a JSON
// object with named keys (e.g. Walle's FileProcessingWorkflow takes a plain blob-path string -
// collectAllowlisted's key-matching has no key to match there at all). Still gated by the same
// per-(namespace, workflow_type) allowlist decision as every other field - nothing is captured
// unless an operator explicitly adds this exact "$argN" entry.
func argKey(i int) string {
	return fmt.Sprintf("$arg%d", i)
}

// extractAllowlistedFields decodes a workflow's start-event input payloads using Temporal's
// default JSON data converter encoding, then recursively walks the resulting JSON looking for
// keys that match the allowlist resolved for this (namespace, workflow type) pair (see
// db.GetAllowlistFields). Only the scalar (string/number/bool) value of a matching key is kept -
// never an object or array, even if an allowlisted name were to collide with one - and nothing
// else from the payload is ever retained or logged. Decoding uses UseNumber() so integer-valued
// fields keep their exact original digits instead of round-tripping through float64, which would
// silently lose precision on identifiers larger than 2^53.
func extractAllowlistedFields(input *commonpb.Payloads, allow db.AllowlistSet) map[string]string {
	out := map[string]string{}
	if input == nil {
		return out
	}
	for i, payload := range input.GetPayloads() {
		if payload == nil {
			continue
		}
		encoding := string(payload.GetMetadata()["encoding"])
		if encoding != "json/plain" {
			// Non-JSON payloads (binary/null, protobuf, etc.) are skipped entirely - we only
			// ever decode the default converter's plain JSON encoding.
			continue
		}
		var decoded interface{}
		dec := json.NewDecoder(bytes.NewReader(payload.GetData()))
		dec.UseNumber()
		if err := dec.Decode(&decoded); err != nil {
			continue
		}
		if canonical, ok := allow.Canonical(argKey(i)); ok {
			if scalar, ok := asScalarString(decoded); ok {
				out[canonical] = scalar
			}
		}
		collectAllowlisted(decoded, allow, out)
	}
	return out
}

func collectAllowlisted(node interface{}, allow db.AllowlistSet, out map[string]string) {
	switch v := node.(type) {
	case map[string]interface{}:
		for key, val := range v {
			if canonical, ok := allow.Canonical(key); ok {
				if scalar, ok := asScalarString(val); ok {
					out[canonical] = scalar
				}
				// An allowlisted key whose value is itself an object/array is intentionally
				// NOT stored and NOT recursed into under that key - no partial credit.
				continue
			}
			collectAllowlisted(val, allow, out)
		}
	case []interface{}:
		for _, item := range v {
			collectAllowlisted(item, allow, out)
		}
	}
}

func asScalarString(v interface{}) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case json.Number:
		return t.String(), true
	case bool:
		if t {
			return "true", true
		}
		return "false", true
	default:
		return "", false
	}
}
