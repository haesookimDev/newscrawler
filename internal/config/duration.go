package config

import (
	"encoding/json"
	"fmt"
	"time"
)

// Duration wraps time.Duration to support human-readable YAML/JSON values.
type Duration struct {
	time.Duration
}

// DurationFrom creates a Duration from a standard time.Duration.
func DurationFrom(d time.Duration) Duration {
	return Duration{Duration: d}
}

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}

func (d *Duration) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		d.Duration = 0
		return nil
	}

	parsed, err := time.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", string(text), err)
	}
	d.Duration = parsed
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	if len(b) == 0 {
		d.Duration = 0
		return nil
	}
	var raw string
	if err := json.Unmarshal(b, &raw); err != nil {
		return fmt.Errorf("duration should be a string: %w", err)
	}
	return d.UnmarshalText([]byte(raw))
}

// MarshalYAML allows emitting duration values as strings.
func (d Duration) MarshalYAML() (any, error) {
	return d.Duration.String(), nil
}

// UnmarshalYAML accepts either a string duration or numeric seconds.
func (d *Duration) UnmarshalYAML(value func(any) error) error {
	var raw any
	if err := value(&raw); err != nil {
		return err
	}
	switch v := raw.(type) {
	case string:
		return d.UnmarshalText([]byte(v))
	case int:
		d.Duration = time.Duration(v) * time.Second
		return nil
	case int64:
		d.Duration = time.Duration(v) * time.Second
		return nil
	case float64:
		d.Duration = time.Duration(v * float64(time.Second))
		return nil
	default:
		return fmt.Errorf("unsupported duration type %T", raw)
	}
}

// IsZero reports whether the duration is zero.
func (d Duration) IsZero() bool {
	return d.Duration == 0
}
