// Package pgvector provides the Vector type for PostgreSQL pgvector integration.
package pgvector

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
)

// Vector represents a pgvector float32 embedding vector.
type Vector struct {
	vec []float32
}

// NewVector creates a Vector from a float32 slice.
func NewVector(vec []float32) Vector {
	if vec == nil {
		return Vector{}
	}
	cp := make([]float32, len(vec))
	copy(cp, vec)
	return Vector{vec: cp}
}

// Slice returns the underlying float32 slice.
func (v Vector) Slice() []float32 {
	if v.vec == nil {
		return nil
	}
	cp := make([]float32, len(v.vec))
	copy(cp, v.vec)
	return cp
}

// String formats the vector in pgvector text representation "[f1,f2,...]".
func (v Vector) String() string {
	if len(v.vec) == 0 {
		return "[]"
	}
	var b strings.Builder
	b.WriteByte('[')
	for i, val := range v.vec {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(val), 'f', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}

// Parse parses a pgvector text representation "[f1,f2,...]" into v.
func (v *Vector) Parse(s string) error {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "[") || !strings.HasSuffix(s, "]") {
		return fmt.Errorf("pgvector: invalid vector format %q", s)
	}
	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		v.vec = []float32{}
		return nil
	}
	parts := strings.Split(inner, ",")
	res := make([]float32, len(parts))
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
		if err != nil {
			return fmt.Errorf("pgvector: invalid element %q: %w", p, err)
		}
		res[i] = float32(f)
	}
	v.vec = res
	return nil
}

// Scan implements sql.Scanner.
func (v *Vector) Scan(src any) error {
	switch val := src.(type) {
	case nil:
		v.vec = nil
		return nil
	case string:
		return v.Parse(val)
	case []byte:
		return v.Parse(string(val))
	case Vector:
		v.vec = val.Slice()
		return nil
	default:
		return fmt.Errorf("pgvector: unsupported scan type %T", src)
	}
}

// Value implements driver.Valuer.
func (v Vector) Value() (driver.Value, error) {
	if len(v.vec) == 0 {
		return nil, nil
	}
	return v.String(), nil
}
