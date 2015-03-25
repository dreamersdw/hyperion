package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseRow(t *testing.T) {
	record, err := parseRow([]byte("\x00\x00\x01U\x11_\xd0\x00\x00\x01\x00\x00\x01\x00\x00\x02\x00\x00\x02"))

	assert.Equal(t, err, nil)
	assert.Equal(t, record.time, uint32(1427202000))
	assert.Equal(t, len(record.tags), 2)

	record, err = parseRow([]byte("\x00\x00\x01U\x11_\xd0\x00\x00\x01\x00\x00\x01\x00\x00\x02\x00\x00\x02\x01"))
	assert.NotEqual(t, err, nil)

	record, err = parseRow([]byte("\x00\x00\x01U\x11_\xd0\x00\x00\x01\x00\x00\x01\x00\x00\x02\x00\x00\x02\x01"))
	assert.NotEqual(t, err, nil)
}

func Benchmark(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseRow([]byte("\x00\x00\x01U\x11_\xd0\x00\x00\x01\x00\x00\x01\x00\x00\x02\x00\x00\x02"))
	}
}
