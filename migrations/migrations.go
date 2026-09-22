// Package migrations embeds the versioned database changes in every binary.
package migrations

import "embed"

//go:embed *.sql
var Files embed.FS
