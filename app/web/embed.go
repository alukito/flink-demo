package web

import "embed"

// DistFS holds the embedded React build output.
// The dist directory is populated by `npm run build` (Vite output).
//go:embed dist
var DistFS embed.FS
