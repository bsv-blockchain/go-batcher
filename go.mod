module github.com/bsv-blockchain/go-batcher

go 1.24.3

toolchain go1.24.5

require (
	github.com/bsv-blockchain/go-tx-map v1.2.1
	github.com/stretchr/testify v1.11.1
	golang.org/x/sync v0.19.0
)

require (
	github.com/bsv-blockchain/go-bt/v2 v2.5.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dolthub/maphash v0.1.0 // indirect
	github.com/dolthub/swiss v0.2.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Security: Force golang.org/x/crypto to v0.45.0 to fix CVE-2025-47914 and CVE-2025-58181
replace golang.org/x/crypto => golang.org/x/crypto v0.45.0
