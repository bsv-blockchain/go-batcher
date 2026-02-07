<div align="center">

# ⚡&nbsp;&nbsp;go-batcher

**High-performance batch processing for Go applications**

<br/>

<a href="https://github.com/bsv-blockchain/go-batcher/releases"><img src="https://img.shields.io/github/release-pre/bsv-blockchain/go-batcher?include_prereleases&style=flat-square&logo=github&color=black" alt="Release"></a>
<a href="https://golang.org/"><img src="https://img.shields.io/github/go-mod/go-version/bsv-blockchain/go-batcher?style=flat-square&logo=go&color=00ADD8" alt="Go Version"></a>
<a href="https://github.com/bsv-blockchain/go-batcher/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-OpenBSV-blue?style=flat-square" alt="License"></a>

<br/>

<table align="center" border="0">
  <tr>
    <td align="right">
       <code>CI / CD</code> &nbsp;&nbsp;
    </td>
    <td align="left">
       <a href="https://github.com/bsv-blockchain/go-batcher/actions"><img src="https://img.shields.io/github/actions/workflow/status/bsv-blockchain/go-batcher/fortress.yml?branch=master&label=build&logo=github&style=flat-square" alt="Build"></a>
       <a href="https://github.com/bsv-blockchain/go-batcher/commits/master"><img src="https://img.shields.io/github/last-commit/bsv-blockchain/go-batcher?style=flat-square&logo=git&logoColor=white&label=last%20update" alt="Last Commit"></a>
    </td>
    <td align="right">
       &nbsp;&nbsp;&nbsp;&nbsp; <code>Quality</code> &nbsp;&nbsp;
    </td>
    <td align="left">
       <a href="https://goreportcard.com/report/github.com/bsv-blockchain/go-batcher"><img src="https://goreportcard.com/badge/github.com/bsv-blockchain/go-batcher?style=flat-square" alt="Go Report"></a>
       <a href="https://codecov.io/gh/bsv-blockchain/go-batcher"><img src="https://codecov.io/gh/bsv-blockchain/go-batcher/branch/master/graph/badge.svg?style=flat-square" alt="Coverage"></a>
    </td>
  </tr>

  <tr>
    <td align="right">
       <code>Security</code> &nbsp;&nbsp;
    </td>
    <td align="left">
       <a href="https://scorecard.dev/viewer/?uri=github.com/bsv-blockchain/go-batcher"><img src="https://api.scorecard.dev/projects/github.com/bsv-blockchain/go-batcher/badge?style=flat-square" alt="Scorecard"></a>
       <a href=".github/SECURITY.md"><img src="https://img.shields.io/badge/policy-active-success?style=flat-square&logo=security&logoColor=white" alt="Security"></a>
    </td>
    <td align="right">
       &nbsp;&nbsp;&nbsp;&nbsp; <code>Community</code> &nbsp;&nbsp;
    </td>
    <td align="left">
       <a href="https://github.com/bsv-blockchain/go-batcher/graphs/contributors"><img src="https://img.shields.io/github/contributors/bsv-blockchain/go-batcher?style=flat-square&color=orange" alt="Contributors"></a>
       <a href="https://github.com/sponsors/bsv-blockchain"><img src="https://img.shields.io/badge/sponsor-BSV-181717?style=flat-square&logo=github" alt="Sponsor"></a>
    </td>
  </tr>
</table>

</div>

<br/>
<br/>

<div align="center">

### <code>Project Navigation</code>

</div>

<table align="center">
  <tr>
    <td align="center" width="33%">
       📦&nbsp;<a href="#-installation"><code>Installation</code></a>
    </td>
    <td align="center" width="33%">
       🎯&nbsp;<a href="#-whats-inside"><code>What's&nbsp;Inside</code></a>
    </td>
    <td align="center" width="33%">
       📚&nbsp;<a href="#-documentation"><code>Documentation</code></a>
    </td>
  </tr>
  <tr>
    <td align="center">
       🧪&nbsp;<a href="#-examples--tests"><code>Examples&nbsp;&&nbsp;Tests</code></a>
    </td>
    <td align="center">
       ⚡&nbsp;<a href="#-benchmarks"><code>Benchmarks</code></a>
    </td>
    <td align="center">
       🛠️&nbsp;<a href="#-code-standards"><code>Code&nbsp;Standards</code></a>
    </td>
  </tr>
  <tr>
    <td align="center">
       🤖&nbsp;<a href="#-ai-usage--assistant-guidelines"><code>AI&nbsp;Usage</code></a>
    </td>
    <td align="center">
       🤝&nbsp;<a href="#-contributing"><code>Contributing</code></a>
    </td>
    <td align="center">
       👥&nbsp;<a href="#-maintainers"><code>Maintainers</code></a>
    </td>
  </tr>
  <tr>
    <td align="center" colspan="3">
       📝&nbsp;<a href="#-license"><code>License</code></a>
    </td>
  </tr>
</table>
<br/>

## 🎯 What's Inside

### Lightning-Fast Batch Processing in Action

```go
package main

import (
    "fmt"
    "time"
    "github.com/bsv-blockchain/go-batcher"
)

func main() {
    // Create a batcher that processes items every 100ms or when batch size hits 1000
    b := batcher.New[string](
        1000,                                        // batch size
        100*time.Millisecond,                        // timeout interval
        func(batch []*string) {                      // processor function
            fmt.Printf("⚡ Processing %d items in one go!\n", len(batch))
            // Your batch processing logic here
            for _, item := range batch {
                fmt.Printf("Processing: %s\n", *item)
            }
        },
        true, // background processing
    )

    // Feed items - they'll be intelligently batched
    for i := 0; i < 5000; i++ {
        item := fmt.Sprintf("item-%d", i)
        b.Put(&item)
    }

    // Process any remaining items before shutdown
    b.Trigger()
    // Note: The batcher worker runs indefinitely - use context cancellation for cleanup
}
```

<br/>

### Constructor Variants

The `go-batcher` library provides several constructor options to fit different use cases:

```go
// Basic batcher - simple batching with size and timeout triggers
b := batcher.New[string](100, time.Second, processFn, true)

// With slice pooling - reduces memory allocations for high-throughput scenarios
b := batcher.NewWithPool[string](100, time.Second, processFn, true)

// With automatic deduplication - filters duplicate items within a 1-minute window
b := batcher.NewWithDeduplication[string](100, time.Second, processFn, true)

// Combined pooling and deduplication - maximum performance with duplicate filtering
b := batcher.NewWithDeduplicationAndPool[string](100, time.Second, processFn, true)
```

<br/>

### Why You'll Love This Batcher

* **Blazing Performance** – Process millions of items with minimal overhead ([benchmarks](#benchmark-results): 135 ns/op)
* **Smart Batching** – Auto-groups by size or time interval, whichever comes first
* **Optional Deduplication** – Built-in dedup variant ensures each item is processed only once within a time window
* **Memory Pool Optimization** – Optional slice pooling reduces GC pressure in high-throughput scenarios
* **Thread-Safe by Design** – Concurrent Put() from multiple goroutines without worry
* **Time-Partitioned Storage** – Efficient memory usage with automatic cleanup (dedup variant)
* **Minimal Dependencies** – Pure Go with only essential external dependencies
* **Flexible Configuration** – Multiple constructor variants for different use cases
* **Production-Ready** – Battle-tested with full test coverage and benchmarks

Perfect for high-throughput scenarios like log aggregation, metrics collection, event processing, or any situation where you need to efficiently batch operations for downstream systems.

<br/>

## 📦 Installation

**go-batcher** requires a [supported release of Go](https://golang.org/doc/devel/release.html#policy).
```shell script
go get -u github.com/bsv-blockchain/go-batcher
```

<br/>

## 📚 Documentation

- **API Reference** – Dive into the godocs at [pkg.go.dev/github.com/bsv-blockchain/go-batcher](https://pkg.go.dev/github.com/bsv-blockchain/go-batcher)
- **Usage Examples** – Browse practical patterns either the [examples directory](examples) or view the [example functions](batcher_example_test.go)
- **Benchmarks** – Check the latest numbers in the [benchmark results](#benchmark-results)
- **Test Suite** – Review both the [unit tests](batcher_test.go) and [fuzz tests](batcher_fuzz_test.go) (powered by [`testify`](https://github.com/stretchr/testify))

<br/>

<details>
<summary><strong><code>Development Build Commands</code></strong></summary>
<br/>

Get the [MAGE-X](https://github.com/mrz1836/mage-x) build tool for development:
```shell script
go install github.com/mrz1836/mage-x/cmd/magex@latest
```

View all build commands

```bash script
magex help
```

</details>

<details>
<summary><strong>Repository Features</strong></summary>
<br/>

This repository includes 25+ built-in features covering CI/CD, security, code quality, developer experience, and community tooling.

**[View the full Repository Features list →](.github/docs/repository-features.md)**

</details>

<details>
<summary><strong><code>Library Deployment</code></strong></summary>
<br/>

This project uses [goreleaser](https://github.com/goreleaser/goreleaser) for streamlined binary and library deployment to GitHub. To get started, install it via:

```bash
brew install goreleaser
```

The release process is defined in the [.goreleaser.yml](.goreleaser.yml) configuration file.


Then create and push a new Git tag using:

```bash
magex version:bump push=true bump=patch branch=master
```

This process ensures consistent, repeatable releases with properly versioned artifacts and citation metadata.

</details>

<details>
<summary><strong><code>Pre-commit Hooks</code></strong></summary>
<br/>

Set up the Go-Pre-commit System to run the same formatting, linting, and tests defined in [AGENTS.md](.github/AGENTS.md) before every commit:

```bash
go install github.com/mrz1836/go-pre-commit/cmd/go-pre-commit@latest
go-pre-commit install
```

The system is configured via [modular env files](.github/env/README.md) and provides 17x faster execution than traditional Python-based pre-commit hooks. See the [complete documentation](http://github.com/mrz1836/go-pre-commit) for details.

</details>

<details>
<summary><strong>GitHub Workflows</strong></summary>
<br/>

All workflows are driven by modular configuration in [`.github/env/`](.github/env/README.md) — no YAML editing required.

**[View all workflows and the control center →](.github/docs/workflows.md)**

</details>

<details>
<summary><strong><code>Updating Dependencies</code></strong></summary>
<br/>

To update all dependencies (Go modules, linters, and related tools), run:

```bash
magex deps:update
```

This command ensures all dependencies are brought up to date in a single step, including Go modules and any tools managed by [MAGE-X](https://github.com/mrz1836/mage-x). It is the recommended way to keep your development environment and CI in sync with the latest versions.

</details>

<br/>

## 🧪 Examples & Tests

All unit tests and [examples](examples) run via [GitHub Actions](https://github.com/bsv-blockchain/go-batcher/actions) and use [Go version 1.24.x](https://go.dev/doc/go1.24). View the [configuration file](.github/workflows/fortress.yml).

Run all tests (fast):

```bash script
magex test
```

Run all tests with race detector (slower):
```bash script
magex test:race
```

<br/>

## ⚡ Benchmarks

Run the Go [benchmarks](batcher_benchmark_test.go):

```bash script
magex bench
```

<br/>

### Benchmark Results

| Benchmark                                                                            | Description                      |   ns/op |  B/op | allocs/op |
|--------------------------------------------------------------------------------------|----------------------------------|--------:|------:|----------:|
| [BenchmarkBatcherPut](batcher_comprehensive_benchmark_test.go)                       | Basic Put operation              |   135.1 |     8 |         0 |
| [BenchmarkBatcherPutParallel](batcher_comprehensive_benchmark_test.go)               | Concurrent Put operations        |   310.0 |     9 |         0 |
| [BenchmarkPutComparison/Put](benchmark_comparison_test.go)                           | Put operation (non-blocking)     |   300.7 |     9 |         0 |
| [BenchmarkPutComparison/PutWithPool](benchmark_comparison_test.go)                   | Put with slice pooling           |   309.9 |     1 |         0 |
| [BenchmarkWithPoolComparison/Batcher](benchmark_comparison_test.go)                  | Standard batcher                 |   171.2 |    18 |         1 |
| [BenchmarkWithPoolComparison/WithPool](benchmark_comparison_test.go)                 | Pooled batcher                   |   184.0 |     9 |         1 |
| [BenchmarkTimePartitionedMapSet](batcher_comprehensive_benchmark_test.go)            | Map Set operation (bloom filter) |   366.7 |   147 |         6 |
| [BenchmarkTimePartitionedMapGet](batcher_comprehensive_benchmark_test.go)            | Map Get operation (bloom filter) |    80.5 |    39 |         2 |
| [BenchmarkBatcherWithDedupPut](batcher_comprehensive_benchmark_test.go)              | Put with deduplication           |   740.1 |   166 |         7 |
| [BenchmarkBatcher](batcher_benchmark_test.go)                                        | Full batch processing (1M items) | 1,081ms | 710MB |      1.9M |
| [BenchmarkBatcherWithDeduplication](batcher_benchmark_test.go)                       | Deduplication processing         |    90.7 |    13 |         0 |

> Performance benchmarks for the core functions in this library, executed on an Apple M1 Max (ARM64).
> The benchmarks demonstrate excellent performance with minimal allocations for basic operations.

<br/>

## 🛠️ Code Standards
Read more about this Go project's [code standards](.github/CODE_STANDARDS.md).

<br/>

## 🤖 AI Usage & Assistant Guidelines
Read the [AI Usage & Assistant Guidelines](.github/tech-conventions/ai-compliance.md) for details on how AI is used in this project and how to interact with AI assistants.

<br/>

## 👥 Maintainers
| [<img src="https://github.com/icellan.png" height="50" alt="Siggi" />](https://github.com/icellan) | [<img src="https://github.com/galt-tr.png" height="50" alt="Galt" />](https://github.com/galt-tr) | [<img src="https://github.com/mrz1836.png" height="50" alt="MrZ" />](https://github.com/mrz1836) |
|:--------------------------------------------------------------------------------------------------:|:-------------------------------------------------------------------------------------------------:|:------------------------------------------------------------------------------------------------:|
|                                [Siggi](https://github.com/icellan)                                 |                                [Dylan](https://github.com/galt-tr)                                 |                                [MrZ](https://github.com/mrz1836)                                 |

<br/>

## 🤝 Contributing
View the [contributing guidelines](.github/CONTRIBUTING.md) and please follow the [code of conduct](.github/CODE_OF_CONDUCT.md).

### How can I help?
All kinds of contributions are welcome :raised_hands:!
The most basic way to show your support is to star :star2: the project, or to raise issues :speech_balloon:.

[![Stars](https://img.shields.io/github/stars/bsv-blockchain/go-batcher?label=Please%20like%20us&style=social&v=1)](https://github.com/bsv-blockchain/go-batcher/stargazers)

<br/>

## 📝 License

[![License](https://img.shields.io/badge/license-OpenBSV-blue?style=flat-square)](LICENSE)
