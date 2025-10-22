# ⚡ go-batcher
> High-performance batch processing for Go applications

<table>
  <thead>
    <tr>
      <th>CI&nbsp;/&nbsp;CD</th>
      <th>Quality&nbsp;&amp;&nbsp;Security</th>
      <th>Docs&nbsp;&amp;&nbsp;Meta</th>
      <th>Community</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td valign="top" align="left">
        <a href="https://github.com/bsv-blockchain/go-batcher/releases">
          <img src="https://img.shields.io/github/release-pre/bsv-blockchain/go-batcher?logo=github&style=flat" alt="Latest Release">
        </a><br/>
        <a href="https://github.com/bsv-blockchain/go-batcher/actions">
          <img src="https://img.shields.io/github/actions/workflow/status/bsv-blockchain/go-batcher/fortress.yml?branch=master&logo=github&style=flat" alt="Build Status">
        </a><br/>
		<a href="https://github.com/bsv-blockchain/go-batcher/actions">
          <img src="https://github.com/bsv-blockchain/go-batcher/actions/workflows/codeql-analysis.yml/badge.svg?style=flat" alt="CodeQL">
        </a><br/>
		<a href="https://sonarcloud.io/project/overview?id=bsv-blockchain_go-batcher">
          <img src="https://sonarcloud.io/api/project_badges/measure?project=bsv-blockchain_go-batcher&metric=alert_status&style-flat" alt="SonarCloud">
        </a>
      </td>
      <td valign="top" align="left">
        <a href="https://goreportcard.com/report/github.com/bsv-blockchain/go-batcher">
          <img src="https://goreportcard.com/badge/github.com/bsv-blockchain/go-batcher?style=flat" alt="Go Report Card">
        </a><br/>
		<a href="https://codecov.io/gh/bsv-blockchain/go-batcher/tree/master">
          <img src="https://codecov.io/gh/bsv-blockchain/go-batcher/branch/master/graph/badge.svg?style=flat" alt="Code Coverage">
        </a><br/>
		<a href="https://scorecard.dev/viewer/?uri=github.com/bsv-blockchain/go-batcher">
          <img src="https://api.scorecard.dev/projects/github.com/bsv-blockchain/go-batcher/badge?logo=springsecurity&logoColor=white" alt="OpenSSF Scorecard">
        </a><br/>
		<a href=".github/SECURITY.md">
          <img src="https://img.shields.io/badge/security-policy-blue?style=flat&logo=springsecurity&logoColor=white" alt="Security policy">
        </a>
      </td>
      <td valign="top" align="left">
        <a href="https://golang.org/">
          <img src="https://img.shields.io/github/go-mod/go-version/bsv-blockchain/go-batcher?style=flat" alt="Go version">
        </a><br/>
        <a href="https://pkg.go.dev/github.com/bsv-blockchain/go-batcher?tab=doc">
          <img src="https://pkg.go.dev/badge/github.com/bsv-blockchain/go-batcher.svg?style=flat" alt="Go docs">
        </a><br/>
        <a href=".github/AGENTS.md">
          <img src="https://img.shields.io/badge/AGENTS.md-found-40b814?style=flat&logo=openai" alt="AGENTS.md rules">
        </a><br/>
        <a href="https://magefile.org/">
          <img src="https://img.shields.io/badge/mage-powered-brightgreen?style=flat&logo=probot&logoColor=white" alt="Mage Powered">
        </a><br/>
		<a href=".github/dependabot.yml">
          <img src="https://img.shields.io/badge/dependencies-automatic-blue?logo=dependabot&style=flat" alt="Dependabot">
        </a>
      </td>
      <td valign="top" align="left">
        <a href="https://github.com/bsv-blockchain/go-batcher/graphs/contributors">
          <img src="https://img.shields.io/github/contributors/bsv-blockchain/go-batcher?style=flat&logo=contentful&logoColor=white" alt="Contributors">
        </a><br/>
		<a href="https://github.com/bsv-blockchain/go-batcher/commits/master">
		  <img src="https://img.shields.io/github/last-commit/bsv-blockchain/go-batcher?style=flat&logo=clockify&logoColor=white" alt="Last commit">
		</a><br/>
        <a href="https://github.com/sponsors/bsv-blockchain">
          <img src="https://img.shields.io/badge/sponsor-BSV-181717.svg?logo=github&style=flat" alt="Sponsor">
        </a><br/>
      </td>
    </tr>
  </tbody>
</table>

<br/>

## 🗂️ Table of Contents
* [What's Inside](#-whats-inside)
* [Installation](#-installation)
* [Documentation](#-documentation)
* [Examples & Tests](#-examples--tests)
* [Benchmarks](#-benchmarks)
* [Code Standards](#-code-standards)
* [AI Compliance](#-ai-compliance)
* [Maintainers](#-maintainers)
* [Contributing](#-contributing)
* [License](#-license)

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
<summary><strong><code>Repository Features</code></strong></summary>
<br/>

* **Continuous Integration on Autopilot** with [GitHub Actions](https://github.com/features/actions) – every push is built, tested, and reported in minutes.
* **Pull‑Request Flow That Merges Itself** thanks to [auto‑merge](.github/workflows/auto-merge-on-approval.yml) and hands‑free [Dependabot auto‑merge](.github/workflows/dependabot-auto-merge.yml).
* **One‑Command Builds** powered by battle‑tested [MAGE-X](https://github.com/mrz1836/mage-x) targets for linting, testing, releases, and more.
* **First‑Class Dependency Management** using native [Go Modules](https://github.com/golang/go/wiki/Modules).
* **Uniform Code Style** via [gofumpt](https://github.com/mvdan/gofumpt) plus zero‑noise linting with [golangci‑lint](https://github.com/golangci/golangci-lint).
* **Confidence‑Boosting Tests** with [testify](https://github.com/stretchr/testify), the Go [race detector](https://blog.golang.org/race-detector), crystal‑clear [HTML coverage](https://blog.golang.org/cover) snapshots, and automatic uploads to [Codecov](https://codecov.io/).
* **Hands‑Free Releases** delivered by [GoReleaser](https://github.com/goreleaser/goreleaser) whenever you create a [new Tag](https://git-scm.com/book/en/v2/Git-Basics-Tagging).
* **Relentless Dependency & Vulnerability Scans** via [Dependabot](https://dependabot.com), [Nancy](https://github.com/sonatype-nexus-community/nancy) and [govulncheck](https://pkg.go.dev/golang.org/x/vuln/cmd/govulncheck).
* **Security Posture by Default** with [CodeQL](https://docs.github.com/en/github/finding-security-vulnerabilities-and-errors-in-your-code/about-code-scanning), [OpenSSF Scorecard](https://openssf.org) and secret‑leak detection via [gitleaks](https://github.com/gitleaks/gitleaks).
* **Automatic Syndication** to [pkg.go.dev](https://pkg.go.dev/) on every release for instant godoc visibility.
* **Polished Community Experience** using rich templates for [Issues & PRs](https://docs.github.com/en/communities/using-templates-to-encourage-useful-issues-and-pull-requests/configuring-issue-templates-for-your-repository).
* **All the Right Meta Files** (`LICENSE`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SUPPORT.md`, `SECURITY.md`) pre‑filled and ready.
* **Code Ownership** clarified through a [CODEOWNERS](.github/CODEOWNERS) file, keeping reviews fast and focused.
* **Zero‑Noise Dev Environments** with tuned editor settings (`.editorconfig`) plus curated *ignore* files for [VS Code](.editorconfig), [Docker](.dockerignore), and [Git](.gitignore).
* **Label Sync Magic**: your repo labels stay in lock‑step with [.github/labels.yml](.github/labels.yml).
* **Friendly First PR Workflow** – newcomers get a warm welcome thanks to a dedicated [workflow](.github/workflows/pull-request-management.yml).
* **Standards‑Compliant Docs** adhering to the [standard‑readme](https://github.com/RichardLitt/standard-readme/blob/master/spec.md) spec.
* **Instant Cloud Workspaces** via [Gitpod](https://gitpod.io/) – spin up a fully configured dev environment with automatic linting and tests.
* **Out‑of‑the‑Box VS Code Happiness** with a preconfigured [Go](https://code.visualstudio.com/docs/languages/go) workspace and [`.vscode`](.vscode) folder with all the right settings.
* **Optional Release Broadcasts** to your community via [Slack](https://slack.com), [Discord](https://discord.com), or [Twitter](https://twitter.com) – plug in your webhook.
* **AI Compliance Playbook** – machine‑readable guidelines ([AGENTS.md](.github/AGENTS.md), [CLAUDE.md](.github/CLAUDE.md), [.cursorrules](.cursorrules), [sweep.yaml](.github/sweep.yaml)) keep ChatGPT, Claude, Cursor & Sweep aligned with your repo's rules.
* **Go-Pre-commit System** - [High-performance Go-native pre-commit hooks](https://github.com/mrz1836/go-pre-commit) with 17x faster execution—run the same formatting, linting, and tests before every commit, just like CI.
* **Zero Python Dependencies** - Pure Go implementation with environment-based configuration via [.env.base](.github/.env.base).
* **DevContainers for Instant Onboarding** – Launch a ready-to-code environment in seconds with [VS Code DevContainers](https://containers.dev/) and the included [.devcontainer.json](.devcontainer.json) config.

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

The system is configured via [.env.base](.github/.env.base) and can be customized using also using [.env.custom](.github/.env.custom) and provides 17x faster execution than traditional Python-based pre-commit hooks. See the [complete documentation](http://github.com/mrz1836/go-pre-commit) for details.

</details>

<details>
<summary><strong><code>GitHub Workflows</code></strong></summary>
<br/>

### 🎛️ The Workflow Control Center

All GitHub Actions workflows in this repository are powered by a single configuration files – your one-stop shop for tweaking CI/CD behavior without touching a single YAML file! 🎯

**Configuration Files:**
- **[.env.base](.github/.env.base)** – Default configuration that works for most Go projects
- **[.env.custom](.github/.env.custom)** – Optional project-specific overrides

This magical file controls everything from:
- **⚙️ Go version matrix** (test on multiple versions or just one)
- **🏃 Runner selection** (Ubuntu or macOS, your wallet decides)
- **🔬 Feature toggles** (coverage, fuzzing, linting, race detection, benchmarks)
- **🛡️ Security tool versions** (gitleaks, nancy, govulncheck)
- **🤖 Auto-merge behaviors** (how aggressive should the bots be?)
- **🏷️ PR management rules** (size labels, auto-assignment, welcome messages)

<br/>

| Workflow Name                                                                      | Description                                                                                                            |
|------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| [auto-merge-on-approval.yml](.github/workflows/auto-merge-on-approval.yml)         | Automatically merges PRs after approval and all required checks, following strict rules.                               |
| [codeql-analysis.yml](.github/workflows/codeql-analysis.yml)                       | Analyzes code for security vulnerabilities using [GitHub CodeQL](https://codeql.github.com/).                          |
| [dependabot-auto-merge.yml](.github/workflows/dependabot-auto-merge.yml)           | Automatically merges [Dependabot](https://github.com/dependabot) PRs that meet all requirements.                       |
| [fortress.yml](.github/workflows/fortress.yml)                                     | Runs the GoFortress security and testing workflow, including linting, testing, releasing, and vulnerability checks.    |
| [pull-request-management.yml](.github/workflows/pull-request-management.yml)       | Labels PRs by branch prefix, assigns a default user if none is assigned, and welcomes new contributors with a comment. |
| [scorecard.yml](.github/workflows/scorecard.yml)                                   | Runs [OpenSSF](https://openssf.org/) Scorecard to assess supply chain security.                                        |
| [stale.yml](.github/workflows/stale-check.yml)                                     | Warns about (and optionally closes) inactive issues and PRs on a schedule or manual trigger.                           |
| [sync-labels.yml](.github/workflows/sync-labels.yml)                               | Keeps GitHub labels in sync with the declarative manifest at [`.github/labels.yml`](./.github/labels.yml).             |

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

## 🤖 AI Compliance
This project documents expectations for AI assistants using a few dedicated files:

- [AGENTS.md](.github/AGENTS.md) — canonical rules for coding style, workflows, and pull requests used by [Codex](https://chatgpt.com/codex).
- [CLAUDE.md](.github/CLAUDE.md) — quick checklist for the [Claude](https://www.anthropic.com/product) agent.
- [.cursorrules](.cursorrules) — machine-readable subset of the policies for [Cursor](https://www.cursor.so/) and similar tools.
- [sweep.yaml](.github/sweep.yaml) — rules for [Sweep](https://github.com/sweepai/sweep), a tool for code review and pull request management.

Edit `AGENTS.md` first when adjusting these policies, and keep the other files in sync within the same pull request.

<br/>

## 👥 Maintainers
| [<img src="https://github.com/mrz1836.png" height="50" width="50" alt="MrZ" />](https://github.com/mrz1836) | [<img src="https://github.com/icellan.png" height="50" alt="Siggi" />](https://github.com/icellan) |
|:-----------------------------------------------------------------------------------------------------------:|:--------------------------------------------------------------------------------------------------:|
|                                      [MrZ](https://github.com/mrz1836)                                      |                                [Siggi](https://github.com/icellan)                                 |

<br/>

## 🤝 Contributing
View the [contributing guidelines](.github/CONTRIBUTING.md) and please follow the [code of conduct](.github/CODE_OF_CONDUCT.md).

### How can I help?
All kinds of contributions are welcome :raised_hands:!
The most basic way to show your support is to star :star2: the project, or to raise issues :speech_balloon:.

[![Stars](https://img.shields.io/github/stars/bsv-blockchain/go-batcher?label=Please%20like%20us&style=social&v=1)](https://github.com/bsv-blockchain/go-batcher/stargazers)

<br/>

## 📝 License

[![License](https://img.shields.io/badge/license-OpenBSV-blue?style=flat&logo=springsecurity&logoColor=white)](LICENSE)
