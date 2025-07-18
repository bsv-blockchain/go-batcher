# 🚀 go-batcher
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
        <a href="Makefile">
          <img src="https://img.shields.io/badge/Makefile-supported-brightgreen?style=flat&logo=probot&logoColor=white" alt="Makefile Supported">
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
        batcher.WithBatchSize[string](1000),
        batcher.WithInterval[string](100 * time.Millisecond),
        batcher.WithProcessor[string](func(batch []string) error {
            fmt.Printf("⚡ Processing %d items in one go!\n", len(batch))
            // Your batch processing logic here
            return nil
        }),
    )
    
    // Feed items - they'll be intelligently batched
    for i := 0; i < 5000; i++ {
        b.Put(fmt.Sprintf("item-%d", i))
    }
    
    // Gracefully shutdown when done
    b.Stop()
}
```

<br/>

### Why You'll Love This Batcher

* **⚡ Blazing Performance** – Process millions of items with minimal overhead ([benchmarks](#benchmark-results): 145 ns/op)
* **🧠 Smart Batching** – Auto-groups by size or time interval, whichever comes first
* **🔁 Built-in Deduplication** – Optional dedup ensures each item is processed only once
* **🛡️ Thread-Safe by Design** – Concurrent Put() from multiple goroutines without worry
* **⏱️ Time-Partitioned Storage** – Efficient memory usage with automatic cleanup
* **🎯 Zero Dependencies** – Pure Go with no external runtime dependencies
* **🔧 Flexible Configuration** – Customize batch sizes, intervals, and processors
* **📊 Production-Ready** – Battle-tested with full test coverage and benchmarks

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
<summary><strong><code>Repository Features</code></strong></summary>
<br/>

* **Continuous Integration on Autopilot** with [GitHub Actions](https://github.com/features/actions) – every push is built, tested, and reported in minutes.
* **Pull‑Request Flow That Merges Itself** thanks to [auto‑merge](.github/workflows/auto-merge-on-approval.yml) and hands‑free [Dependabot auto‑merge](.github/workflows/dependabot-auto-merge.yml).
* **One‑Command Builds** powered by battle‑tested [Make](https://www.gnu.org/software/make) targets for linting, testing, releases, and more.
* **First‑Class Dependency Management** using native [Go Modules](https://github.com/golang/go/wiki/Modules).
* **Uniform Code Style** via [gofumpt](https://github.com/mvdan/gofumpt) plus zero‑noise linting with [golangci‑lint](https://github.com/golangci/golangci-lint).
* **Confidence‑Boosting Tests** with [testify](https://github.com/stretchr/testify), the Go [race detector](https://blog.golang.org/race-detector), crystal‑clear [HTML coverage](https://blog.golang.org/cover) snapshots, and automatic uploads to [Codecov](https://codecov.io/).
* **Hands‑Free Releases** delivered by [GoReleaser](https://github.com/goreleaser/goreleaser) whenever you create a [new Tag](https://git-scm.com/book/en/v2/Git-Basics-Tagging).
* **Relentless Dependency & Vulnerability Scans** via [Dependabot](https://dependabot.com), [Nancy](https://github.com/sonatype-nexus-community/nancy), and [govulncheck](https://pkg.go.dev/golang.org/x/vuln/cmd/govulncheck).
* **Security Posture by Default** with [CodeQL](https://docs.github.com/en/github/finding-security-vulnerabilities-and-errors-in-your-code/about-code-scanning), [OpenSSF Scorecard](https://openssf.org), and secret‑leak detection via [gitleaks](https://github.com/gitleaks/gitleaks).
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
* **AI Compliance Playbook** – machine‑readable guidelines ([AGENTS.md](.github/AGENTS.md), [CLAUDE.md](.github/CLAUDE.md), [.cursorrules](.cursorrules), [sweep.yaml](.github/sweep.yaml)) keep ChatGPT, Claude, Cursor & Sweep aligned with your repo’s rules.
* **Pre-commit Hooks for Consistency** powered by [pre-commit](https://pre-commit.com) and the [.pre-commit-config.yaml](.pre-commit-config.yaml) file—run the same formatting, linting, and tests before every commit, just like CI.
* **Automated Hook Updates** keep the [.pre-commit-config.yaml](.pre-commit-config.yaml) current via a weekly [workflow](.github/workflows/update-pre-commit-hooks.yml).
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

To generate a snapshot (non-versioned) release for testing purposes, run:

```bash
make release-snap
```

Then create and push a new Git tag using:

```bash
make tag version=x.y.z
```

This process ensures consistent, repeatable releases with properly versioned artifacts and citation metadata.

</details>

<details>
<summary><strong><code>Makefile Commands</code></strong></summary>
<br/>

View all `makefile` commands

```bash script
make help
```

List of all current commands:

<!-- make-help-start -->
```text
bench                 ## Run all benchmarks in the Go application
build-go              ## Build the Go application (locally)
citation              ## Update version in CITATION.cff (use version=X.Y.Z)
clean-mods            ## Remove all the Go mod cache
coverage              ## Show test coverage
diff                  ## Show git diff and fail if uncommitted changes exist
fumpt                 ## Run fumpt to format Go code
generate              ## Run go generate in the base of the repo
godocs                ## Trigger GoDocs tag sync
govulncheck-install   ## Install govulncheck (pass VERSION= to override)
govulncheck           ## Scan for vulnerabilities
help                  ## Display this help message
install-go            ## Install using go install with specific version
install-releaser      ## Install GoReleaser
install-stdlib        ## Install the Go standard library for the host platform
install               ## Install the application binary
lint-version          ## Show the golangci-lint version
lint-yaml             ## Format YAML files with prettier
lint                  ## Run the golangci-lint application (install if not found)
loc                   ## Total lines of code table
mod-download          ## Download Go module dependencies
mod-tidy              ## Clean up go.mod and go.sum
pre-build             ## Pre-build all packages to warm cache
release-snap          ## Build snapshot binaries
release-test          ## Run release dry-run (no publish)
release               ## Run production release (requires github_token)
tag-remove            ## Remove local and remote tag (use version=X.Y.Z)
tag-update            ## Force-update tag to current commit (use version=X.Y.Z)
tag                   ## Create and push a new tag (use version=X.Y.Z)
test-ci-no-race       ## CI test suite without race detector
test-ci               ## CI test runs tests with race detection and coverage (no lint - handled separately)
test-cover-race       ## Runs unit tests with race detector and outputs coverage
test-cover            ## Unit tests with coverage (no race)
test-fuzz             ## Run fuzz tests only (no unit tests)
test-no-lint          ## Run only tests (no lint)
test-parallel         ## Run tests in parallel (faster for large repos)
test-race             ## Unit tests with race detector (no coverage)
test-short            ## Run tests excluding integration tests (no lint)
test                  ## Default testing uses lint + unit tests (fast)
uninstall             ## Uninstall the Go binary
update-linter         ## Upgrade golangci-lint (macOS only)
update-releaser       ## Reinstall GoReleaser
update                ## Update dependencies
vet-parallel          ## Run go vet in parallel (faster for large repos)
vet                   ## Run go vet only on your module packages
```
<!-- make-help-end -->

</details>

<details>
<summary><strong><code>GitHub Workflows</code></strong></summary>
<br/>


### 🎛️ The Workflow Control Center

All GitHub Actions workflows in this repository are powered by a single configuration file: [**.env.shared**](.github/.env.shared) – your one-stop shop for tweaking CI/CD behavior without touching a single YAML file! 🎯

This magical file controls everything from:
- **🚀 Go version matrix** (test on multiple versions or just one)
- **🏃 Runner selection** (Ubuntu or macOS, your wallet decides)
- **🔬 Feature toggles** (coverage, fuzzing, linting, race detection)
- **🛡️ Security tool versions** (gitleaks, nancy, govulncheck)
- **🤖 Auto-merge behaviors** (how aggressive should the bots be?)
- **🏷️ PR management rules** (size labels, auto-assignment, welcome messages)

> **Pro tip:** Want to disable code coverage? Just flip `ENABLE_CODE_COVERAGE=false` in [.env.shared](.github/.env.shared) and push. No YAML archaeology required!

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
| [update-python-dependencies.yml](.github/workflows/update-python-dependencies.yml) | Updates Python dependencies for pre-commit hooks in the repository.                                                    |
| [update-pre-commit-hooks.yml](.github/workflows/update-pre-commit-hooks.yml)       | Automatically update versions for [pre-commit](https://pre-commit.com/) hooks                                          |

</details>

<details>
<summary><strong><code>Updating Dependencies</code></strong></summary>
<br/>

To update all dependencies (Go modules, linters, and related tools), run:

```bash
make update
```

This command ensures all dependencies are brought up to date in a single step, including Go modules and any tools managed by the Makefile. It is the recommended way to keep your development environment and CI in sync with the latest versions.

</details>

<details>
<summary><strong><code>Pre-commit Hooks</code></strong></summary>
<br/>

Set up the optional [pre-commit](https://pre-commit.com) hooks to run the same formatting, linting, and tests defined in [AGENTS.md](.github/AGENTS.md) before every commit:

```bash
pip install pre-commit
pre-commit install
```

The hooks are configured in [.pre-commit-config.yaml](.pre-commit-config.yaml) and mirror the CI pipeline.

</details>

<br/>

## 🧪 Examples & Tests

All unit tests and [examples](examples) run via [GitHub Actions](https://github.com/bsv-blockchain/go-batcher/actions) and use [Go version 1.24.x](https://go.dev/doc/go1.24). View the [configuration file](.github/workflows/fortress.yml).

Run all tests (fast):

```bash script
make test
```

Run all tests with race detector (slower):
```bash script
make test-race
```

<br/>

## ⚡ Benchmarks

Run the Go [benchmarks](batcher_benchmark_test.go):

```bash script
make bench
```

<br/>

### Benchmark Results

| Benchmark                                                                            | Description                      |   ns/op |  B/op | allocs/op |
|--------------------------------------------------------------------------------------|----------------------------------|--------:|------:|----------:|
| [BenchmarkBatcherPut](batcher_comprehensive_benchmark_test.go)                       | Basic Put operation              |   145.2 |    11 |         0 |
| [BenchmarkBatcherPutParallel](batcher_comprehensive_benchmark_test.go)               | Concurrent Put operations        |   308.1 |    11 |         0 |
| [BenchmarkBatcherTrigger](batcher_comprehensive_benchmark_test.go)                   | Manual batch trigger             |   466.2 |   248 |         3 |
| [BenchmarkBatcherWithBackground/Foreground](batcher_comprehensive_benchmark_test.go) | Foreground processing            |   146.0 |    61 |         0 |
| [BenchmarkTimePartitionedMapSet](batcher_comprehensive_benchmark_test.go)            | Map Set operation                |   248.0 |   301 |         2 |
| [BenchmarkTimePartitionedMapGet](batcher_comprehensive_benchmark_test.go)            | Map Get operation                |   169.2 |   236 |         2 |
| [BenchmarkTimePartitionedMapDelete](batcher_comprehensive_benchmark_test.go)         | Map Delete operation             |   623.2 |   343 |         3 |
| [BenchmarkTimePartitionedMapCount](batcher_comprehensive_benchmark_test.go)          | Map Count operation              |    0.54 |     0 |         0 |
| [BenchmarkTimePartitionedMapConcurrent](batcher_comprehensive_benchmark_test.go)     | Concurrent map operations        |   345.1 |   258 |         2 |
| [BenchmarkBatcherWithDedupPut](batcher_comprehensive_benchmark_test.go)              | Put with deduplication           |   425.3 |   402 |         4 |
| [BenchmarkBatcher](batcher_benchmark_test.go)                                        | Full batch processing (1M items) | 1,193ms | 895MB |      3.6M |
| [BenchmarkBatcherWithDeduplication](batcher_benchmark_test.go)                       | Deduplication processing         |   803ms | 530MB |      5.9M |

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
| [<img src="https://github.com/icellan.png" height="50" alt="Siggi" />](https://github.com/icellan) |
|:--------------------------------------------------------------------------------------------------:|
|                                [Siggi](https://github.com/icellan)                                 |

<br/>

## 🤝 Contributing
View the [contributing guidelines](.github/CONTRIBUTING.md) and please follow the [code of conduct](.github/CODE_OF_CONDUCT.md).

### How can I help?
All kinds of contributions are welcome :raised_hands:!
The most basic way to show your support is to star :star2: the project, or to raise issues :speech_balloon:.
You can also support this project by [becoming a sponsor on GitHub](https://github.com/sponsors/bsv-blockchain) :clap:

[![Stars](https://img.shields.io/github/stars/bsv-blockchain/go-batcher?label=Please%20like%20us&style=social&v=1)](https://github.com/bsv-blockchain/go-batcher/stargazers)

<br/>

## 📝 License

[![License](https://img.shields.io/badge/license-OpenBSV-blue?style=flat&logo=springsecurity&logoColor=white)](LICENSE)
