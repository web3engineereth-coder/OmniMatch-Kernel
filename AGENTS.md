# AGENTS.md

This file provides repository-specific instructions for coding agents working in this repo.

## Project Structure & Module Organization

The project is a single-module Maven build targeting Java 21. Production code lives under `src/main/java/cn/inlook/cex`, split by responsibility:

* `domain/model` for order-book entities
* `domain/service` for matching, balances, recovery, and snapshots
* `infrastructure/*` for Disruptor handlers, journaling, and the mock broker

Runtime configuration is in `src/main/resources`, mainly `application.properties` and `logback.xml`.

Tests live under `src/test/java` and are organized around throughput, snapshots, and recovery scenarios.

## Build, Test, and Development Commands

Use Maven from the repository root:

* `mvn clean test`: compile and run the JUnit 5 test suite
* `mvn clean package`: build the jar in `target/`
* `mvn test -Dtest=OmniMatchPerfTest`: run the throughput benchmark test only
* `java -cp target/classes cn.inlook.cex.OmniMatchDisruptorApp`: start the local Disruptor demo without packaging first

If you change any runnable entrypoint or jar path, keep the README aligned with the current Maven build output.

## Coding Style & Naming Conventions

Follow the existing Java style:

* 4-space indentation
* braces on the same line
* `UpperCamelCase` for classes
* `lowerCamelCase` for methods and fields
* `SCREAMING_SNAKE_CASE` for constants

Keep packages under `cn.inlook.cex.*`.

Prefer clear domain names such as `MatchingEngine`, `SnapshotManager`, and `JournalEventHandler`.

Lombok is enabled, but use it sparingly and keep public APIs explicit.

No formatter or Checkstyle plugin is configured, so match surrounding code closely.

## Testing Guidelines

Tests use JUnit 5 through Maven Surefire.

* Name new test classes `*Test`
* Place them under `src/test/java`
* Keep functional assertions in regular tests
* Isolate heavy benchmarks or recovery flows in dedicated classes such as `OmniMatchPerfTest` and `OmniMatchRecoveryTest`

Some tests create local artifacts such as `redo_log.bin` and `engine_snapshot.bin`; do not commit generated binaries.

## Commit & Pull Request Guidelines

Recent history follows Conventional Commit style with scopes, for example:

* `feat(persistence): ...`
* `perf(recovery): ...`
* `fix(disruptor): ...`

Keep commit subjects imperative and narrowly scoped.

For pull requests:

* include a concise summary
* note affected modules
* list verification commands you ran
* attach logs or screenshots only when behavior or performance changed

Link the relevant issue when one exists.

## Configuration & Safety Notes

This code handles persistence and replay paths.

Review changes touching journaling, snapshots, recovery, or Web3 entry points carefully, and keep environment-specific endpoints or secrets out of source control.

## Repo Goal

This repository is a high-performance in-memory matching engine prototype focused on deterministic matching, low-latency order book operations, and production-like engineering clarity.

## Current Priority

Refactor the matching core toward:

* `OrderBook`
* `PriceLevel`
* `OrderNode`
* `orderMap`
* O(1) cancel
* partial fill lifecycle

In this phase, do not expand into full distributed routing, settlement, or externalized account infrastructure unless explicitly requested.

## Engineering Rules

* Preserve single-threaded matching semantics
* Prefer minimal, reviewable changes over wide rewrites
* Do not introduce Redis, Kafka, DB, or other external infrastructure in core refactors
* Keep matching core isolated from persistence, notification, reporting, and other non-core concerns
* Keep code readable and testable
* Add small comments only on core data structures and critical paths
* Do not perform broad architectural rewrites unless explicitly requested

## Done Means

A task is complete only when:

* core code compiles
* relevant tests pass, or new minimal tests are added
* output explains modified files, rationale, and remaining follow-up work

## Review Focus

Pay special attention to:

* order lifecycle correctness
* O(1) cancel path
* partial fill correctness
* price level cleanup
* best bid / best ask correctness
