# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an OpenTelemetry extension to instrument Apache Spark applications. It's a Java-based project that extends the OpenTelemetry Java agent to provide distributed tracing for Spark jobs, stages, and tasks.

## Build Commands

- **Build the project**: `./gradlew build`
- **Build shadow JAR**: `./gradlew shadowJar` (creates the all-in-one extension JAR)
- **Create extended agent**: `./gradlew extendedAgent` (packages extension into OpenTelemetry agent)
- **Run tests**: `./gradlew test`
- **Format code**: `./gradlew spotlessApply`
- **Check code formatting**: `./gradlew spotlessCheck`

The main build artifact is `build/libs/opentelemetry-spark-<version>-all.jar`.

## Architecture

### Core Components

The instrumentation works by intercepting Spark's internal components:

- **ApacheSparkInstrumentationModule**: Main entry point that registers all type instrumentations
- **ApacheSparkSingletons**: Central registry for OpenTelemetry instances, context management, and shared utilities
- **LiveListenerBusInstrumentation**: Instruments Spark's event bus to capture job/stage lifecycle events
- **DAGSchedulerInstrumentation**: Captures DAG scheduler events for job and stage tracking
- **TaskRunnerInstrumentation**: Instruments task execution on executors
- **TaskInstrumentation**: Provides task-level instrumentation

### Key Patterns

- Uses ByteBuddy for runtime bytecode instrumentation
- Follows OpenTelemetry Java agent extension patterns
- Context propagation through job → stage → task hierarchy
- Reflection-based access to Spark internal fields when necessary
- Event-driven instrumentation using Spark's listener bus

### Span Hierarchy

```
spark_job (from DAGScheduler events)
└── spark_stage (created when stage starts)
    └── spark_task (from TaskRunner execution)
```

### Context Management

- Job contexts stored in `JOB_CONTEXT_REGISTRY`
- Stage contexts stored in `STAGE_CONTEXT_REGISTRY`
- Parent-child relationships maintained through OpenTelemetry context propagation

## Key Files

- `src/main/java/io/opentelemetry/javaagent/instrumentation/spark/` - Main instrumentation code
- `ApacheSparkSingletons.java` - Central utilities and context management
- `build.gradle` - Build configuration with muzzle checks for Spark compatibility
- `spotless.license.java` - License header template

## Dependencies

- Targets Spark 2.4+ (both Scala 2.11 and 2.12 variants)
- Uses OpenTelemetry Java agent APIs
- Muzzle plugin ensures compatibility across Spark versions
- Shadow plugin creates fat JARs for deployment

## Code Style

- Google Java Format enforced via Spotless
- MIT license headers required on all Java files
- No comments added unless explicitly requested
- Java 11 source/target compatibility