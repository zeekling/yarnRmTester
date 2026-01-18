# AGENTS.md

This file provides guidelines for agentic coding assistants working in this repository.

## Project Overview

yarnRmTester is a Hadoop YARN ResourceManager stress-testing tool that simulates massive pressure scenarios with fake NodeManagers and Applications. It creates thousands of Fake NM nodes to optimize RM performance under load.

- **Build System**: Maven
- **Language**: Java 17
- **Primary Framework**: Apache Hadoop 3.4.1
- **Testing**: JUnit 3.8.1 (test infrastructure not fully implemented)

## Build and Test Commands

### Building the Project
```bash
mvn package
```
Compiles source, runs tests, and packages JAR with dependencies to `target/lib/`.

### Running the Application

**Fake NodeManager mode** (creates fake NM nodes):
```bash
java -cp target/yarnRmTester-1.0-SNAPSHOT.jar:target/lib/* org.apache.hadoop.sls.SLSNodeManager /path/to/config/
```

**SLSRunner mode** (submits stress test jobs):
```bash
java -cp target/yarnRmTester-1.0-SNAPSHOT.jar:target/lib/* org.apache.hadoop.sls.SLSRunner /path/to/config/
```

### Running Tests
Tests go in `src/test/java/` following `src/main/java/` package structure.

Run all tests:
```bash
mvn test
```

Run single test:
```bash
mvn test -Dtest=ClassName#methodName
```

### Clean Build
```bash
mvn clean package
```

## Code Style Guidelines

### Imports
Order: Standard Java imports → Third-party (org.apache.*) → Static imports (bottom). No wildcard imports.

```java
import java.io.File;
import java.util.ArrayList;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.sls.nm.NodeManagerCommon.FAKE_NODE_MANAGER_MAP;
```

### Formatting
- 4 spaces indentation (no tabs)
- Opening brace on same line, closing on new line
- One blank line between methods
- Lines under 120 characters preferred

### Naming Conventions
| Element | Convention | Example |
|---------|------------|---------|
| Classes | PascalCase | `FakeJob`, `SLSConfig` |
| Methods | camelCase | `getFakeNMCount()`, `submit()` |
| Constants | UPPER_SNAKE_CASE | `SLS_NM_COUNT`, `LOG` |
| Instance vars | camelCase, private | `config`, `jobName` |
| Static vars | camelCase | `recordFactory`, `executor` |
| Packages | lowercase.dots | `org.apache.hadoop.sls` |

### Types
Use interface types for declarations (`List<>`, `Map<>`), specify implementation for initialization. Use `ConcurrentHashMap` for thread-safe maps.

```java
private final Map<ApplicationId, List<Container>> containers = new ConcurrentHashMap<>();
private final List<Future<?>> futures = new ArrayList<>();
```

### Error Handling
Multi-catch related exceptions: `catch (IOException | YarnException e)`. Log warnings before handling. Convert `InterruptedException` to `RuntimeException`.

```java
try {
    fakeNodeManager.heartbeat();
} catch (IOException | YarnException e) {
    LOG.warn("heart beat failed");
}
```

### Logging
Always use SLF4J with static final LOG field. Use parameterized logging.

```java
private static final Logger LOG = LoggerFactory.getLogger(ClassName.class);
LOG.info("Fake NM count={}", count);
LOG.warn("submit job failed");
LOG.debug("begin heartbeat for {}", nodeId);
```

### Concurrency
Use `ExecutorService` and thread pools. Track async tasks with `Future<?>`. Wait completion with `CommonUtils.waitFutures(List<Future<?>>)`.

```java
ExecutorService executor = Executors.newFixedThreadPool(poolSize);
List<Future<?>> futures = new ArrayList<>();
Future<?> future = executor.submit(runnable);
CommonUtils.waitFutures(futures);
```

### Configuration
Load from `java.util.Properties`. Store keys as `private static final String` constants. Provide defaults: `Integer.parseInt(properties.getProperty(KEY, "default"))`.

### Comments
Javadoc only for public API methods. Minimal inline comments for non-obvious logic. Use TODO for known issues.

### Resource Management
Use Hadoop `Resource`, `ResourceInformation`, `RecordFactory`, and `Records.newRecord()`.

```java
private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
Resource capability = Resource.newInstance(memory, vcore);
GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
```

## Project Structure

```
src/main/java/org/apache/hadoop/sls/
├── SLSRunner.java              # Job submission entry point
├── SLSNodeManager.java         # Fake NM creation entry point
├── config/SLSConfig.java       # Configuration loader
├── job/FakeJob.java            # Fake job submission
├── job/FakeApplication.java    # Fake application lifecycle
├── nm/YarnFakeNodeManager.java # Fake NodeManager impl
├── nm/NMHttpHandler.java       # HTTP server for NM
├── nm/NodeManagerCommon.java   # Shared NM utilities
├── nm/JobStatUpdater.java      # Job status updates
└── util/CommonUtils.java       # Shared utility methods
```

## Key Dependencies

- `org.apache.hadoop:hadoop-common:3.4.1`
- `org.apache.hadoop:hadoop-yarn-api:3.4.1`
- `org.apache.hadoop:hadoop-yarn-server-resourcemanager:3.4.1`
- `org.apache.hadoop:hadoop-yarn-client:3.4.1`
- `org.apache.hadoop:hadoop-yarn-server-nodemanager:3.4.1`

## Configuration Files

Place in directory passed as first argument to main classes:

- `core-site.xml` - Hadoop core config
- `hdfs-site.xml` - HDFS config
- `yarn-site.xml` - YARN config (modify resource allocation limits)
- `fake.properites` - SLS-specific config (NM count, ports, job settings)
