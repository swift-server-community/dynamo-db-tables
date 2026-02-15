# Configuration

Control table behavior with `DynamoDBTableConfiguration` and load settings from external sources using swift-configuration.

## Overview

``DynamoDBTableConfiguration`` controls runtime behavior for a DynamoDB table — whether reads are strongly consistent, whether single quotes are escaped in PartiQL statements, and how failed requests are retried. ``RetryConfiguration`` defines the retry strategy: how many retries, the backoff intervals, and whether jitter is applied.

Both types provide an `init(from: ConfigReader)` initializer that reads values from any [swift-configuration](https://github.com/apple/swift-configuration) provider (environment variables, JSON files, and others), falling back to defaults for any key that isn't set.

## Default Values

### DynamoDBTableConfiguration

| Property | Type | Default |
|----------|------|---------|
| `consistentRead` | `Bool` | `true` |
| `escapeSingleQuoteInPartiQL` | `Bool` | `false` |
| `retry` | ``RetryConfiguration`` | `.default` |

### RetryConfiguration

| Property | Type | Default |
|----------|------|---------|
| `numRetries` | `Int` | `5` |
| `baseRetryInterval` | `UInt32` | `500` (ms) |
| `maxRetryInterval` | `UInt32` | `10000` (ms) |
| `exponentialBackoff` | `Double` | `2` |
| `jitter` | `Bool` | `true` |

## Loading from swift-configuration

Add [swift-configuration](https://github.com/apple/swift-configuration) to your package (DynamoDBTables already depends on it), then create a `ConfigReader` and pass it to the initializer:

```swift
import Configuration

let config = ConfigReader(providers: [
    EnvironmentVariablesProvider(),
    try await FileProvider<JSONSnapshot>(filePath: "/etc/myapp/config.json")
])

let tableConfig = DynamoDBTableConfiguration(from: config.scoped(to: "dynamodb"))
```

With a JSON file like:

```json
{
    "dynamodb": {
        "consistentRead": false,
        "retry": {
            "numRetries": 3,
            "baseRetryInterval": 1000
        }
    }
}
```

Any key that isn't present in the configuration falls back to its default value. In the example above, `escapeSingleQuoteInPartiQL` defaults to `false` and the remaining retry properties (`maxRetryInterval`, `exponentialBackoff`, `jitter`) keep their defaults.

## Configuration Key Reference

Retry keys are nested under a `retry` prefix when used through `DynamoDBTableConfiguration`. When constructing a `RetryConfiguration` directly, the keys are top-level.

| Key | Type | Description |
|-----|------|-------------|
| `consistentRead` | Bool | Use strongly consistent reads |
| `escapeSingleQuoteInPartiQL` | Bool | Escape `'` in PartiQL statements |
| `retry.numRetries` | Int | Maximum number of retry attempts |
| `retry.baseRetryInterval` | Int | Initial retry interval in milliseconds |
| `retry.maxRetryInterval` | Int | Maximum retry interval in milliseconds |
| `retry.exponentialBackoff` | Double | Multiplier applied to the interval after each retry |
| `retry.jitter` | Bool | Randomize retry intervals to reduce contention |
