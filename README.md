<p align="center">
<a href="https://github.com/swift-server-community/dynamo-db-tables/actions">
<img src="https://github.com/swift-server-community/dynamo-db-tables/actions/workflows/swift.yml/badge.svg?branch=main" alt="Build - Main Branch">
</a>
<a href="http://swift.org">
<img src="https://img.shields.io/badge/swift-6.2|6.1-orange.svg?style=flat" alt="Swift 6.2 and 6.1 Compatible and Tested">
</a>
<a href="https://swiftpackageindex.com/swift-server-community/dynamo-db-tables/documentation">
<img src="https://img.shields.io/badge/docc-documentation-blue.svg?style=flat" alt="Package documentation">
</a>
<img src="https://img.shields.io/badge/ubuntu-22.04|24.04-yellow.svg?style=flat" alt="Ubuntu 22.04 and 24.04 Tested">
<img src="https://img.shields.io/badge/license-Apache2-blue.svg?style=flat" alt="Apache 2">
</p>

# DynamoDBTables

A type-safe, Sendable-first DynamoDB layer for Swift with optimistic concurrency. DynamoDBTables makes it easy to use DynamoDB from Swift-based applications, with a particular focus on usage with polymorphic database tables — tables that don't have a single schema for all rows.

DynamoDBTables is a fork of [smoke-dynamodb](https://github.com/amzn/smoke-dynamodb) and acknowledges the authors of that original package.

## Features

- ✅ **Strongly typed rows** — `Codable` models with automatic serialization
- ✅ **Optimistic concurrency** — automatic row versioning and conditional writes
- ✅ **Polymorphic tables** — support single table designs - store and query heterogeneous item types
- ✅ **Transactions** — atomic multi-item writes with constraint support
- ✅ **Retrying operations** — automatic retry on concurrency conflicts
- ✅ **Historical rows** — append-only audit trails alongside mutable state
- ✅ **TTL support** — per-item time-to-live timestamps
- ✅ **Testable** — in-memory table implementations for unit testing

## Documentation

Full documentation is available on the [Swift Package Index](https://swiftpackageindex.com/swift-server-community/dynamo-db-tables/documentation/dynamodbtables).

## Installation

Add DynamoDBTables to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/swift-server-community/dynamo-db-tables", from: "0.1.0")
]

.target(
    name: "MyApp",
    dependencies: [
        .product(name: "DynamoDBTables", package: "dynamo-db-tables")
    ]
)
```

## Basic Example

```swift
import DynamoDBTables

struct Customer: Codable, Sendable {
    let name: String
    let email: String
}

// Insert
let key = StandardCompositePrimaryKey(partitionKey: "customer-123", sortKey: "profile")
let item = StandardTypedDatabaseItem.newItem(
    withKey: key,
    andValue: Customer(name: "Alice", email: "alice@example.com")
)
try await table.insertItem(item)

// Retrieve
if let retrieved: StandardTypedDatabaseItem<Customer> = try await table.getItem(forKey: key) {
    print(retrieved.rowValue.name)  // "Alice"
}
```

## License

This library is licensed under the Apache 2.0 License.
