<p align="center">
<a href="https://github.com/swift-server-community/dynamo-db-tables/actions">
<img src="https://github.com/swift-server-community/dynamo-db-tables/actions/workflows/swift.yml/badge.svg?branch=main" alt="Build - Main Branch">
</a>
<a href="http://swift.org">
<img src="https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fswift-server-community%2Fdynamo-db-tables%2Fbadge%3Ftype%3Dswift-versions" alt="Swift Version compatibility">
</a>
<a href="https://swiftpackageindex.com/swift-server-community/dynamo-db-tables/documentation">
<img src="https://img.shields.io/badge/docc-documentation-blue.svg?style=flat" alt="Package documentation">
</a>
<a href="https://codecov.io/gh/swift-server-community/dynamo-db-tables">
<img src="https://img.shields.io/codecov/c/github/swift-server-community/dynamo-db-tables?logo=codecov&label=codecov" alt="Code Coverage">
</a>
<img src="https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fswift-server-community%2Fdynamo-db-tables%2Fbadge%3Ftype%3Dplatforms" alt="Platform Compatibility">
<img src="https://img.shields.io/badge/license-Apache2-yellow.svg?style=flat" alt="Apache 2">
</p>

# DynamoDBTables

A type-safe, Sendable-first DynamoDB layer for Swift with optimistic concurrency. DynamoDBTables makes it easy to use DynamoDB from Swift-based applications, with a particular focus on usage with polymorphic database tables — tables that don't have a single schema for all rows. It integrates with [aws-sdk-swift](https://github.com/awslabs/aws-sdk-swift) by default and [Soto](https://github.com/soto-project/soto) by an opt-in package trait.

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

Add DynamoDBTables to your `Package.swift`. Choose the SDK integration that matches your project:

### With [aws-sdk-swift](https://github.com/awslabs/aws-sdk-swift) (default)

```swift
dependencies: [
    .package(url: "https://github.com/swift-server-community/dynamo-db-tables", from: "0.1.0")
]

.target(
    name: "MyApp",
    dependencies: [
        .product(name: "DynamoDBTablesAWS", package: "dynamo-db-tables")
    ]
)
```

### With [Soto](https://github.com/soto-project/soto)

```swift
dependencies: [
    .package(url: "https://github.com/swift-server-community/dynamo-db-tables", traits: ["SOTOSDK"], from: "0.1.0")
]

.target(
    name: "MyApp",
    dependencies: [
        .product(name: "DynamoDBTablesSoto", package: "dynamo-db-tables")
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

## What Gets Stored

The `insertItem` call above produces the following row in DynamoDB:

| PK | SK | CreateDate | RowType | RowVersion | LastUpdatedDate | Name | Email |
|----|-----|------------|---------|------------|-----------------|------|-------|
| customer-123 | profile | 2025-01-15T10:30:00Z | Customer | 1 | 2025-01-15T10:30:00Z | Alice | alice@example.com |

- **PK** and **SK** are the partition and sort key attributes, derived from the `StandardCompositePrimaryKey` you provided.
- **CreateDate**, **RowType**, **RowVersion**, and **LastUpdatedDate** are managed automatically by the library. `RowType` records which Swift type was stored so it can be decoded back correctly, and `RowVersion` enables optimistic concurrency — it starts at 1 and increments on each update.
- **Name** and **Email** are your payload fields from the `Customer` struct, serialized by the library's `Codable` encoding (attribute names are automatically capitalized).

## License

This library is licensed under the Apache 2.0 License.
