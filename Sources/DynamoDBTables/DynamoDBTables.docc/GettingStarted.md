# Getting Started

Add DynamoDBTables to your project, define a model, and perform your first insert and retrieval.

## Overview

This guide walks you through adding DynamoDBTables as a dependency, defining a `Codable` model, and performing basic insert and retrieval operations.

## Add the Dependency

Add DynamoDBTables to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/swift-server-community/dynamo-db-tables", from: "0.1.0")
]
```

Then add the product to your target:

```swift
.target(
    name: "MyApp",
    dependencies: [
        .product(name: "DynamoDBTables", package: "dynamo-db-tables")
    ]
)
```

## Define a Model

Any `Codable & Sendable` struct can be stored as a row value:

```swift
import DynamoDBTables

struct Customer: Codable, Sendable {
    let name: String
    let email: String
}
```

## Insert and Retrieve

Create a composite primary key, wrap your model in a database item, and insert it:

```swift
let key = StandardCompositePrimaryKey(partitionKey: "customer-123", sortKey: "profile")
let item = StandardTypedDatabaseItem.newItem(
    withKey: key,
    andValue: Customer(name: "Alice", email: "alice@example.com")
)
try await table.insertItem(item)
```

Retrieve the item by key:

```swift
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

## Next Steps

- <doc:CRUDOperations> — Learn about all CRUD operations including optimistic concurrency on updates.
- <doc:CompositePrimaryKeys> — Understand how partition and sort keys work.
- <doc:DatabaseItems> — Explore the database item type hierarchy, versioning, and TTL.
- <doc:Testing> — Set up in-memory tables for unit testing.
