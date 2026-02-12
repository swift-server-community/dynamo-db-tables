# Testing

Use in-memory table implementations for unit testing without connecting to DynamoDB.

## Overview

DynamoDBTables provides in-memory implementations of its protocols that are interchangeable with production code, enabling fast and reliable unit tests.

## InMemoryDynamoDBCompositePrimaryKeyTable

A full in-memory implementation that simulates DynamoDB behavior using an in-memory dictionary:

```swift
let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

let key = StandardCompositePrimaryKey(partitionKey: "pk", sortKey: "sk")
let item = StandardTypedDatabaseItem.newItem(withKey: key, andValue: myPayload)
try await table.insertItem(item)

let retrieved: StandardTypedDatabaseItem<MyType>? = try await table.getItem(forKey: key)
```

This table supports all operations including queries, transactions, and bulk writes. More advanced behaviors such as indexes are not simulated.

## SimulateConcurrencyDynamoDBCompositePrimaryKeyTable

A wrapper around another table that injects version conflicts, useful for testing retry logic:

```swift
let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
    wrappedDynamoDBTable: wrappedTable,
    simulateConcurrencyModifications: 5,  // fail first 5 attempts
    simulateOnInsertItem: false           // only fail on updates
)
```

Use this with retrying operations to verify they handle concurrency correctly:

```swift
try await table.retryingUpdateItem(forKey: key, withRetries: 10) { (existing: MyType) in
    MyType(/* updated fields */)
}
```

Both types conform to `DynamoDBCompositePrimaryKeyTable`, so they can be used anywhere the protocol is expected — the same code runs against the in-memory table in tests and against `AWSDynamoDBCompositePrimaryKeyTable` in production.
