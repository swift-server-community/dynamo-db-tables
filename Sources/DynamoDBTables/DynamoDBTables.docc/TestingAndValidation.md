# Testing and Validation

Use in-memory table implementations for unit testing and LocalStack (or the actual DynamoDB service) for integration testing.

## Overview

DynamoDBTables supports two complementary testing strategies:

- **In-memory tables** for fast, reliable unit tests of core DynamoDB semantics — CRUD, optimistic concurrency, transactions, batch operations, and retry logic. No external dependencies required.
- **Integration tests against LocalStack/or the actual DynamoDB service** for features that depend on DynamoDB's server-side behavior — PartiQL WHERE clause evaluation, GSI projections, capacity limits, and any scenario where the in-memory simulation isn't faithful enough.

The two approaches are complementary: use in-memory tables for the fast feedback loop during development, and integration tests against LocalStack for greater confidence .

## In-Memory Tables

### InMemoryDynamoDBCompositePrimaryKeyTable

A full in-memory implementation that simulates DynamoDB behavior using an in-memory dictionary:

```swift
let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

let key = StandardCompositePrimaryKey(partitionKey: "pk", sortKey: "sk")
let item = StandardTypedDatabaseItem.newItem(withKey: key, andValue: myPayload)
try await table.insertItem(item)

let retrieved: StandardTypedDatabaseItem<MyType>? = try await table.getItem(forKey: key)
```

This table supports all operations including queries, transactions, and bulk writes.

### SimulateConcurrencyDynamoDBCompositePrimaryKeyTable

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

### What the in-memory table covers well

- Insert, get, update, delete, and clobber operations
- Optimistic concurrency (version checks on conditional writes)
- Duplicate-key detection on insert
- Stale-version detection on update
- Transactions (`transactWrite`) with multi-item atomicity
- Bulk writes (`bulkWrite`) via PartiQL batch statements
- Partition-key queries with sort key conditions
- Polymorphic operations (heterogeneous item types in the same table)
- Retrying operations under simulated concurrency

### What requires integration tests

The in-memory table cannot simulate all DynamoDB behaviors. The following
features should be tested against the actual DynamoDB service or LocalStack:

- **PartiQL WHERE clause evaluation.** The `execute` method's
  `additionalWhereClause` parameter passes a PartiQL expression to DynamoDB
  for server-side evaluation. The in-memory table cannot parse or evaluate
  PartiQL and will fatal-error if an `additionalWhereClause` is provided.
- **GSI projections and secondary index queries.** While
  `InMemoryDynamoDBCompositePrimaryKeyTableWithIndex` simulates basic GSI
  behavior, real GSI projection behavior (attribute subsetting, eventual
  consistency) is not faithfully reproduced.
- **Capacity and throttling behavior.** The in-memory table never throttles
  and has no concept of provisioned or on-demand capacity.
- **Item size limits and request size limits.** The in-memory table does not
  enforce DynamoDB's 400KB item size limit or 25-item transaction limit.

## Integration Tests with LocalStack

For features that require real DynamoDB behavior, one option is to use
[swift-local-containers](https://github.com/tachyonics/swift-local-containers)
to run tests against a LocalStack instance. This provides a real DynamoDB
implementation running locally in Docker, so PartiQL evaluation, GSI
projections, and capacity behavior all work as they would in production.

```swift
import ContainerMacrosLib
import ContainerTestSupport
import DynamoDBTables

@Containers
struct DynamoDBContainers {
    @LocalStackContainer(stackName: "my-integration")
    var stack: DynamodbTableOutputs
}

@Suite(
    DynamoDBContainers.containerTrait,
    .tags(.integration),
    .enabled(if: containerRuntimeAvailable, "Container runtime required")
)
struct MyIntegrationTests {
    let containers = DynamoDBContainers()

    private func makeTable() throws -> any DynamoDBCompositePrimaryKeyTable {
        let stack = containers.stack
        return try createDynamoDBTable(
            tableName: stack.tableName,
            endpoint: stack.awsEndpoint
        )
    }

    @Test("Insert and retrieve round-trips correctly")
    func insertAndGet() async throws {
        let table = try makeTable()
        let key = CompositePrimaryKey(
            partitionKey: UUID().uuidString,
            sortKey: "sk"
        )
        let item = StandardTypedDatabaseItem.newItem(
            withKey: key,
            andValue: MyPayload(message: "hello")
        )
        try await table.insertItem(item)

        let retrieved: StandardTypedDatabaseItem<MyPayload>? =
            try await table.getItem(forKey: key)
        #expect(retrieved?.rowValue.message == "hello")
    }
}
```

## Choosing a Strategy

| Scenario | Recommended approach |
|---|---|
| CRUD operations, concurrency, retries | In-memory table |
| Transaction and bulk write correctness | In-memory table |
| PartiQL WHERE clause evaluation | Integration test (LocalStack) |
| GSI projection behavior | Integration test (LocalStack) |
| End-to-end SDK serialization | Integration test (LocalStack) |
| CI pipeline (fast feedback) | In-memory table |
| CI pipeline (full confidence) | Both |
