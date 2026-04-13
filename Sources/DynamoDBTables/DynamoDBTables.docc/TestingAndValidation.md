# Testing and Validation

Three complementary strategies for testing DynamoDBTables code — from fast
in-memory unit tests through to full integration tests against a real
DynamoDB implementation.

## Overview

DynamoDBTables supports three testing tiers, each suited to a different
category of behavior:

1. **Protocol mocks** — for code that depends on a narrow protocol surface
   like ``DynamoDBCompositePrimaryKeysProjection`` (a GSI keys-only
   projection). A mock gives you full control over return values and lets
   you verify the exact query parameters your code passes, without needing
   to populate a data store or run a real DynamoDB instance. Best suited
   for read-only interactions where the test cares about *what the code
   asks for*, not *how the store evaluates it*.

2. **In-memory tables** — fast, deterministic unit tests for core DynamoDB
   semantics. ``InMemoryDynamoDBCompositePrimaryKeyTable`` conforms to the
   same ``DynamoDBCompositePrimaryKeyTable`` protocol as the production
   client, and provides a basic level of state propagation. Use
   this tier for CRUD operations, optimistic concurrency, transactions,
   batch writes, polymorphic queries, and retry logic. No external
   dependencies, no network, no Docker — tests run in milliseconds.

3. **Integration tests against LocalStack (or the actual DynamoDB
   service)** — for behaviors that depend on DynamoDB's server-side
   implementation: PartiQL WHERE clause evaluation, GSI write-through and
   projection semantics, capacity and throttling, and end-to-end SDK
   serialization. These tests run against a real DynamoDB-compatible
   service (typically LocalStack in Docker via
   [swift-local-containers](https://github.com/tachyonics/swift-local-containers)),
   so you get full fidelity at the cost of a container startup per test
   suite.

The three tiers are complementary: use in-memory tables and mocks for the
fast feedback loop during development, and integration tests for confidence
that your code works against a real DynamoDB implementation.

## Protocol Mocks

For code that depends on a narrow protocol surface like
``DynamoDBCompositePrimaryKeysProjection`` (a GSI keys-only projection), a
mock is the simplest and most explicit testing approach. The protocol has a
small read-only surface (three `query` methods), making it straightforward
to mock with [Smockable](https://github.com/tachyonics/smockable):

```swift
import Smockable
@testable import MyApp

@Smock
protocol TestProjection: DynamoDBCompositePrimaryKeysProjection {
    func query<AttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?
    ) async throws -> [CompositePrimaryKey<AttributesType>]

    func query<AttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        exclusiveStartKey: String?
    ) async throws
        -> (keys: [CompositePrimaryKey<AttributesType>], lastEvaluatedKey: String?)

    func query<AttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        scanIndexForward: Bool,
        exclusiveStartKey: String?
    ) async throws
        -> (keys: [CompositePrimaryKey<AttributesType>], lastEvaluatedKey: String?)
}

@Test("Service queries the GSI projection for matching keys")
func serviceQueriesProjection() async throws {
    let expectedKeys = [
        StandardCompositePrimaryKey(partitionKey: "tenant-1", sortKey: "order-1"),
        StandardCompositePrimaryKey(partitionKey: "tenant-1", sortKey: "order-2"),
    ]

    var expectations = MockTestProjection.Expectations()
    when(
        expectations.query(
            forPartitionKey: .exact("tenant-1"),
            sortKeyCondition: .any
        ),
        return: expectedKeys
    )

    let mock = MockTestProjection(expectations: expectations)
    let service = OrderService(projection: mock)

    let orders = try await service.listOrders(forTenant: "tenant-1")
    #expect(orders.count == 2)
}
```

This approach lets you:
- **Control the exact return values** for each query pattern, including empty
  results, pagination tokens, and specific key orderings.
- **Verify the query parameters** your code passes (partition key, sort key
  condition, limit, scan direction) without running a real DynamoDB instance.
- **Test edge cases** (empty results, pagination boundaries) that are
  difficult to reproduce with an in-memory store.

For write-through GSI behavior (verifying that writes to the primary table
correctly propagate to a GSI), use integration tests against LocalStack.

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
- **GSI write-through behavior.** Real GSI behavior (projection semantics,
  attribute subsetting, eventual consistency, index key mapping) is not
  simulated. Use integration tests against LocalStack for end-to-end GSI
  verification.
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
| Code that reads from a GSI keys projection | Mock `DynamoDBCompositePrimaryKeysProjection` |
| CRUD operations, concurrency, retries | In-memory table |
| Transaction and bulk write correctness | In-memory table |
| PartiQL WHERE clause evaluation | Integration test (LocalStack) |
| GSI write-through behavior | Integration test (LocalStack) |
| End-to-end SDK serialization | Integration test (LocalStack) |
| CI pipeline (fast feedback) | Mocks + in-memory table |
| CI pipeline (full confidence) | All three |
