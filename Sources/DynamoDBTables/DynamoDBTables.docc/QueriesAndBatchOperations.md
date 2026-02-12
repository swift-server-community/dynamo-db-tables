# Queries and Batch Operations

Query partitions, paginate results, and batch-get items.

## Overview

DynamoDBTables provides monomorphic queries (where all items share one type) and polymorphic queries (where items can be different types). Both support pagination and sort key conditions.

## Monomorphic Query

Retrieve all items of a single type from a partition:

```swift
let items: [StandardTypedDatabaseItem<Customer>] = try await table.query(
    forPartitionKey: "customer-123",
    sortKeyCondition: .beginsWith("order#")
)
```

This overload internally handles pagination, making multiple calls to DynamoDB if necessary.

## Paginated Query

For explicit control over pagination, use the overload with `limit`, `scanIndexForward`, and `exclusiveStartKey`:

```swift
let (items, lastKey): ([StandardTypedDatabaseItem<Customer>], String?) = try await table.query(
    forPartitionKey: "customer-123",
    sortKeyCondition: nil,
    limit: 100,
    scanIndexForward: true,
    exclusiveStartKey: nil
)
```

The returned `lastKey` can be passed as `exclusiveStartKey` in the next call to continue pagination.

### Full Pagination Loop

```swift
var allItems: [StandardTypedDatabaseItem<Customer>] = []
var exclusiveStartKey: String?
repeat {
    let (page, nextKey): ([StandardTypedDatabaseItem<Customer>], String?) = try await table.query(
        forPartitionKey: "customer-123",
        sortKeyCondition: nil,
        limit: 100,
        scanIndexForward: true,
        exclusiveStartKey: exclusiveStartKey
    )
    allItems += page
    exclusiveStartKey = nextKey
} while exclusiveStartKey != nil
```

## Polymorphic Query

When a partition contains items of different types, use `polymorphicQuery` with a return type declared via the `@PolymorphicOperationReturnType` macro. See <doc:PolymorphicOperations> for details.

## Batch Get

Retrieve multiple items by key using DynamoDB's BatchGetItem API:

```swift
let batch: [StandardCompositePrimaryKey: StandardTypedDatabaseItem<Customer>]
    = try await table.getItems(forKeys: [key1, key2])

if let item1 = batch[key1] {
    print(item1.rowValue.name)
}
```

Missing keys in the returned dictionary indicate those items don't exist. For polymorphic batch gets, use `polymorphicGetItems` (see <doc:PolymorphicOperations>).

## Sort Key Conditions

The `AttributeCondition` enum supports these conditions on the sort key:

| Case | Description |
|------|-------------|
| `.equals(String)` | Sort key equals the value |
| `.lessThan(String)` | Sort key is less than the value |
| `.lessThanOrEqual(String)` | Sort key is less than or equal to the value |
| `.greaterThan(String)` | Sort key is greater than the value |
| `.greaterThanOrEqual(String)` | Sort key is greater than or equal to the value |
| `.between(String, String)` | Sort key is between two values |
| `.beginsWith(String)` | Sort key begins with the prefix |

Pass `nil` for `sortKeyCondition` to return all items in the partition.
