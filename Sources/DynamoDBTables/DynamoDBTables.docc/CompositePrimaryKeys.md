# Composite Primary Keys

Understand how partition keys and sort keys map to DynamoDB attributes.

## Overview

Every item in DynamoDB is identified by a composite primary key consisting of a **partition key** and a **sort key**. DynamoDBTables models this with the generic `CompositePrimaryKey<AttributesType>` struct.

The partition key determines which partition the item is stored in. All items that share the same partition key are colocated, which means DynamoDB can retrieve them together in a single query. The sort key orders items within a partition and enables efficient range-based filtering — you can query for items whose sort key begins with a prefix, falls within a range, or matches an exact value (see <doc:QueriesAndBatchOperations> for the full set of sort key conditions).

This two-part key design is what makes single-table design practical: by choosing a partition key that groups related entities (e.g. `account-123`) and a sort key that distinguishes them (e.g. `profile`, `order#2024-001`, `invoice#INV-100`), you can store and retrieve heterogeneous item types in a single query against one partition.

## StandardCompositePrimaryKey

The most common usage is `StandardCompositePrimaryKey`, which maps the partition key to an attribute named `PK` and the sort key to an attribute named `SK`:

```swift
let key = StandardCompositePrimaryKey(partitionKey: "customer-123", sortKey: "profile")
```

`StandardCompositePrimaryKey` is a typealias for `CompositePrimaryKey<StandardPrimaryKeyAttributes>`.

## Custom Attribute Names

If your table uses different attribute names — for example, when querying a Global Secondary Index (GSI) — conform a type to `PrimaryKeyAttributes`:

```swift
struct GSI1PrimaryKeyAttributes: PrimaryKeyAttributes {
    static var partitionKeyAttributeName: String { "GSI1PK" }
    static var sortKeyAttributeName: String { "GSI1SK" }
    static var indexName: String? { "GSI1" }
}
```

Then use the generic `CompositePrimaryKey` directly:

```swift
let gsiKey = CompositePrimaryKey<GSI1PrimaryKeyAttributes>(
    partitionKey: "index-value",
    sortKey: "sort-value"
)
```

The `indexName` property defaults to `nil` for primary table queries. Set it to the GSI name when querying an index.

## Querying with Custom Keys

When querying a GSI, the return type drives which key attributes and index are used:

```swift
let (items, nextKey): ([TypedDatabaseItem<GSI1PrimaryKeyAttributes, Customer>], String?) =
    try await table.query(
        forPartitionKey: "index-value",
        sortKeyCondition: nil,
        limit: 100,
        scanIndexForward: true,
        exclusiveStartKey: nil
    )
```
