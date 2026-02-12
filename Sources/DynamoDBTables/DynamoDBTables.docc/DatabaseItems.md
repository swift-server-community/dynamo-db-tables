# Database Items

Explore the typed database item hierarchy, automatic versioning, and TTL support.

## Overview

`TypedTTLDatabaseItem` is the core type that wraps your row value with metadata managed by DynamoDBTables. It handles type safety across polymorphic tables, automatic row versioning for optimistic concurrency, and optional time-to-live (TTL) support.

## Type Alias Chain

The library provides convenience typealiases to simplify common usage:

- `StandardTypedDatabaseItem<RowType>` = `TypedDatabaseItem<StandardPrimaryKeyAttributes, RowType>`
- `TypedDatabaseItem<AttributesType, RowType>` = `TypedTTLDatabaseItem<AttributesType, RowType, StandardTimeToLiveAttributes>`

For most use cases, `StandardTypedDatabaseItem<YourType>` is all you need.

## Properties

A `TypedTTLDatabaseItem` carries these properties:

- `compositePrimaryKey` — the partition key and sort key for this item
- `createDate` — timestamp when the item was first created
- `rowStatus` — contains `rowVersion` (Int) and `lastUpdatedDate` (Date)
- `rowValue` — your `Codable` payload
- `timeToLive` — optional TTL timestamp

## Row Versioning

When you create a new item, it starts at row version 1:

```swift
let item = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
// item.rowStatus.rowVersion == 1
```

Calling `createUpdatedItem(withValue:)` increments the version:

```swift
let updated = item.createUpdatedItem(withValue: newPayload)
// updated.rowStatus.rowVersion == 2
```

This version is checked on `updateItem` to implement optimistic concurrency — if another writer has incremented the version, your update will fail.

## TTL Support

To set a time-to-live on an item, pass a `TimeToLive` value when creating or updating:

```swift
let item = StandardTypedDatabaseItem.newItem(
    withKey: key,
    andValue: payload,
    andTimeToLive: StandardTimeToLive(timeToLiveTimestamp: 1_700_000_000)
)
```

DynamoDB will automatically delete the item shortly after the specified Unix timestamp. The default TTL attribute name is `ExpireDate` (via `StandardTimeToLiveAttributes`). You can customize this by conforming a type to `TimeToLiveAttributes`:

```swift
struct MyTTLAttributes: TimeToLiveAttributes {
    static var timeToLiveAttributeName: String { "MyTTLAttribute" }
}
```

## RowWithItemVersion

For scenarios that need dual versioning — a row version for optimistic concurrency and a separate item version for business logic — use `RowWithItemVersion`:

```swift
let versionedPayload = RowWithItemVersion.newItem(
    withValue: Customer(name: "Alice", email: "alice@example.com")
)
let item = StandardTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
// item.rowStatus.rowVersion == 1
// item.rowValue.itemVersion == 1
```

`RowWithItemVersion` is used with the historical row operations described in <doc:HistoricalRows>.
