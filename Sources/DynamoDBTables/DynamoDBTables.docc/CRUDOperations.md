# CRUD Operations

Insert, retrieve, update, and delete items with optimistic concurrency control.

## Overview

All operations are performed through a type conforming to the `DynamoDBCompositePrimaryKeyTable` protocol. In production, use `AWSDynamoDBCompositePrimaryKeyTable`; for testing, use `InMemoryDynamoDBCompositePrimaryKeyTable`.

## insertItem

Insert a new item into the table. This operation throws `DynamoDBTableError.duplicateItem` if an item with the same key already exists.

```swift
let key = StandardCompositePrimaryKey(partitionKey: "customer-123", sortKey: "profile")
let item = StandardTypedDatabaseItem.newItem(
    withKey: key,
    andValue: Customer(name: "Alice", email: "alice@example.com")
)
try await table.insertItem(item)
```

## getItem

Retrieve an item by its composite primary key. Returns `nil` if the item doesn't exist. The type annotation drives which `RowType` to decode:

```swift
let item: StandardTypedDatabaseItem<Customer>? = try await table.getItem(forKey: key)
```

The operation throws `DynamoDBTableError.typeMismatch` if the stored `RowType` doesn't match the requested type.

## updateItem

Update an existing item using optimistic concurrency. You must pass both the new item and the existing item you previously retrieved — the operation verifies the row version hasn't changed:

```swift
guard let existing: StandardTypedDatabaseItem<Customer> = try await table.getItem(forKey: key) else {
    // handle missing item
    return
}
let updated = existing.createUpdatedItem(
    withValue: Customer(name: "Alice B.", email: "alice@example.com")
)
try await table.updateItem(newItem: updated, existingItem: existing)
```

`createUpdatedItem(withValue:)` automatically increments the row version. The operation throws `DynamoDBTableError.conditionalCheckFailed` if another writer has modified the item since you retrieved it.

## deleteItem

There are two overloads:

**Idempotent delete by key** — succeeds even if the item doesn't exist:

```swift
try await table.deleteItem(forKey: key)
```

**Version-checked delete** — fails if the item has been modified since you retrieved it:

```swift
guard let existing: StandardTypedDatabaseItem<Customer> = try await table.getItem(forKey: key) else {
    return
}
try await table.deleteItem(existingItem: existing)
```

## clobberItem

Unconditional put — overwrites whatever is in the table with no version check:

```swift
try await table.clobberItem(item)
```

Use this when you intentionally want to overwrite any existing data at this key.
