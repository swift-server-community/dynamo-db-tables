# Retrying Operations

Automatically retry operations that fail due to optimistic concurrency conflicts.

## Overview

When multiple writers contend for the same item, optimistic concurrency checks will cause some writes to fail. The retrying operations handle this automatically by re-reading the current state, re-applying your transform, and retrying — up to a configurable number of attempts (default 10).

## retryingUpdateItem

Read the current item, apply a transform to its value, and retry on version conflicts:

```swift
try await table.retryingUpdateItem(
    forKey: key,
    withRetries: 10
) { (existing: Customer) in
    Customer(name: existing.name, email: "newemail@example.com")
}
```

There is also an overload that receives the full database item rather than just the row value:

```swift
try await table.retryingUpdateItem(
    forKey: key,
    withRetries: 10
) { (existingItem: StandardTypedDatabaseItem<Customer>) in
    existingItem.createUpdatedItem(
        withValue: Customer(name: existingItem.rowValue.name, email: "newemail@example.com")
    )
}
```

## retryingUpsertItem

Insert if the item doesn't exist, or update if it does — with automatic retry:

```swift
try await table.retryingUpsertItem(
    forKey: key,
    withRetries: 10,
    newItemProvider: {
        StandardTypedDatabaseItem.newItem(
            withKey: key,
            andValue: Customer(name: "Alice", email: "alice@example.com")
        )
    },
    updatedItemProvider: { existingItem in
        existingItem.createUpdatedItem(
            withValue: Customer(
                name: existingItem.rowValue.name,
                email: "updated@example.com"
            )
        )
    }
)
```

## retryingTransactWrite

Multi-key transactional update with automatic retry. The `writeEntryProvider` closure is called for each key with its current item (or `nil` if it doesn't exist):

```swift
try await table.retryingTransactWrite(forKeys: [key1, key2]) {
    (key, existingItem: StandardTypedDatabaseItem<Customer>?)
    -> StandardWriteEntry<Customer>? in

    if let existingItem {
        let updated = existingItem.createUpdatedItem(
            withValue: Customer(name: "Updated", email: "u@e.com")
        )
        return .update(new: updated, existing: existingItem)
    } else {
        let newItem = StandardTypedDatabaseItem.newItem(
            withKey: key,
            andValue: Customer(name: "New", email: "n@e.com")
        )
        return .insert(new: newItem)
    }
}
```

Return `nil` from the closure to skip writing that key.

## retryingTransactWriteWithHistoricalRows

Same as `retryingTransactWrite` but also writes historical audit entries alongside each primary item:

```swift
try await table.retryingTransactWriteWithHistoricalRows(forKeys: [key1, key2]) {
    (key, existingItem: StandardTypedDatabaseItem<RowWithItemVersion<Customer>>?)
    -> (entry: StandardWriteEntry<RowWithItemVersion<Customer>>,
        historicalEntry: StandardWriteEntry<RowWithItemVersion<Customer>>?)? in

    let item = StandardTypedDatabaseItem.newItem(
        withKey: key,
        andValue: RowWithItemVersion.newItem(
            withValue: Customer(name: "Alice", email: "a@e.com")
        )
    )
    let historicalItem = StandardTypedDatabaseItem.newItem(
        withKey: StandardCompositePrimaryKey(
            partitionKey: "historical.\(key.partitionKey)",
            sortKey: "v0001.\(key.sortKey)"
        ),
        andValue: item.rowValue
    )
    return (
        entry: .insert(new: item),
        historicalEntry: .insert(new: historicalItem)
    )
}
```

## Error Handling

- Throws `DynamoDBTableError.concurrencyError` when all retries are exhausted.
- Throws `DynamoDBTableError.constraintFailure` if a constraint violation occurs during the transaction.
