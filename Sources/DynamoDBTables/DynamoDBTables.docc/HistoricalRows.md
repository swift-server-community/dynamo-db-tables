# Historical Rows

Store current state alongside an append-only audit trail of all previous versions.

## Overview

The historical row pattern stores a primary item (the current state) and a separate historical item for each version. This provides an audit trail of all changes. These operations use DynamoDB transactions to ensure the primary and historical items are written atomically.

## insertItemWithHistoricalRow

Atomically insert both a primary item and its initial historical record:

```swift
let primaryItem = StandardTypedDatabaseItem.newItem(
    withKey: StandardCompositePrimaryKey(partitionKey: "customer-123", sortKey: "profile"),
    andValue: RowWithItemVersion.newItem(withValue: customer)
)
let historicalItem = StandardTypedDatabaseItem.newItem(
    withKey: StandardCompositePrimaryKey(
        partitionKey: "historical.customer-123",
        sortKey: "v00001.profile"
    ),
    andValue: primaryItem.rowValue
)
try await table.insertItemWithHistoricalRow(primaryItem: primaryItem, historicalItem: historicalItem)
```

## updateItemWithHistoricalRow

Atomically update the primary item and insert a new historical record:

```swift
try await table.updateItemWithHistoricalRow(
    primaryItem: updatedItem,
    existingItem: existingItem,
    historicalItem: historicalItem
)
```

## retryingUpdateItemWithHistoricalRow

Automatically retry on concurrency conflicts. The `primaryItemProvider` transforms the existing item, and the `historicalItemProvider` creates the historical record from the updated item:

```swift
try await table.retryingUpdateItemWithHistoricalRow(
    forKey: key,
    primaryItemProvider: { existingItem in
        existingItem.createUpdatedItem(
            withValue: existingItem.rowValue.createUpdatedItem(withValue: updatedCustomer)
        )
    },
    historicalItemProvider: { updatedItem in
        StandardTypedDatabaseItem.newItem(
            withKey: StandardCompositePrimaryKey(
                partitionKey: "historical.\(key.partitionKey)",
                sortKey: "v\(String(format: "%05d", updatedItem.rowValue.itemVersion)).\(key.sortKey)"
            ),
            andValue: updatedItem.rowValue
        )
    }
)
```

## retryingUpsertItemWithHistoricalRow

Insert-or-update variant with separate providers for new items, updated items, and historical records:

```swift
try await table.retryingUpsertItemWithHistoricalRow(
    forKey: key,
    withRetries: 10,
    newItemProvider: {
        StandardTypedDatabaseItem.newItem(
            withKey: key,
            andValue: RowWithItemVersion.newItem(withValue: customer)
        )
    },
    updatedItemProvider: { existingItem in
        existingItem.createUpdatedItem(
            withValue: existingItem.rowValue.createUpdatedItem(withValue: updatedCustomer)
        )
    },
    historicalItemProvider: { writtenItem in
        StandardTypedDatabaseItem.newItem(
            withKey: StandardCompositePrimaryKey(
                partitionKey: "historical.\(key.partitionKey)",
                sortKey: "v\(String(format: "%05d", writtenItem.rowValue.itemVersion)).\(key.sortKey)"
            ),
            andValue: writtenItem.rowValue
        )
    }
)
```

## What Gets Stored

Consider a customer whose email is updated twice using the APIs above. After the initial insert and two updates, the table contains these rows:

**After initial insert** (version 1):

| PK | SK | RowVersion | ItemVersion | Name | Email |
|----|----|------------|-------------|------|-------|
| customer-123 | profile | 1 | 1 | Alice | alice@example.com |
| historical.customer-123 | v00001.profile | 1 | 1 | Alice | alice@example.com |

The primary row holds the current state. The historical row is an immutable snapshot of version 1.

**After first update** (version 2 — email changed):

| PK | SK | RowVersion | ItemVersion | Name | Email |
|----|----|------------|-------------|------|-------|
| customer-123 | profile | 2 | 2 | Alice | alice@company.com |
| historical.customer-123 | v00001.profile | 1 | 1 | Alice | alice@example.com |
| historical.customer-123 | v00002.profile | 1 | 2 | Alice | alice@company.com |

The primary row is updated in place (its `RowVersion` and `ItemVersion` both increment). A new historical row is inserted for version 2. The version 1 historical row is untouched.

**After second update** (version 3 — name changed):

| PK | SK | RowVersion | ItemVersion | Name | Email |
|----|----|------------|-------------|------|-------|
| customer-123 | profile | 3 | 3 | Alice B. | alice@company.com |
| historical.customer-123 | v00001.profile | 1 | 1 | Alice | alice@example.com |
| historical.customer-123 | v00002.profile | 1 | 2 | Alice | alice@company.com |
| historical.customer-123 | v00003.profile | 1 | 3 | Alice B. | alice@company.com |

This example highlights the difference between `RowVersion` and `ItemVersion`. `RowVersion` counts how many times the physical DynamoDB row has been written — the primary row's `RowVersion` increments on each update, while each historical row has `RowVersion` 1 because it was inserted once and never modified. `ItemVersion` tracks the logical version of the entity — it increments across both the primary and historical rows, so you can see that `v00002.profile` represents the second version of this customer regardless of how many times the underlying rows were written.

Because the historical rows share a partition key (`historical.customer-123`) and use a version-prefixed sort key, you can query the `historical.customer-123` partition to page through the full change history, or retrieve a specific version directly by its sort key (e.g. `v00002.profile`).
