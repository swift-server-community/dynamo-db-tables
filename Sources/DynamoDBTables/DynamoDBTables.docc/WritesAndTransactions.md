# Writes and Transactions

Perform multi-item writes using bulk operations or DynamoDB transactions.

## Overview

DynamoDBTables supports writing multiple items in a single operation, either as a non-atomic bulk write (using BatchWriteItem internally) or as an atomic DynamoDB transaction.

## WriteEntry

The `WriteEntry` enum describes what to do with each item:

| Case | Description |
|------|-------------|
| `.insert(new:)` | Insert a new item (fails if exists) |
| `.update(new:existing:)` | Version-checked update |
| `.deleteAtKey(key:)` | Idempotent delete by key |
| `.deleteItem(existing:)` | Version-checked delete |

## Transact Write

Atomic multi-item writes using DynamoDB transactions:

```swift
let entry1: StandardWriteEntry<Customer> = .insert(new: item1)
let entry2: StandardWriteEntry<Customer> = .update(new: updatedItem2, existing: existingItem2)
try await table.transactWrite([entry1, entry2])
```

All entries succeed or fail together. The maximum is 100 items per transaction (entries + constraints combined).

### With Constraints

Add version-check-only constraints — items that must exist at a specific version but aren't being modified:

```swift
let constraint: StandardTransactionConstraintEntry<Customer> = .required(existing: guardItem)
try await table.transactWrite([entry1], constraints: [constraint])
```

The transaction is cancelled if any constraint's version doesn't match.

## Bulk Write

Non-atomic batch write — items are written independently:

```swift
try await table.bulkWrite([.insert(new: item1), .insert(new: item2)])
```

Unlike `transactWrite`, individual item failures don't roll back other items.

## Polymorphic Writes

When writing items of different types in a single operation, use `polymorphicTransactWrite` and `polymorphicBulkWrite` with enums declared via the `@PolymorphicWriteEntry` and `@PolymorphicTransactionConstraintEntry` macros. See <doc:PolymorphicOperations> for details.
