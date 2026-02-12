# Error Handling

Understand the error types thrown by DynamoDBTables operations.

## Overview

`DynamoDBTableError` is the unified error type for all table operations. Each case indicates a specific failure mode with associated context.

## Common Error Cases

| Error | When It Occurs |
|-------|---------------|
| `.conditionalCheckFailed` | Version mismatch on `updateItem` or `deleteItem(existingItem:)` |
| `.duplicateItem` | Item already exists on `insertItem` |
| `.transactionCanceled(reasons:)` | One or more items in a `transactWrite` failed |
| `.constraintFailure(reasons:)` | Constraint violations in `retryingTransactWrite` |
| `.concurrencyError` | Retries exhausted in retrying operations |
| `.batchFailures(errors:)` | Partial failures in `bulkWrite` |
| `.itemCollectionSizeLimitExceeded` | Transaction exceeds 100-item limit |
| `.typeMismatch` | Stored `RowType` doesn't match the requested decode type |
| `.unexpectedType` | Polymorphic decode failure — stored type isn't in the return type enum |

## Handling Transaction Errors

The `reasons` array in `.transactionCanceled` corresponds positionally to the entries in the transaction:

```swift
do {
    try await table.transactWrite(entries)
} catch DynamoDBTableError.transactionCanceled(let reasons) {
    for reason in reasons {
        if case .conditionalCheckFailed(let pk, let sk, _) = reason {
            print("Version conflict on \(pk)/\(sk)")
        }
    }
}
```

## Handling Concurrency Errors

Retrying operations throw `.concurrencyError` when all retry attempts are exhausted:

```swift
do {
    try await table.retryingUpdateItem(forKey: key, withRetries: 5) { (existing: Customer) in
        Customer(name: existing.name, email: "new@example.com")
    }
} catch DynamoDBTableError.concurrencyError(let pk, let sk, _) {
    print("Failed to update \(pk)/\(sk) after all retries")
}
```
