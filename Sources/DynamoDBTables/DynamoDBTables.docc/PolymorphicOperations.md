# Polymorphic Operations

Work with DynamoDB tables that store heterogeneous item types.

## Overview

DynamoDB tables commonly store items of different types in the same partition. Since Swift is strongly typed, DynamoDBTables provides macros to declare enums that represent the possible types for reads and writes.

## Polymorphic Reads

### @PolymorphicOperationReturnType

Declare an enum where each case wraps a typed database item:

```swift
@PolymorphicOperationReturnType
enum AccountItem {
    case customer(StandardTypedDatabaseItem<Customer>)
    case order(StandardTypedDatabaseItem<Order>)
}
```

The macro generates conformances to both `PolymorphicOperationReturnType` and `BatchCapableReturnType`, enabling the enum to be used with `polymorphicQuery` and `polymorphicGetItems`:

```swift
let items: [AccountItem] = try await table.polymorphicQuery(
    forPartitionKey: "account-123",
    sortKeyCondition: nil
)
for item in items {
    switch item {
    case .customer(let customerItem):
        print(customerItem.rowValue.name)
    case .order(let orderItem):
        print(orderItem.rowValue.orderId)
    }
}
```

### polymorphicGetItems

Batch-get multiple items by key, with each item decoded into the correct enum case:

```swift
let batch: [StandardCompositePrimaryKey: AccountItem] =
    try await table.polymorphicGetItems(forKeys: [key1, key2])
```

## Polymorphic Writes

### @PolymorphicWriteEntry

Declare an enum for polymorphic writes where each case wraps a `WriteEntry`:

```swift
typealias CustomerWriteEntry = StandardWriteEntry<Customer>
typealias OrderWriteEntry = StandardWriteEntry<Order>

@PolymorphicWriteEntry
enum AccountWriteEntry {
    case customer(CustomerWriteEntry)
    case order(OrderWriteEntry)
}

let entries: [AccountWriteEntry] = [
    .customer(.insert(new: customerItem)),
    .order(.insert(new: orderItem)),
]
try await table.polymorphicTransactWrite(entries)
```

`polymorphicBulkWrite` works the same way:

```swift
try await table.polymorphicBulkWrite(entries)
```

### @PolymorphicTransactionConstraintEntry

Declare an enum for polymorphic transaction constraints:

```swift
typealias CustomerConstraintEntry = StandardTransactionConstraintEntry<Customer>

@PolymorphicTransactionConstraintEntry
enum AccountConstraintEntry {
    case customer(CustomerConstraintEntry)
}

try await table.polymorphicTransactWrite(
    entries,
    constraints: [.customer(.required(existing: guardItem))]
)
```
