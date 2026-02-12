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

The macro generates the conformance to `PolymorphicOperationReturnType`, enabling the enum to be used with `polymorphicQuery`:

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

### BatchCapableReturnType

To use `polymorphicGetItems`, the enum must also conform to `BatchCapableReturnType`:

```swift
extension AccountItem: BatchCapableReturnType {
    func getItemKey() -> StandardCompositePrimaryKey {
        switch self {
        case .customer(let item): item.compositePrimaryKey
        case .order(let item): item.compositePrimaryKey
        }
    }
}

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
