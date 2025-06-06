<p align="center">
<a href="https://github.com/swift-server-community/dynamo-db-tables/actions">
<img src="https://github.com/swift-server-community/dynamo-db-tables/actions/workflows/swift.yml/badge.svg?branch=main" alt="Build - Main Branch">
</a>
<a href="http://swift.org">
<img src="https://img.shields.io/badge/swift-6.1|6.0-orange.svg?style=flat" alt="Swift 6.1 and 6.0 Compatible and Tested">
</a>
<img src="https://img.shields.io/badge/ubuntu-22.04|24.04-yellow.svg?style=flat" alt="Ubuntu 22.04 and 24.04 Tested">
<img src="https://img.shields.io/badge/license-Apache2-blue.svg?style=flat" alt="Apache 2">
</p>

DynamoDBTables is a library to make it easy to use DynamoDB from Swift-based applications, with a particular focus on usage with polymorphic database tables (tables that don't have a single schema for all rows).

DynamoDBTables is a fork of https://github.com/amzn/smoke-dynamodb and acknowledges the authors of that original package.

# Getting Started

## Step 1: Add the DynamoDBTables dependency

DynamoDBTables uses the Swift Package Manager. To use the framework, add the following dependency
to your Package.swift-
 
```swift
dependencies: [
    .package(url: "https://github.com/swift-server-community/dynamo-db-tables", from: "0.1.0")
]

.target(
    name: ...,
    dependencies: [..., "DynamoDBTables"]),
```

# Basic Usage

## Naming Schema

For consistency in naming across the library, DynamoDBTables will case DynamoDB to what is observed and standardized in AWS's documentation of DynamoDB:

- Uppercase: `DynamoDB`
  - Use-cases: Class names, struct names, upper-cased while in the middle of a camel cased function/variable name, and strings referring to it as a proper noun.
  - Examples:
    - `DynamoDBCompositePrimaryKeyTable`
    - `dropAsDynamoDBKeyPrefix`

- Lowercase: `dynamodb`
  - Use-cases: When used as a prefix to a function/variable name that is lower-cased or camel-cased.
  - Example:
    - `dynamodbKeyWithPrefixedVersion`

## Performing operations on a DynamoDB Table

This package enables operations to be performed on a DynamoDB table using a type that conforms to the `DynamoDBCompositePrimaryKeyTable` protocol. In a production scenario, operations can be performed using `AWSDynamoDBCompositePrimaryKeyTable`.

For basic use cases, you can initialise a table with the tableName, credentialsProvider and awsRegion.

```swift 
let table = AWSDynamoDBCompositePrimaryKeyTable(tableName: tableName,
                                                client: existingDynamoDBClient)
                
// perform actions on the table
```

The initialisers of this table type can also accept optional parameters such as the `Logger` to use. There is also an 
additional initializer that will create a client internally from a set of common parameters.

## Testing

### In Memory mocking

The `InMemory*` types - such as `InMemoryDynamoDBCompositePrimaryKeyTable` - provide the ability to perform basic validation of table operations by using an in-memory dictionary to simulate the behaviour of a DynamoDb table. More advanced behaviours such as indexes are not simulated with these types.

The `SimulateConcurrency*` types provide a wrapper around another table and simulates additional writes to that table in-between accesses. These types are designed to allow unit testing of table concurrency handling.

### DynamoDB Local

With the downloadable version of Amazon DynamoDB, you can develop and test applications without accessing the DynamoDB web service. This version can be used when the full functionality of DynamoDB is needed for local testing.

The instructions to set up  DynamoDB Local is [here](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html).

// TODO: Add Example Code


## Insertion

An item can be inserted into the DynamoDB table using the following-

```swift
struct PayloadType: Codable, Equatable {
    let firstly: String
    let secondly: String
}

let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                      sortKey: "sortId")
let payload = PayloadType(firstly: "firstly", secondly: "secondly")
let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
try await table.insertItem(databaseItem)
```

The `insertItem` operation will attempt to create the following row in the DynamoDB table-
* **PK**: "partitionId" (table partition key)
* **SK**: "sortId" (table sort key)
* **CreateDate**: <the current date>
* **RowType**: "PayloadType"
* **RowVersion**: 1
* **LastUpdatedDate**: <the current date>
* **firstly**: "firstly"
* **secondly**: "secondly"

By default, this operation will fail if an item with the same partition key and sort key already exists.

**Note:** The `StandardCompositePrimaryKey` will place the partition key in the attribute called *PK* and the sort key in an attribute called *SK*. Custom partition and sort key attribute names can be used by dropping down to the underlying `CompositePrimaryKey` type and the `PrimaryKeyAttributes` protocol.

## Retrieval 

An item can be retrieved from the DynamoDB table using the following-

```swift
let retrievedItem: StandardTypedDatabaseItem<PayloadType>? = try await table.getItem(forKey: key)
```

The `getItem` operation return an optional `TypedTTLDatabaseItem` which will be nil if the item doesn't exist in the table. These operations will also fail if the *RowType* recorded in the database row doesn't match the type being requested.

## Update

An item can be updated in the DynamoDB table using the following-

```swift
let updatedPayload = PayloadType(firstly: "firstlyX2", secondly: "secondlyX2")
let updatedDatabaseItem = retrievedItem.createUpdatedItem(withValue: updatedPayload)
try await table.updateItem(newItem: updatedDatabaseItem, existingItem: retrievedItem)
```

The `updateItem` (or `updateItem`) operation will attempt to insert the following row in the DynamoDB table-
* **PK**: "partitionId" (table partition key)
* **SK**: "sortId" (table sort key)
* **CreateDate**: <the original date when the row was created>
* **RowType**: "PayloadType"
* **RowVersion**: 2
* **LastUpdatedDate**: <the current date>
* **firstly**: "firstlyX2"
* **secondly**: "secondlyX2"

By default, this operation will fail if an item with the same partition key and sort key doesn't exist in the table and if the existing row doesn't have the same version number as the `existingItem` submitted in the operation. The `DynamoDBCompositePrimaryKeyTable` protocol also provides the `clobberItem` operation which will overwrite a row in the database regardless of the existing row.

## Conditionally Update

The `conditionallyUpdateItem` operation will attempt to update the primary item, repeatedly calling the `updatedPayloadProvider` to retrieve an updated version of the current row value until the  `update` operation succeeds. The `updatedPayloadProvider` can throw an exception to indicate that the current row value is unable to be updated.

```swift
try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: updatedPayloadProvider)
```

The `conditionallyUpdateItem` operation could also be provided with `updatedItemProvider`. It will attempt to update the primary item, repeatedly calling the `updatedItemProvider` to retrieve an updated version of the current row until the  `update` operation succeeds. The `updatedItemProvider` can throw an exception to indicate that the current row is unable to be updated.

```swift
try await table.conditionallyUpdateItem(forKey: key, updatedItemProvider: updatedItemProvider)
```

## Delete

An item can be deleted in the DynamoDB table using the following-

```swift
try await table.deleteItem(forKey: key)
```

The `deleteItem` operation will succeed even if the specified row doesn't exist in the database table.

## Queries and Batch

All or a subset of the rows from a partition can be retrieved using a query.

```swift
let (queryItems, nextPageToken): ([StandardTypedDatabaseItem<TestTypeA>], String?) =
    try await table.query(forPartitionKey: "partitionId",
                          sortKeyCondition: nil,
                          limit: 100,
                          exclusiveStartKey: exclusiveStartKey)
                                 
for databaseItem in queryItems {                         
    ...
}
```

1. The sort key condition can restrict the query to a subset of the partition rows. A nil condition will return all rows in the partition. 
2. The `query` operation will fail if any of the rows being returned are not of type `TestTypeA`.
3. The optional String returned by the `query` operation can be used as the `exclusiveStartKey` in another request to retrieve the next "page" of results from DynamoDB.
4. There is an overload of the `query` operation that doesn't accept a `limit` or `exclusiveStartKey`. This overload will internally handle the API pagination, making multiple calls to DynamoDB if necessary.

There is also an equivalent `getItems` call that uses DynamoDB's BatchGetItem API-

```swift
let batch: [StandardCompositePrimaryKey: StandardTypedDatabaseItem<TestTypeA>]
    = try await table.getItems(forKeys: [key1, key2])
    
guard let retrievedDatabaseItem1 = batch[key1] else {
    ...
}
        
guard let retrievedDatabaseItem2 = batch[key2] else {
    ...
}
```

## Polymorphic Queries and Batch

In addition to the `query` operation, there is a more complex API that allows retrieval of multiple rows that have different types.

This API is most conveniently used in conjunction with an enum annotated with the `@PolymorphicOperationReturnType` macro-

```swift
@PolymorphicOperationReturnType
enum TestPolymorphicOperationReturnType {
    case typeA(StandardTypedDatabaseItem<TypeA>)
    case typeB(StandardTypedDatabaseItem<TypeB>)
}

let (queryItems, nextPageToken): ([TestPolymorphicOperationReturnType], String?) =
    try await table.polymorphicQuery(forPartitionKey: partitionId,
                                     sortKeyCondition: nil,
                                     limit: 100,
                                     exclusiveStartKey: exclusiveStartKey)
                                 
for item in queryItems {                         
    switch item {
    case .typeA(let databaseItem):
        ...
    case .typeB(let databaseItem):
    }
}
```

1. The `polymorphicQuery` operation will fail if any of the rows being returned are not specified in the output `PolymorphicOperationReturnType` type.

A similar operation utilises DynamoDB's BatchGetItem API, returning items in a dictionary keyed by the provided `CompositePrimaryKey` instance-

```swift
let batch: [StandardCompositePrimaryKey: TestPolymorphicOperationReturnType] = try await table.polymorphicGetItems(forKeys: [key1, key2])

guard case .testTypeA(let retrievedDatabaseItem1) = batch[key1] else {
    ...
}

guard case .testTypeB(let retrievedDatabaseItem2) = batch[key2] else {
    ...
}
```

This operation will automatically handle retrying unprocessed items (with exponential backoff) if the table doesn't have the capacity during the initial request.

## Queries on Indices

There are two mechanisms for querying on indices depending on if you have any projected attributes.

### Using Projected Attributes

If you are projecting all attributes or some attributes (for this option to work you **must** project at least the attributes managed directly by `smoke-dynamodb` which are `CreateDate`, `LastUpdatedDate`, `RowType` and `RowVersion`), you can use the `DynamoDBCompositePrimaryKeyTable` protocol and its conforming types as usual but with a custom `PrimaryKeyAttributes` type-

```swift
public struct GSI1PrimaryKeyAttributes: PrimaryKeyAttributes {
    public static var partitionKeyAttributeName: String {
        return "GSI-1-PK"
    }
    public static var sortKeyAttributeName: String {
        return "GSI-1-SK"
    }
    public static var indexName: String? {
        return "GSI-1"
    }
}

let (queryItems, nextPageToken): ([TypedDatabaseItem<GSI1PrimaryKeyAttributes, TestTypeA>], String?) =
    try await table.query(forPartitionKey: "partitionId",
                          sortKeyCondition: nil,
                          limit: 100,
                          exclusiveStartKey: exclusiveStartKey)
                                 
for databaseItem in queryItems {                         
    ...
}
```

and similarly for polymorphic queries, most conveniently used in conjunction with an enum annotated with the `@PolymorphicOperationReturnType` macro-

```swift
@PolymorphicOperationReturnType
enum TestPolymorphicOperationReturnType {
    case typeA(StandardTypedDatabaseItem<TypeA>)
    case typeB(StandardTypedDatabaseItem<TypeB>)
}

let (queryItems, nextPageToken): ([TestPolymorphicOperationReturnType], String?) =
    try await table.polymorphicQuery(forPartitionKey: partitionId,
                          sortKeyCondition: nil,
                          limit: 100,
                          exclusiveStartKey: exclusiveStartKey)
                                 
for item in queryItems {                         
    switch item {
    case .typeA(let databaseItem):
        ...
    case .typeB(let databaseItem):
    }
}
```

### Using No Projected Attributes

To simply query a partition on an index that has no projected attributes, you can use the `DynamoDBCompositePrimaryKeysProjection` protocol and conforming types like ` AWSDynamoDBCompositePrimaryKeysProjection`. This type is created using a generator class in the same way as the primary table type-

```swift
let generator = AWSDynamoDBCompositePrimaryKeysProjectionGenerator(
    credentialsProvider: credentialsProvider, region: region,
    endpointHostName: dynamodbEndpointHostName, tableName: dynamodbTableName)

let projection = generator.with(logger: logger)
```

The list of keys in a partition can then be retrieved using the functions provided by this protocol-

```swift
let (queryItems, nextPageToken): ([CompositePrimaryKey<GSI1PrimaryKeyAttributes>], String?) =
    try await projection.query(
        forPartitionKey: "partitionId",
        sortKeyCondition: nil,
        limit: 100,
        exclusiveStartKey: exclusiveStartKey)
                                 
for primaryKey in queryItems {                         
    ...
}
```

## Writes and Batch or Transactions

You can write multiple database rows using either a bulk or [transaction](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html) write-

```swift
typealias TestTypeAWriteEntry = StandardWriteEntry<TestTypeA>

let entryList: [TestTypeAWriteEntry] = [
    .insert(new: databaseItem1),
    .insert(new: databaseItem2)
]
        
try await table.bulkWrite(entryList)
```

Or alternatively executed within a DynamoDB transaction-

```swift
try await table.transactWrite(entryList)
```

and similarly for polymorphic queries, most conveniently by using the `@PolymorphicWriteEntry` macro-

```swift
typealias TestTypeBWriteEntry = StandardWriteEntry<TestTypeB>

@PolymorphicWriteEntry
enum TestPolymorphicWriteEntry {
    case testTypeA(TestTypeAWriteEntry)
    case testTypeB(TestTypeBWriteEntry)
}

let entryList: [TestPolymorphicWriteEntry] = [
    .testTypeA(.insert(new: databaseItem1)),
    .testTypeB(.insert(new: databaseItem3))
]
        
try await table.polymorphicBulkWrite(entryList)
```

Or alternatively executed within a DynamoDB transaction-

```swift
try await table.polymorphicTransactWrite(entryList)
```

For transactions, you can additionally specify a set of constraints to be part of the transaction-

```swift
let constraintList: [StandardTransactionConstraintEntry<TestTypeA>] = [
    .required(existing: databaseItem3),
    .required(existing: databaseItem4),
]

try await table.transactWrite(entryList, constraints: constraintList)
```

and similarly for polymorphic queries, most conveniently by using the `@PolymorphicTransactionConstraintEntry` macro-

```swift
typealias TestTypeAStandardTransactionConstraintEntry = StandardTransactionConstraintEntry<TestTypeA>
typealias TestTypeBStandardTransactionConstraintEntry = StandardTransactionConstraintEntry<TestTypeB>

@PolymorphicTransactionConstraintEntry
enum TestPolymorphicTransactionConstraintEntry {
    case testTypeA(TestTypeAStandardTransactionConstraintEntry)
    case testTypeB(TestTypeBStandardTransactionConstraintEntry)
}

let constraintList: [TestPolymorphicTransactionConstraintEntry] = [
    .testTypeA(.required(existing: databaseItem3)),
    .testTypeB(.required(existing: databaseItem4))
    ]
         
try await table.polymorphicTransactWrite(entryList, constraints: constraintList)
```

## Recording updates in a historical partition

This package contains a number of convenience functions for storing versions of a row in a historical partition

### Insertion

The `insertItemWithHistoricalRow` operation provide a single call to insert both a primary and historical item-

```swift
try await table.insertItemWithHistoricalRow(primaryItem: databaseItem, historicalItem: historicalItem)
```

### Update

The `updateItemWithHistoricalRow` operation provide a single call to update a primary item and insert a historical item-

```swift
try await table.updateItemWithHistoricalRow(primaryItem: updatedItem, 
                                            existingItem: databaseItem, 
                                            historicalItem: historicalItem)
```

### Clobber

The `clobberItemWithHistoricalRow` operation will attempt to insert or update the primary item, repeatedly calling the `primaryItemProvider` to retrieve an updated version of the current row (if it exists) until the appropriate `insert` or  `update` operation succeeds. The `historicalItemProvider` is called to provide the historical item based on the primary item that was inserted into the database table. The primary item may not exist in the database table to begin with.

```swift
try await table.clobberItemWithHistoricalRow(primaryItemProvider: primaryItemProvider,
                                             historicalItemProvider: historicalItemProvider)
```

The `clobberItemWithHistoricalRow` operation is typically used when it is unknown if the primary item already exists in the database table and you want to either insert it or write a new version of that row (which may or may not be based on the existing item).

This operation can fail with a concurrency error if the `insert` or  `update` operation repeatedly fails (the default is after 10 attempts).

### Conditionally Update

The `conditionallyUpdateItemWithHistoricalRow` operation will attempt to update the primary item, repeatedly calling the `primaryItemProvider` to retrieve an updated version of the current row until the  `update` operation succeeds. The `primaryItemProvider` can thrown an exception to indicate that the current row is unable to be updated. The `historicalItemProvider` is called to provide the historical item based on the primary item that was inserted into the database table.

```swift
try await table.conditionallyUpdateItemWithHistoricalRow(
    forPrimaryKey: dKey,
    primaryItemProvider: conditionalUpdatePrimaryItemProvider,
    historicalItemProvider: conditionalUpdateHistoricalItemProvider)
```

The `conditionallyUpdateItemWithHistoricalRow` operation is typically used when it is known that the primary item exists and you want to test if you can update it based on some attribute of its current version. A common scenario is adding a subordinate related item to the primary item where there is a limit of the number of related items. Here you would want to test the current version of the primary item to ensure the number of related items isn't exceeded.

This operation can fail with a concurrency error if the  `update` operation repeatedly fails (the default is after 10 attempts).

**Note:** The `clobberItemWithHistoricalRow` operation is similar in nature but have slightly different use cases. The `clobber` operation is typically used to create or update the primary item. The `conditionallyUpdate` operation is typically used when creating a subordinate related item that requires checking if the primary item can be updated.

## Managing versioned rows

The `clobberVersionedItemWithHistoricalRow` operation provide a mechanism for managing mutable database rows and storing all previous versions of that row in a historical partition. This operation stores the primary item under a "version zero" sort key with a payload that replicates the current version of the row. This historical partition contains rows for each version, including the current version under a sort key for that version.

```swift
let payload1 = PayloadType(firstly: "firstly", secondly: "secondly")
let partitionKey = "partitionId"
let historicalPartitionPrefix = "historical"
let historicalPartitionKey = "\(historicalPartitionPrefix).\(partitionKey)"
                
func generateSortKey(withVersion version: Int) -> String {
    let prefix = String(format: "v%05d", version)
    return [prefix, "sortId"].dynamodbKey
}
    
try await table.clobberVersionedItemWithHistoricalRow(forPrimaryKey: partitionKey,
                                                      andHistoricalKey: historicalPartitionKey,
                                                      item: payload1,
                                                      primaryKeyType: StandardPrimaryKeyAttributes.self,
                                                      generateSortKey: generateSortKey)
                                                             
// the v0 row, copy of version 1
let key1 = StandardCompositePrimaryKey(partitionKey: partitionKey, sortKey: generateSortKey(withVersion: 0))
let item1: StandardTypedDatabaseItem<RowWithItemVersion<PayloadType>> = try await table.getItem(forKey: key1)
item1.rowValue.itemVersion // 1
item1.rowStatus.rowVersion // 1
item1.rowValue.rowValue // payload1
        
// the v1 row, has version 1
let key2 = StandardCompositePrimaryKey(partitionKey: historicalPartitionKey, sortKey: generateSortKey(withVersion: 1))
let item2: StandardTypedDatabaseItem<RowWithItemVersion<PayloadType>> = try await table.getItem(forKey: key2)
item1.rowValue.itemVersion // 1
item1.rowStatus.rowVersion // 1
item1.rowValue.rowValue // payload1
        
let payload2 = PayloadType(firstly: "thirdly", secondly: "fourthly")
        
try await table.clobberVersionedItemWithHistoricalRow(forPrimaryKey: partitionKey,
                                                      andHistoricalKey: historicalPartitionKey,
                                                      item: payload2,
                                                      primaryKeyType: StandardPrimaryKeyAttributes.self,
                                                      generateSortKey: generateSortKey)
        
// the v0 row, copy of version 2
let key3 = StandardCompositePrimaryKey(partitionKey: partitionKey, sortKey: generateSortKey(withVersion: 0))
let item3: StandardTypedDatabaseItem<RowWithItemVersion<PayloadType>> = try await table.getItem(forKey: key3)
item1.rowValue.itemVersion // 2
item1.rowStatus.rowVersion // 2
item1.rowValue.rowValue // payload2
        
// the v1 row, still has version 1
let key4 = StandardCompositePrimaryKey(partitionKey: historicalPartitionKey, sortKey: generateSortKey(withVersion: 1))
let item4: StandardTypedDatabaseItem<RowWithItemVersion<PayloadType>> = try await table.getItem(forKey: key4)
item1.rowValue.itemVersion // 1
item1.rowStatus.rowVersion // 1
item1.rowValue.rowValue // payload1
        
// the v2 row, has version 2
let key5 = StandardCompositePrimaryKey(partitionKey: historicalPartitionKey, sortKey: generateSortKey(withVersion: 2))
let item5: StandardTypedDatabaseItem<RowWithItemVersion<PayloadType>> = try await table.getItem(forKey: key5)
item1.rowValue.itemVersion // 2
item1.rowStatus.rowVersion // 1
item1.rowValue.rowValue // payload2
```

This provides a localized synchronization mechanism for updating mutable rows in a database table where the lock is tracked as the rowVersion of the primary item. This allows versioned mutable rows to updated safely and updates to different primary items do not contend for a table-wide lock.

## Time To Live (TTL)

DynamoDB Time To Live feature is supported by adding a per-item timestamp attribute to the item. Shortly after the date and time of the specified timestamp, DynamoDB deletes the item from your table. For more details and instructions to enable TTL on your table, check [here](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html).

### Insertion

An item with TTL can be inserted into the DynamoDB table using the following-

```swift
struct PayloadType: Codable, Equatable {
    let firstly: String
    let secondly: String
}

let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                      sortKey: "sortId")
let payload = PayloadType(firstly: "firstly", secondly: "secondly")
let timeToLive = StandardTimeToLive(timeToLiveTimestamp: 123456789)
let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload, andTimeToLive: timeToLive)
        
try await table.insertItem(databaseItem)
```
The `insertItem` operation will attempt to create the following row in the DynamoDB table-
* **PK**: "partitionId" (table partition key)
* **SK**: "sortId" (table sort key)
* **CreateDate**: <the current date>
* **RowType**: "PayloadType"
* **RowVersion**: 1
* **LastUpdatedDate**: <the current date>
* **firstly**: "firstly"
* **secondly**: "secondly"
* **ExpireDate**: 123456789

By default, this operation will fail if an item with the same partition key and sort key already exists.

**Note:**

The `StandardCompositePrimaryKey` will place the partition key in the attribute called *PK* and the sort key in an attribute called *SK*. Custom partition and sort key attribute names can be used by dropping down to the underlying `CompositePrimaryKey` type and the `PrimaryKeyAttributes` protocol.

The `StandardTimeToLive` will place the TTL timestamp in the attribute called *ExpireDate*. Custom TTL timestamp attribute name can be used by dropping down to the underlying `TimeToLive` type and `TimeToLiveAttributes` protocol.

### Update

An item with TTL can be updated in the DynamoDB table using the following-

```swift
let updatedPayload = PayloadType(firstly: "firstlyX2", secondly: "secondlyX2")
let updatedTimeToLive = StandardTimeToLive(timeToLiveTimestamp: 234567890)
let updatedDatabaseItem = retrievedItem.createUpdatedItem(withValue: updatedPayload, andTimeToLive: updatedTimeToLive)
try await table.updateItem(newItem: updatedDatabaseItem, existingItem: retrievedItem)
```

The `updateItem` (or `updateItem`) operation will attempt to insert the following row in the DynamoDB table-
* **PK**: "partitionId" (table partition key)
* **SK**: "sortId" (table sort key)
* **CreateDate**: <the original date when the row was created>
* **RowType**: "PayloadType"
* **RowVersion**: 2
* **LastUpdatedDate**: <the current date>
* **firstly**: "firstlyX2"
* **secondly**: "secondlyX2"
* **ExpireDate**: 234567890

By default, this operation will fail if an item with the same partition key and sort key doesn't exist in the table and if the existing row doesn't have the same version number as the `existingItem` submitted in the operation. The `DynamoDBCompositePrimaryKeyTable` protocol also provides the `clobberItem` operation which will overwrite a row in the database regardless of the existing row.

# Entities

The main entities provided by this package are
* *CompositePrimaryKey*: a struct that stores the partition and sort values for a composite primary key.
* *TimeToLive*: a struct that stores TTL timestamp for a database item.
* *TypedTTLDatabaseItem*: a struct that manages decoding and encoding rows of a particular type along with any TTL settings from polymorphic database tables.
* *TypedDatabaseItem*: a typealias that points to `TypedDatabaseItemWithTimeToLive` with default `StandardTimeToLiveAttributes` generic type for backwards compatibility.
* *PolymorphicDatabaseItem*: a struct that manages decoding rows that are one out of a number of types from polymorphic database tables.
* *DynamoDBCompositePrimaryKeyTable*: a protocol for interacting with a DynamoDB database table.
* *`InMemoryDynamoDBCompositePrimaryKeyTable`*: a struct conforming to the `DynamoDBCompositePrimaryKeyTable` protocol that interacts with a local in-memory table.
* *AWSDynamoDBCompositePrimaryKeyTable*: a struct conforming to the `DynamoDBCompositePrimaryKeyTable` protocol that interacts with the AWS DynamoDB service.

## CompositePrimaryKey

The CompositePrimaryKey struct defines the partition and sort key values for a row in the database. It is also used to serialize and deserialize these values. For convenience, this package provides a typealias called `StandardCompositePrimaryKey` that uses a partition key with an attribute name of *PK* and a sort key with an attribute name of *SK*. This struct can be instantiated as shown-

```swift
let key = StandardCompositePrimaryKey(partitionKey: "partitionKeyValue",
                                      sortKey: "sortKeyValue")
```

## TimeToLive

The TimeToLive struct defines the TTL timestamp value for a row in the database. It is also used to serialize and deserialize this value. For convenience, this package provides a typealias called `StandardTimeToLive` that uses the TTL timestamp with an attribute name of *ExpireDate*. This struct can be instantiated as shown-

```swift
let timeToLive = StandardTimeToLive(timeToLiveTimestamp: 123456789)
```

## TypedTTLDatabaseItem

The TypedTTLDatabaseItem struct manages a number of attributes in the database table to enable decoding and encoding rows to and from the correct type. In addition it also manages other conveniences such as versioning. The attributes this struct will add to a database row are-
* *CreateDate*: The timestamp when the row was created.
* *RowType*: Specifies the schema used by the other attributes of this row.
* *RowVersion*: A version number for the values currently in this row. Used to enable optimistic locking.
* *LastUpdatedDate*: The timestamp when the row was last updated.

Similar to CompositePrimaryKey, this package provides a typealias called `TypedDatabaseItem` that expects the standard partition, sort key, and TTL attribute names.

This struct can be instantiated as shown-

```swift
let newDatabaseItem = StandardTypedDatabaseItem.newItem(withKey: compositePrimaryKey, andValue: rowValueType)
```

or with TTL-

```swift
let newDatabaseItem = StandardTypedDatabaseItem.newItem(withKey: compositePrimaryKey, andValue: rowValueType, andTimeToLive: timeToLive)
```

Here *compositePrimaryKey* must be of type `CompositePrimaryKey`, *rowValueType* must conform to the [Codable protocol](https://developer.apple.com/documentation/foundation/archives_and_serialization/encoding_and_decoding_custom_types), and *timeToLive* must be of type `TimeToLive`. By default, performing a **PutItem** operation with this item on a table where this row already exists will fail.

The *createUpdatedItem* function on this struct can be used to create an updated version of this row-

```swift
let updatedDatabaseItem = newDatabaseItem.createUpdatedItem(withValue: updatedValue)
```

or with TTL-

```swift
let updatedDatabaseItem = newDatabaseItem.createUpdatedItem(withValue: updatedValue, andTimeToLive: updatedTimeToLive)
```

This function will create a new instance of TypedTTLDatabaseItem with the same key and updated LastUpdatedDate and RowVersion values. By default, performing a **PutItem** operation with this item on a table where this row already exists and the RowVersion isn't equal to the value of the original row will fail.

## DynamoDBCompositePrimaryKeyTable

The `DynamoDBCompositePrimaryKeyTable` protocol provides a number of functions for interacting with the DynamoDB tables. Typically the `AWSDynamoDBCompositePrimaryKeyTable` implementation of this protocol is instantiated using a `CredentialProvider` (such as one from the `smoke-aws-credentials` module to automatically handle rotating credentials), the service region and endpoint and the table name to use.

```swift
let generator = AWSDynamoDBCompositePrimaryKeyTableGenerator(
    credentialsProvider: credentialsProvider, region: region,
    endpointHostName: dynamodbEndpointHostName, tableName: dynamodbTableName)
   
let table = generator.with(logger: logger)
```

Internally `AWSDynamoDBCompositePrimaryKeyTable` uses a custom Decoder and Encoder to serialize types that conform to `Codable` to and from the JSON schema required by the DynamoDB service. These Decoder and Encoder implementation automatically capitalize attribute names.

# Customization

## PrimaryKeyAttributes

`CompositePrimaryKey`, `TypedTTLDatabaseItem` and `PolymorphicDatabaseItem` are all generic to a type conforming to the `PrimaryKeyAttributes` protocol. This protocol can be used to use custom attribute names for the partition and sort keys.

```swift
public struct MyPrimaryKeyAttributes: PrimaryKeyAttributes {
    public static var partitionKeyAttributeName: String {
        return "MyPartitionAttributeName"
    }
    public static var sortKeyAttributeName: String {
        return "MySortKeyAttributeName"
    }
}
```

## TimeToLiveAttributes

`TimeToLive`, `TypedTTLDatabaseItem` and `PolymorphicDatabaseItem` are all generic to a type conforming to the `TimeToLiveAttributes` protocol. This protocol can be used to use custom attribute names for the TTL timestamp.

```swift
public struct MyTimeToLiveAttributes: TimeToLiveAttributes {
    public static var timeToLiveAttributeName: String {
        return "MyTimeToLiveAttributeName"
    }
}
```

## CustomRowTypeIdentifier

If the `Codable` type is used for a row type also conforms to the `CustomRowTypeIdentifier`, the *rowTypeIdentifier* property of this type will be used as the RowType recorded in the database row.

```swift
struct TypeB: SCodable, CustomRowTypeIdentifier {
    static let rowTypeIdentifier: String? = "TypeBCustom"
    
    let thirdly: String
    let fourthly: String
}
```

## RowWithIndex

RowWithIndex is a helper struct that provides an index (such as a GSI) attribute as part of the type of a database row.


## RowWithItemVersion

RowWithItemVersion is a helper struct that provides an "ItemVersion" to be used in conjunction with the historical item extensions.

## License
This library is licensed under the Apache 2.0 License.
