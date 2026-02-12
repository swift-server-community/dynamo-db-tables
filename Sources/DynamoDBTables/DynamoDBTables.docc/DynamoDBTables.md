# ``DynamoDBTables``

A type-safe, Sendable-first DynamoDB layer for Swift with optimistic concurrency.

## Overview

DynamoDBTables is a Swift library for working with Amazon DynamoDB. It sits on top of the AWS SDK and provides a type-safe, `Sendable`-first API for storing, retrieving, and querying items — including tables where different rows have different schemas.

The raw AWS SDK for DynamoDB operates on untyped attribute-value dictionaries — every read requires manual deserialization, every write requires constructing low-level attribute maps, and there's no compile-time guarantee that the data you read matches the schema you expect. DynamoDBTables replaces all of that with a strongly typed Swift layer where your `Codable` models are serialized and deserialized automatically, and the compiler catches schema mismatches before your code ever runs.

Beyond type safety, DynamoDBTables handles concerns that would otherwise require significant boilerplate on top of the raw SDK:

- **Optimistic concurrency** is built in — every item tracks a row version, and updates automatically include a condition check so concurrent writers don't silently overwrite each other.
- **Single-table design** is a first-class concept. AWS [recommends](https://aws.amazon.com/blogs/compute/creating-a-single-table-design-with-amazon-dynamodb/) storing different entity types in the same table, but the raw SDK gives you back raw dictionaries with no indication of which schema applies. DynamoDBTables decodes each row into the correct Swift type based on a stored type discriminator.
- **Retrying operations** handle the common pattern of read-modify-write under contention — automatically re-reading, re-applying your transform, and retrying on version conflicts.
- **Transactions and batch writes** are expressed as arrays of typed entries rather than low-level TransactWriteItem dictionaries.
- **Historical audit trails** can be maintained atomically alongside mutable state with a single method call.
- **In-memory test doubles** conform to the same protocol as the production client, so unit tests run without any DynamoDB connection and without mocking.

DynamoDBTables is a fork of [smoke-dynamodb](https://github.com/amzn/smoke-dynamodb) and acknowledges the authors of that original package.

## Topics

### Essentials

- <doc:GettingStarted>
- <doc:CRUDOperations>

### Data Modeling

- <doc:CompositePrimaryKeys>
- <doc:DatabaseItems>

### Operations

- <doc:QueriesAndBatchOperations>
- <doc:WritesAndTransactions>
- <doc:PolymorphicOperations>

### Advanced Patterns

- <doc:RetryingOperations>
- <doc:HistoricalRows>
- <doc:ErrorHandling>

### Testing

- <doc:Testing>
