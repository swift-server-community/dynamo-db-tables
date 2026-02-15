//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/DynamoDBCompositePrimaryKeyTable.swift
// Copyright 2018-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// Licensed under Apache License v2.0
//
// Changes specified by
// https://github.com/swift-server-community/dynamo-db-tables/compare/9ab0e7a..main
// Copyright (c) 2024 the DynamoDBTables authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of DynamoDBTables authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

//
//  DynamoDBCompositePrimaryKeyTable.swift
//  DynamoDBTables
//

/// Enumeration of the errors that can be thrown by a DynamoDBTable.
public enum DynamoDBTableError: Error {
    case accessDenied(message: String?)
    case internalServerError(message: String?)
    case requestLimitExceeded(message: String?)
    case databaseError(reason: String)
    case unexpectedError(cause: Swift.Error)
    case unexpectedResponse(reason: String)
    case conditionalCheckFailed(partitionKey: String, sortKey: String, message: String?)
    case duplicateItem(partitionKey: String, sortKey: String, message: String?)
    case resourceNotFound(partitionKey: String, sortKey: String, message: String?)
    case typeMismatch(expected: String, provided: String)
    case unexpectedType(provided: String)
    case concurrencyError(partitionKey: String, sortKey: String, message: String?)
    case unableToUpdateError(reason: String)
    case unrecognizedError(String, String?)
    case multipleUnexpectedErrors(cause: [Swift.Error])
    case batchAPIExceededRetries(retryCount: Int)
    case batchFailures(errors: [DynamoDBTableError])
    case statementLengthExceeded(reason: String)
    case itemCollectionSizeLimitExceeded(attemptedSize: Int, maximumSize: Int)
    case provisionedThroughputExceeded(message: String?)
    case throttling(message: String?)
    case validation(partitionKey: String?, sortKey: String?, message: String?)
    case unknown(code: String?, partitionKey: String?, sortKey: String?, message: String?)
    case transactionConflict(message: String?)
    case transactionCanceled(reasons: [DynamoDBTableError])
    case constraintFailure(reasons: [DynamoDBTableError])
}

public typealias DynamoDBTableErrorResult<SuccessPayload> = Result<SuccessPayload, DynamoDBTableError>

extension Swift.Error {
    public func asUnrecognizedDynamoDBTableError() -> DynamoDBTableError {
        let errorType = String(describing: type(of: self))
        let errorDescription = String(describing: self)
        return .unrecognizedError(errorType, errorDescription)
    }
}

/// Enumeration of the types of conditions that can be specified for an attribute.
public enum AttributeCondition: Sendable {
    case equals(String)
    case lessThan(String)
    case lessThanOrEqual(String)
    case greaterThan(String)
    case greaterThanOrEqual(String)
    case between(String, String)
    case beginsWith(String)
}

public enum WriteEntry<
    AttributesType: PrimaryKeyAttributes,
    ItemType: Codable & Sendable,
    TimeToLiveAttributesType: TimeToLiveAttributes
>: Sendable {
    case update(
        new: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        existing: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>
    )
    case insert(new: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>)
    case deleteAtKey(key: CompositePrimaryKey<AttributesType>)
    case deleteItem(existing: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>)

    public var compositePrimaryKey: CompositePrimaryKey<AttributesType> {
        switch self {
        case .update(let new, existing: _):
            new.compositePrimaryKey
        case let .insert(new: new):
            new.compositePrimaryKey
        case let .deleteAtKey(key: key):
            key
        case let .deleteItem(existing: existing):
            existing.compositePrimaryKey
        }
    }
}

public typealias StandardWriteEntry<ItemType: Codable> = WriteEntry<
    StandardPrimaryKeyAttributes, ItemType, StandardTimeToLiveAttributes
>

public protocol DynamoDBCompositePrimaryKeyTable {
    /**
     * Insert item is a non-destructive API. If an item already exists with the specified key this
     * API should fail.
     */
    func insertItem(_ item: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws

    /**
     * Clobber item is destructive API. Regardless of what is present in the database the provided
     * item will be inserted.
     */
    func clobberItem(_ item: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws

    /**
     * Update item requires having gotten an item from the database previously and will not update
     * if the item at the specified key is not the existing item provided.
     */
    func updateItem<AttributesType, ItemType, TimeToLiveAttributesType>(
        newItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        existingItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>
    ) async throws

    /**
     * Provides the ability to bulk write database rows in a transaction.
     * The transaction will comprise of the write entries specified in `entries`.
     * The transaction will fail if the number of entries is greater than 100.
     */
    func transactWrite(_ entries: [WriteEntry<some Any, some Any, some Any>]) async throws

    func polymorphicTransactWrite(
        _ entries: [some PolymorphicWriteEntry]
    ) async throws

    /**
     * Provides the ability to bulk write database rows in a transaction.
     * The transaction will comprise of the write entries specified in `entries`.
     * The transaction will be cancelled if the constraints specified in `constraints` are not met (for example you can specify that an item
     * with a specified version must exist regardless of if it will be written to by the transaction).
     * The transaction will fail if the number of entries and constraints combined is greater than 100.
     */
    func transactWrite<AttributesType, ItemType, TimeToLiveAttributesType>(
        _ entries: [WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>],
        constraints: [TransactionConstraintEntry<AttributesType, ItemType, TimeToLiveAttributesType>]
    ) async throws

    func polymorphicTransactWrite<
        WriteEntryType: PolymorphicWriteEntry,
        TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry
    >(
        _ entries: [WriteEntryType],
        constraints: [TransactionConstraintEntryType]
    ) async throws
    where WriteEntryType.AttributesType == TransactionConstraintEntryType.AttributesType

    /**
     * Provides the ability to bulk write database rows
     */
    func bulkWrite(_ entries: [WriteEntry<some Any, some Any, some Any>]) async throws

    func bulkWriteWithFallback(_ entries: [WriteEntry<some Any, some Any, some Any>]) async throws

    func polymorphicBulkWrite(_ entries: [some PolymorphicWriteEntry]) async throws

    /**
     * Retrieves an item from the database table. Returns nil if the item doesn't exist.
     */
    func getItem<AttributesType, ItemType, TimeToLiveAttributesType>(
        forKey key: CompositePrimaryKey<AttributesType>
    ) async throws
        -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>?

    /**
     * Retrieves items from the database table as a dictionary mapped to the provided key. Missing entries from the provided map indicate that item doesn't exist.
     */
    func polymorphicGetItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>]
    ) async throws
        -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]

    /**
     * Removes an item from the database table. Is an idempotent operation; running it multiple times
     * on the same item or attribute does not result in an error response.
     */
    func deleteItem(forKey key: CompositePrimaryKey<some Any>) async throws

    /**
     * Removes an item from the database table. Is an idempotent operation; running it multiple times
     * on the same item or attribute does not result in an error response. This operation will not modify the table
     * if the item at the specified key is not the existing item provided.
     */
    func deleteItem(existingItem: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws

    /**
     * Removes items from the database table. Is an idempotent operation; running it multiple times
     * on the same item or attribute does not result in an error response.
     */
    func deleteItems(forKeys keys: [CompositePrimaryKey<some Any>]) async throws

    /**
     * Removes items from the database table. Is an idempotent operation; running it multiple times
     * on the same item or attribute does not result in an error response. This operation will not modify the table
     * if the item at the specified key is not the existing item provided.
     */
    func deleteItems(existingItems: [TypedTTLDatabaseItem<some Any, some Any, some Any>]) async throws

    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will potentially make multiple calls to DynamoDB to retrieve all results for
       the query.
     */
    func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?
    ) async throws
        -> [ReturnedType]

    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will return paginated results based on the limit and exclusiveStartKey provided.
     */
    func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        exclusiveStartKey: String?
    ) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)

    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will return paginated results based on the limit and exclusiveStartKey provided.
     */
    func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        scanIndexForward: Bool,
        exclusiveStartKey: String?
    ) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)

    /**
     * Uses the ExecuteStatement API to perform batch reads or writes on data stored in DynamoDB, using PartiQL.
     * ExecuteStatement API has a maximum limit on the number of decomposed read operations per request. This function handles pagination internally.
     * This function will potentially make multiple calls to DynamoDB to retrieve all results.
     *
     * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ExecuteStatement.html
     */
    func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?
    ) async throws -> [ReturnedType]

    /**
     * Uses the ExecuteStatement API to to perform batch reads or writes on data stored in DynamoDB, using PartiQL.
     * ExecuteStatement API has a maximum limit on the number of decomposed read operations per request.
     * Caller of this function needs to handle pagination on their side.
     *
     * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ExecuteStatement.html
     */
    func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?,
        nextToken: String?
    ) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)

    // MARK: Monomorphic batch and queries

    /**
     * Retrieves items from the database table as a dictionary mapped to the provided key. Missing entries from the provided map indicate that item doesn't exist.
     */
    func getItems<AttributesType, ItemType, TimeToLiveAttributesType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>]
    ) async throws
        -> [CompositePrimaryKey<AttributesType>: TypedTTLDatabaseItem<
            AttributesType, ItemType, TimeToLiveAttributesType
        >]

    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will potentially make multiple calls to DynamoDB to retrieve all results for
       the query.
     */
    func query<AttributesType, ItemType, TimeToLiveAttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?
    ) async throws
        -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]

    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will return paginated results based on the limit and exclusiveStartKey provided.
     */
    func query<AttributesType, ItemType, TimeToLiveAttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        scanIndexForward: Bool,
        exclusiveStartKey: String?
    ) async throws
        -> (
            items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>], lastEvaluatedKey: String?
        )

    /**
     * Uses the ExecuteStatement API to to perform batch reads or writes on data stored in DynamoDB, using PartiQL.
     * ExecuteStatement API has a maximum limit on the number of decomposed read operations per request. This function handles pagination internally.
     * This function will potentially make multiple calls to DynamoDB to retrieve all results.
     *
     * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ExecuteStatement.html
     */
    func execute<AttributesType, ItemType, TimeToLiveAttributesType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?
    ) async throws -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]

    /**
     * Uses the ExecuteStatement API to to perform batch reads or writes on data stored in DynamoDB, using PartiQL.
     * ExecuteStatement API has a maximum limit on the number of decomposed read operations per request.
     * Caller of this function needs to handle pagination on their side.
     *
     * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ExecuteStatement.html
     */
    func execute<AttributesType, ItemType, TimeToLiveAttributesType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?,
        nextToken: String?
    ) async throws
        -> (
            items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>], lastEvaluatedKey: String?
        )
}
