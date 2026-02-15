//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// Copyright (c) 2026 the DynamoDBTables authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of DynamoDBTables authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

//
//  SotoDynamoDBCompositePrimaryKeyTable.swift
//  DynamoDBTablesSoto
//

#if SOTOSDK
import DynamoDBTables
import Logging
import SotoDynamoDB

/// A `DynamoDBCompositePrimaryKeyTable` implementation backed by the DynamoDB
/// service provided by Soto.
///
/// ```swift
/// let client = AWSClient()
/// let table = SotoDynamoDBCompositePrimaryKeyTable(
///     tableName: "MyTable",
///     client: client,
///     region: .useast1
/// )
///
/// let table = SotoDynamoDBCompositePrimaryKeyTable(
///     tableName: "MyTable",
///     client: existingDynamoDB
/// )
/// ```
public struct SotoDynamoDBCompositePrimaryKeyTable: DynamoDBCompositePrimaryKeyTable, Sendable {
    // Wrapper struct rather than typealias so that GenericDynamoDBCompositePrimaryKeyTable
    // can use package access level while this type remains public.
    private let wrapped: GenericDynamoDBCompositePrimaryKeyTable<SotoDynamoDB.DynamoDB>

    public var tableConfiguration: DynamoDBTableConfiguration { self.wrapped.tableConfiguration }
    public var tableMetrics: DynamoDBTableMetrics { self.wrapped.tableMetrics }

    public init(
        tableName: String,
        client: AWSClient,
        region: Region? = nil,
        tableConfiguration: DynamoDBTableConfiguration = .init(),
        tableMetrics: DynamoDBTableMetrics = .init(),
        logger: Logging.Logger? = nil
    ) {
        self.init(
            tableName: tableName,
            client: DynamoDB(client: client, region: region),
            tableConfiguration: tableConfiguration,
            tableMetrics: tableMetrics,
            logger: logger
        )
    }

    public init(
        tableName: String,
        client: SotoDynamoDB.DynamoDB,
        tableConfiguration: DynamoDBTableConfiguration = .init(),
        tableMetrics: DynamoDBTableMetrics = .init(),
        logger: Logging.Logger? = nil
    ) {
        self.wrapped = GenericDynamoDBCompositePrimaryKeyTable(
            tableName: tableName,
            client: client,
            tableConfiguration: tableConfiguration,
            tableMetrics: tableMetrics,
            logger: logger
        )
    }

    // MARK: - DynamoDBCompositePrimaryKeyTable forwarding

    public func insertItem(_ item: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
        try await self.wrapped.insertItem(item)
    }

    public func clobberItem(_ item: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
        try await self.wrapped.clobberItem(item)
    }

    public func updateItem<AttributesType, ItemType, TimeToLiveAttributesType>(
        newItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        existingItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>
    ) async throws {
        try await self.wrapped.updateItem(newItem: newItem, existingItem: existingItem)
    }

    public func transactWrite(_ entries: [WriteEntry<some Any, some Any, some Any>]) async throws {
        try await self.wrapped.transactWrite(entries)
    }

    public func polymorphicTransactWrite(
        _ entries: [some PolymorphicWriteEntry]
    ) async throws {
        try await self.wrapped.polymorphicTransactWrite(entries)
    }

    public func transactWrite<AttributesType, ItemType, TimeToLiveAttributesType>(
        _ entries: [WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>],
        constraints: [TransactionConstraintEntry<AttributesType, ItemType, TimeToLiveAttributesType>]
    ) async throws {
        try await self.wrapped.transactWrite(entries, constraints: constraints)
    }

    public func polymorphicTransactWrite<
        WriteEntryType: PolymorphicWriteEntry,
        TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry
    >(
        _ entries: [WriteEntryType],
        constraints: [TransactionConstraintEntryType]
    ) async throws
    where WriteEntryType.AttributesType == TransactionConstraintEntryType.AttributesType {
        try await self.wrapped.polymorphicTransactWrite(entries, constraints: constraints)
    }

    public func bulkWrite(_ entries: [WriteEntry<some Any, some Any, some Any>]) async throws {
        try await self.wrapped.bulkWrite(entries)
    }

    public func bulkWriteWithFallback(_ entries: [WriteEntry<some Any, some Any, some Any>]) async throws {
        try await self.wrapped.bulkWriteWithFallback(entries)
    }

    public func polymorphicBulkWrite(_ entries: [some PolymorphicWriteEntry]) async throws {
        try await self.wrapped.polymorphicBulkWrite(entries)
    }

    public func getItem<AttributesType, ItemType, TimeToLiveAttributesType>(
        forKey key: CompositePrimaryKey<AttributesType>
    ) async throws
        -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>?
    {
        try await self.wrapped.getItem(forKey: key)
    }

    public func polymorphicGetItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>]
    ) async throws
        -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]
    {
        try await self.wrapped.polymorphicGetItems(forKeys: keys)
    }

    public func deleteItem(forKey key: CompositePrimaryKey<some Any>) async throws {
        try await self.wrapped.deleteItem(forKey: key)
    }

    public func deleteItem(existingItem: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
        try await self.wrapped.deleteItem(existingItem: existingItem)
    }

    public func deleteItems(forKeys keys: [CompositePrimaryKey<some Any>]) async throws {
        try await self.wrapped.deleteItems(forKeys: keys)
    }

    public func deleteItems(existingItems: [TypedTTLDatabaseItem<some Any, some Any, some Any>]) async throws {
        try await self.wrapped.deleteItems(existingItems: existingItems)
    }

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?
    ) async throws
        -> [ReturnedType]
    {
        try await self.wrapped.polymorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition)
    }

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        exclusiveStartKey: String?
    ) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        try await self.wrapped.polymorphicQuery(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition,
            limit: limit,
            exclusiveStartKey: exclusiveStartKey
        )
    }

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        scanIndexForward: Bool,
        exclusiveStartKey: String?
    ) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        try await self.wrapped.polymorphicQuery(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition,
            limit: limit,
            scanIndexForward: scanIndexForward,
            exclusiveStartKey: exclusiveStartKey
        )
    }

    public func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?
    ) async throws -> [ReturnedType] {
        try await self.wrapped.polymorphicExecute(
            partitionKeys: partitionKeys,
            attributesFilter: attributesFilter,
            additionalWhereClause: additionalWhereClause
        )
    }

    public func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?,
        nextToken: String?
    ) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        try await self.wrapped.polymorphicExecute(
            partitionKeys: partitionKeys,
            attributesFilter: attributesFilter,
            additionalWhereClause: additionalWhereClause,
            nextToken: nextToken
        )
    }

    public func getItems<AttributesType, ItemType, TimeToLiveAttributesType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>]
    ) async throws
        -> [CompositePrimaryKey<AttributesType>: TypedTTLDatabaseItem<
            AttributesType, ItemType, TimeToLiveAttributesType
        >]
    {
        try await self.wrapped.getItems(forKeys: keys)
    }

    public func query<AttributesType, ItemType, TimeToLiveAttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?
    ) async throws
        -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        try await self.wrapped.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition)
    }

    public func query<AttributesType, ItemType, TimeToLiveAttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        scanIndexForward: Bool,
        exclusiveStartKey: String?
    ) async throws
        -> (
            items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>], lastEvaluatedKey: String?
        )
    {
        try await self.wrapped.query(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition,
            limit: limit,
            scanIndexForward: scanIndexForward,
            exclusiveStartKey: exclusiveStartKey
        )
    }

    public func execute<AttributesType, ItemType, TimeToLiveAttributesType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?
    ) async throws -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] {
        try await self.wrapped.execute(
            partitionKeys: partitionKeys,
            attributesFilter: attributesFilter,
            additionalWhereClause: additionalWhereClause
        )
    }

    public func execute<AttributesType, ItemType, TimeToLiveAttributesType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?,
        nextToken: String?
    ) async throws
        -> (
            items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>], lastEvaluatedKey: String?
        )
    {
        try await self.wrapped.execute(
            partitionKeys: partitionKeys,
            attributesFilter: attributesFilter,
            additionalWhereClause: additionalWhereClause,
            nextToken: nextToken
        )
    }
}
#endif
