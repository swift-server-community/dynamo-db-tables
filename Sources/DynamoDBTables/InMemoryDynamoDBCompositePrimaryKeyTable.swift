// swiftlint:disable cyclomatic_complexity
//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/main/Sources/SmokeDynamoDB/InMemoryDynamoDBCompositePrimaryKeyTable.swift
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
//  InMemoryDynamoDBCompositePrimaryKeyTable.swift
//  DynamoDBTables
//

@preconcurrency import AWSDynamoDB
import Foundation

public protocol PolymorphicOperationReturnTypeConvertable: Sendable {
    var createDate: Foundation.Date { get }
    var rowStatus: RowStatus { get }

    var rowTypeIdentifier: String { get }
}

extension TypedDatabaseItem: PolymorphicOperationReturnTypeConvertable {
    public var rowTypeIdentifier: String {
        getTypeRowIdentifier(type: RowType.self)
    }
}

public typealias ExecuteItemFilterType = @Sendable (String, String, String, PolymorphicOperationReturnTypeConvertable)
    -> Bool

public protocol InMemoryTransactionDelegate {
    /**
      Inject errors into a `transactWrite` or `polymorphicTransactWrite` call.
     */
    func injectErrors<AttributesType>(
        inputKeys: [CompositePrimaryKey<AttributesType>?], table: InMemoryDynamoDBCompositePrimaryKeyTable) async throws -> [DynamoDBTableError]
}

public struct InMemoryDynamoDBCompositePrimaryKeyTable: DynamoDBCompositePrimaryKeyTable {
    public let escapeSingleQuoteInPartiQL: Bool
    public let transactionDelegate: InMemoryTransactionDelegate?
    let storeWrapper: InMemoryDynamoDBCompositePrimaryKeyTableStore

    public init(executeItemFilter: ExecuteItemFilterType? = nil,
                escapeSingleQuoteInPartiQL: Bool = false,
                transactionDelegate: InMemoryTransactionDelegate? = nil)
    {
        self.storeWrapper = InMemoryDynamoDBCompositePrimaryKeyTableStore(executeItemFilter: executeItemFilter)
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.transactionDelegate = transactionDelegate
    }

    init(storeWrapper: InMemoryDynamoDBCompositePrimaryKeyTableStore,
         escapeSingleQuoteInPartiQL: Bool = false,
         transactionDelegate: InMemoryTransactionDelegate? = nil)
    {
        self.storeWrapper = storeWrapper
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.transactionDelegate = transactionDelegate
    }

    public var store: [String: [String: PolymorphicOperationReturnTypeConvertable]] {
        get async {
            await self.storeWrapper.store
        }
    }

    public func validateEntry(entry: WriteEntry<some Any, some Any>) throws {
        try self.storeWrapper.validateEntry(entry: entry)
    }

    public func insertItem(_ item: TypedDatabaseItem<some Any, some Any>) async throws {
        try await self.storeWrapper.insertItem(item)
    }

    public func clobberItem(_ item: TypedDatabaseItem<some Any, some Any>) async throws {
        try await self.storeWrapper.clobberItem(item)
    }

    public func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     existingItem: TypedDatabaseItem<AttributesType, ItemType>) async throws
    {
        try await self.storeWrapper.updateItem(newItem: newItem, existingItem: existingItem)
    }

    public func transactWrite(_ entries: [WriteEntry<some Any, some Any>]) async throws {
        try await self.transactWrite(entries, constraints: [])
    }

    public func transactWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>],
                                                        constraints: [TransactionConstraintEntry<AttributesType, ItemType>]) async throws
    {
        // if there is a transaction delegate and it wants to inject errors
        let inputKeys = entries.map(\.compositePrimaryKey) + constraints.map(\.compositePrimaryKey)
        if let errors = try await transactionDelegate?.injectErrors(inputKeys: inputKeys, table: self), !errors.isEmpty {
            throw DynamoDBTableError.transactionCanceled(reasons: errors)
        }

        return try await self.storeWrapper.bulkWrite(entries, constraints: constraints, isTransaction: true)
    }

    public func polymorphicTransactWrite(_ entries: sending [some PolymorphicWriteEntry]) async throws {
        let noConstraints: [EmptyPolymorphicTransactionConstraintEntry] = []
        return try await self.polymorphicTransactWrite(entries, constraints: noConstraints)
    }

    public func polymorphicTransactWrite(
        _ entries: sending [some PolymorphicWriteEntry], constraints: sending [some PolymorphicTransactionConstraintEntry]) async throws
    {
        // if there is a transaction delegate and it wants to inject errors
        let inputKeys = entries.map(\.compositePrimaryKey) + constraints.map(\.compositePrimaryKey)
        if let errors = try await transactionDelegate?.injectErrors(inputKeys: inputKeys, table: self), !errors.isEmpty {
            throw DynamoDBTableError.transactionCanceled(reasons: errors)
        }

        return try await self.storeWrapper.polymorphicBulkWrite(entries, constraints: constraints, isTransaction: true)
    }

    public func polymorphicBulkWrite(_ entries: sending [some PolymorphicWriteEntry]) async throws {
        let noConstraints: [EmptyPolymorphicTransactionConstraintEntry] = []
        return try await self.storeWrapper.polymorphicBulkWrite(entries, constraints: noConstraints, isTransaction: false)
    }

    public func bulkWrite(_ entries: [WriteEntry<some Any, some Any>]) async throws {
        try await self.storeWrapper.bulkWrite(entries, constraints: [], isTransaction: false)
    }

    public func bulkWriteWithFallback(_ entries: [WriteEntry<some Any, some Any>]) async throws {
        try await self.storeWrapper.bulkWriteWithFallback(entries)
    }

    public func bulkWriteWithoutThrowing(_ entries: [WriteEntry<some Any, some Any>]) async throws
        -> Set<DynamoDBClientTypes.BatchStatementErrorCodeEnum>
    {
        try await self.storeWrapper.bulkWriteWithoutThrowing(entries)
    }

    public func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) async throws
        -> TypedDatabaseItem<AttributesType, ItemType>?
    {
        try await self.storeWrapper.getItem(forKey: key)
    }

    public func polymorphicGetItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>]) async throws
        -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]
    {
        try await self.storeWrapper.polymorphicGetItems(forKeys: keys)
    }

    public func deleteItem(forKey key: CompositePrimaryKey<some Any>) async throws {
        try await self.storeWrapper.deleteItem(forKey: key)
    }

    public func deleteItems(forKeys keys: [CompositePrimaryKey<some Any>]) async throws {
        try await self.storeWrapper.deleteItems(forKeys: keys)
    }

    public func deleteItems(existingItems: [some DatabaseItem]) async throws {
        try await self.storeWrapper.deleteItems(existingItems: existingItems)
    }

    public func deleteItem(existingItem: TypedDatabaseItem<some Any, some Any>) async throws {
        try await self.storeWrapper.deleteItem(existingItem: existingItem)
    }

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                               sortKeyCondition: AttributeCondition?,
                                                                               consistentRead: Bool) async throws
        -> [ReturnedType]
    {
        try await self.storeWrapper.polymorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                     consistentRead: consistentRead)
    }

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                               sortKeyCondition: AttributeCondition?,
                                                                               limit: Int?,
                                                                               exclusiveStartKey: String?,
                                                                               consistentRead: Bool) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        try await self.storeWrapper.polymorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                     limit: limit, exclusiveStartKey: exclusiveStartKey, consistentRead: consistentRead)
    }

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                               sortKeyCondition: AttributeCondition?,
                                                                               limit: Int?,
                                                                               scanIndexForward: Bool,
                                                                               exclusiveStartKey: String?,
                                                                               consistentRead: Bool) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        try await self.storeWrapper.polymorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                     limit: limit, scanIndexForward: scanIndexForward,
                                                     exclusiveStartKey: exclusiveStartKey, consistentRead: consistentRead)
    }

    public func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
        -> [ReturnedType]
    {
        try await self.storeWrapper.polymorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                       additionalWhereClause: additionalWhereClause)
    }

    public func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        try await self.storeWrapper.polymorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                       additionalWhereClause: additionalWhereClause, nextToken: nextToken)
    }

    public func execute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
        -> [TypedDatabaseItem<AttributesType, ItemType>]
    {
        try await self.storeWrapper.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                            additionalWhereClause: additionalWhereClause)
    }

    public func execute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
        -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?)
    {
        try await self.storeWrapper.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                            additionalWhereClause: additionalWhereClause, nextToken: nextToken)
    }

    public func getItems<AttributesType, ItemType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws
        -> [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>]
    {
        try await self.storeWrapper.getItems(forKeys: keys)
    }

    public func query<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                sortKeyCondition: AttributeCondition?,
                                                consistentRead: Bool) async throws
        -> [TypedDatabaseItem<AttributesType, ItemType>]
    {
        try await self.storeWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                          consistentRead: consistentRead)
    }

    public func query<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                sortKeyCondition: AttributeCondition?,
                                                limit: Int?,
                                                scanIndexForward: Bool,
                                                exclusiveStartKey: String?,
                                                consistentRead: Bool) async throws
        -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?)
    {
        try await self.storeWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                          limit: limit, scanIndexForward: scanIndexForward,
                                          exclusiveStartKey: exclusiveStartKey, consistentRead: consistentRead)
    }
}
