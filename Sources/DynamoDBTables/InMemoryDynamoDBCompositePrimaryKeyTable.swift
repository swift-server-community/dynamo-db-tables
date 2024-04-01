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
// https://github.com/swift-server-community/dynamo-db-tables/compare/6fec4c8..main
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

import Foundation
import SmokeHTTPClient
import DynamoDBModel

public protocol PolymorphicOperationReturnTypeConvertable {
    var createDate: Foundation.Date { get }
    var rowStatus: RowStatus { get }
    
    var rowTypeIdentifier: String { get }
}

extension TypedDatabaseItem: PolymorphicOperationReturnTypeConvertable {
    public var rowTypeIdentifier: String {
        return getTypeRowIdentifier(type: RowType.self)
    }
}

public typealias ExecuteItemFilterType = (String, String, String, PolymorphicOperationReturnTypeConvertable)
    -> Bool

public protocol InMemoryTransactionDelegate {
    func injectErrors<WriteEntryType: PolymorphicWriteEntry,
                      TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
                        _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType],
                        table: InMemoryDynamoDBCompositePrimaryKeyTable) async throws -> [DynamoDBTableError]
}

public struct InMemoryDynamoDBCompositePrimaryKeyTable: DynamoDBCompositePrimaryKeyTable {
    public let escapeSingleQuoteInPartiQL: Bool
    public let transactionDelegate: InMemoryTransactionDelegate?
    internal let storeWrapper: InMemoryDynamoDBCompositePrimaryKeyTableStore
    
    public init(executeItemFilter: ExecuteItemFilterType? = nil,
                escapeSingleQuoteInPartiQL: Bool = false,
                transactionDelegate: InMemoryTransactionDelegate? = nil) {
        self.storeWrapper = InMemoryDynamoDBCompositePrimaryKeyTableStore(executeItemFilter: executeItemFilter)
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.transactionDelegate = transactionDelegate
    }
    
    internal init(storeWrapper: InMemoryDynamoDBCompositePrimaryKeyTableStore,
                  escapeSingleQuoteInPartiQL: Bool = false,
                  transactionDelegate: InMemoryTransactionDelegate? = nil) {
        self.storeWrapper = storeWrapper
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.transactionDelegate = transactionDelegate
    }
    
    public var store: [String: [String: PolymorphicOperationReturnTypeConvertable]] {
        get async {
            return await self.storeWrapper.store
        }
    }

    public func validateEntry<AttributesType, ItemType>(entry: WriteEntry<AttributesType, ItemType>) throws {
        try self.storeWrapper.validateEntry(entry: entry)
    }

    public func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        return try await storeWrapper.insertItem(item)
    }

    public func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        return try await storeWrapper.clobberItem(item)
    }

    public func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     existingItem: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        return try await storeWrapper.updateItem(newItem: newItem, existingItem: existingItem)
    }
    
    public func transactWrite<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws {
        let noConstraints: [EmptyPolymorphicTransactionConstraintEntry] = []
        return try await transactWrite(entries, constraints: noConstraints)
    }
    
    public func transactWrite<WriteEntryType: PolymorphicWriteEntry,
                              TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
                                _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType]) async throws {
        // if there is a transaction delegate and it wants to inject errors
        if let errors = try await transactionDelegate?.injectErrors(entries, constraints: constraints, table: self), !errors.isEmpty {
            throw DynamoDBTableError.transactionCanceled(reasons: errors)
        }
                                    
        return try await storeWrapper.bulkWrite(entries, constraints: constraints, isTransaction: true)
    }
    
    public func bulkWrite<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws {
        let noConstraints: [EmptyPolymorphicTransactionConstraintEntry] = []
        return try await storeWrapper.bulkWrite(entries, constraints: noConstraints, isTransaction: false)
    }
    
    public func monomorphicBulkWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws {
        return try await storeWrapper.monomorphicBulkWrite(entries)
    }
    
    public func monomorphicBulkWriteWithFallback<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws {
        try await self.storeWrapper.monomorphicBulkWriteWithFallback(entries)
    }

    public func monomorphicBulkWriteWithoutThrowing<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws
    -> Set<BatchStatementErrorCodeEnum> {
        return try await storeWrapper.monomorphicBulkWriteWithoutThrowing(entries)
    }
    
    public func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) async throws
    -> TypedDatabaseItem<AttributesType, ItemType>? {
        return try await storeWrapper.getItem(forKey: key)
    }
    
    public func getItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>]) async throws
    -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType] {
        return try await storeWrapper.getItems(forKeys: keys)
    }

    public func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) async throws {
        return try await storeWrapper.deleteItem(forKey: key)
    }
    
    public func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws {
        return try await storeWrapper.deleteItems(forKeys: keys)
    }
    
    public func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType]) async throws {
        return try await storeWrapper.deleteItems(existingItems: existingItems)
    }
    
    public func deleteItem<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        return try await storeWrapper.deleteItem(existingItem: existingItem)
    }

    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    consistentRead: Bool) async throws
    -> [ReturnedType] {
        return try await storeWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                            consistentRead: consistentRead)
    }
    
    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    limit: Int?,
                                                                    exclusiveStartKey: String?,
                                                                    consistentRead: Bool) async throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        return try await storeWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                            limit: limit, exclusiveStartKey: exclusiveStartKey, consistentRead: consistentRead)
    }

    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    limit: Int?,
                                                                    scanIndexForward: Bool,
                                                                    exclusiveStartKey: String?,
                                                                    consistentRead: Bool) async throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        return try await storeWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                            limit: limit, scanIndexForward: scanIndexForward,
                                            exclusiveStartKey: exclusiveStartKey, consistentRead: consistentRead)
    }
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
    -> [ReturnedType] {
        return try await storeWrapper.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                              additionalWhereClause: additionalWhereClause)
    }
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        return try await storeWrapper.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                              additionalWhereClause: additionalWhereClause, nextToken: nextToken)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        return try await storeWrapper.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                         additionalWhereClause: additionalWhereClause)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
    -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?) {
        return try await storeWrapper.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                               additionalWhereClause: additionalWhereClause, nextToken: nextToken)
    }
    
    public func monomorphicGetItems<AttributesType, ItemType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws
    -> [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>] {
        return try await storeWrapper.monomorphicGetItems(forKeys: keys)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                           sortKeyCondition: AttributeCondition?,
                                                           consistentRead: Bool) async throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        return try await storeWrapper.monomorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                       consistentRead: consistentRead)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                           sortKeyCondition: AttributeCondition?,
                                                           limit: Int?,
                                                           scanIndexForward: Bool,
                                                           exclusiveStartKey: String?,
                                                           consistentRead: Bool) async throws
       -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?) {
        return try await storeWrapper.monomorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                       limit: limit, scanIndexForward: scanIndexForward,
                                                       exclusiveStartKey: exclusiveStartKey, consistentRead: consistentRead)
    }
}
