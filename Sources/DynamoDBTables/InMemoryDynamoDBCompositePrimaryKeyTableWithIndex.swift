// swiftlint:disable cyclomatic_complexity type_body_length
//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/main/Sources/SmokeDynamoDB/InMemoryDynamoDBCompositePrimaryKeyTableWithIndex.swift
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
//  InMemoryDynamoDBCompositePrimaryKeyTableWithIndex.swift
//  DynamoDBTables
//

import AWSDynamoDB
import Foundation

private let maxStatementLength = 8192

public enum GSIError: Error {
    case unknownIndex(name: String)
}

public struct InMemoryDynamoDBCompositePrimaryKeyTableWithIndex<GSILogic: DynamoDBCompositePrimaryKeyGSILogic>: DynamoDBCompositePrimaryKeyTable {
    public let primaryTable: InMemoryDynamoDBCompositePrimaryKeyTable
    public let gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable

    private let gsiName: String
    private let gsiLogic: GSILogic

    public init(gsiName: String,
                gsiLogic: GSILogic,
                executeItemFilter: ExecuteItemFilterType? = nil)
    {
        self.gsiName = gsiName
        self.gsiLogic = gsiLogic
        self.primaryTable = InMemoryDynamoDBCompositePrimaryKeyTable(executeItemFilter: executeItemFilter)
        self.gsiDataStore = InMemoryDynamoDBCompositePrimaryKeyTable(executeItemFilter: executeItemFilter)
    }

    public func validateEntry(entry: WriteEntry<some Any, some Any, some Any>) throws {
        let entryString = "\(entry)"
        if entryString.count > maxStatementLength {
            throw DynamoDBTableError.statementLengthExceeded(
                reason: "failed to satisfy constraint: Member must have length less than or equal to \(maxStatementLength). Actual length \(entryString.count)")
        }
    }

    public func insertItem(_ item: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
        try await self.primaryTable.insertItem(item)
        try await self.gsiLogic.onInsertItem(item, gsiDataStore: self.gsiDataStore)
    }

    public func clobberItem(_ item: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
        try await self.primaryTable.clobberItem(item)
        try await self.gsiLogic.onClobberItem(item, gsiDataStore: self.gsiDataStore)
    }

    public func updateItem<AttributesType, ItemType, TimeToLiveAttributesType>(
        newItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        existingItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) async throws
    {
        try await self.primaryTable.updateItem(newItem: newItem, existingItem: existingItem)
        try await self.gsiLogic.onUpdateItem(newItem: newItem, existingItem: existingItem, gsiDataStore: self.gsiDataStore)
    }

    public func transactWrite(_ entries: [WriteEntry<some Any, some Any, some Any>]) async throws {
        try await self.primaryTable.transactWrite(entries)
    }

    public func transactWrite<AttributesType, ItemType, TimeToLiveAttributesType>(
        _ entries: [WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>],
        constraints: [TransactionConstraintEntry<AttributesType, ItemType, TimeToLiveAttributesType>]) async throws
    {
        try await self.primaryTable.transactWrite(entries, constraints: constraints)
    }

    public func polymorphicTransactWrite(_ entries: [some PolymorphicWriteEntry]) async throws {
        try await self.primaryTable.polymorphicTransactWrite(entries)
    }

    public func polymorphicTransactWrite<WriteEntryType: PolymorphicWriteEntry,
        TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
        _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType]) async throws
        where WriteEntryType.AttributesType == TransactionConstraintEntryType.AttributesType
    {
        try await self.primaryTable.polymorphicTransactWrite(entries, constraints: constraints)
    }

    public func polymorphicBulkWrite(_ entries: [some PolymorphicWriteEntry]) async throws {
        try await self.primaryTable.polymorphicBulkWrite(entries)
    }

    public func bulkWrite(_ entries: [WriteEntry<some Any, some Any, some Any>]) async throws {
        try await entries.asyncForEach { entry in
            switch entry {
            case let .update(new: new, existing: existing):
                return try await self.updateItem(newItem: new, existingItem: existing)
            case let .insert(new: new):
                return try await self.insertItem(new)
            case let .deleteAtKey(key: key):
                return try await self.deleteItem(forKey: key)
            case let .deleteItem(existing: existing):
                return try await self.deleteItem(existingItem: existing)
            }
        }
    }

    public func bulkWriteWithFallback(
        _ entries: [WriteEntry<some Any, some Any, some Any>]) async throws
    {
        try await self.bulkWrite(entries)
    }

    public func getItem<AttributesType, ItemType, TimeToLiveAttributesType>(forKey key: CompositePrimaryKey<AttributesType>) async throws
        -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>?
    {
        try await self.primaryTable.getItem(forKey: key)
    }

    public func polymorphicGetItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>]) async throws
        -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]
    {
        try await self.primaryTable.polymorphicGetItems(forKeys: keys)
    }

    public func deleteItem(forKey key: CompositePrimaryKey<some Any>) async throws {
        try await self.primaryTable.deleteItem(forKey: key)
        try await self.gsiLogic.onDeleteItem(forKey: key, gsiDataStore: self.gsiDataStore)
    }

    public func deleteItem(existingItem: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
        try await self.primaryTable.deleteItem(existingItem: existingItem)
        try await self.gsiLogic.onDeleteItem(forKey: existingItem.compositePrimaryKey, gsiDataStore: self.gsiDataStore)
    }

    public func deleteItems(forKeys keys: [CompositePrimaryKey<some Any>]) async throws {
        try await self.primaryTable.deleteItems(forKeys: keys)

        try await keys.asyncForEach { key in
            try await self.gsiLogic.onDeleteItem(forKey: key, gsiDataStore: self.gsiDataStore)
        }
    }

    public func deleteItems(existingItems: [TypedTTLDatabaseItem<some Any, some Any, some Any>]) async throws {
        try await self.primaryTable.deleteItems(existingItems: existingItems)

        try await existingItems.asyncForEach { existingItem in
            try await self.gsiLogic.onDeleteItem(forKey: existingItem.compositePrimaryKey, gsiDataStore: self.gsiDataStore)
        }
    }

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                               sortKeyCondition: AttributeCondition?,
                                                                               consistentRead: Bool) async throws
        -> [ReturnedType]
    {
        // if this is querying an index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == self.gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }

            // query on the index
            return try await self.gsiDataStore.polymorphicQuery(forPartitionKey: partitionKey,
                                                                sortKeyCondition: sortKeyCondition,
                                                                consistentRead: consistentRead)
        }

        // query on the main table
        return try await self.primaryTable.polymorphicQuery(forPartitionKey: partitionKey,
                                                            sortKeyCondition: sortKeyCondition,
                                                            consistentRead: consistentRead)
    }

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                               sortKeyCondition: AttributeCondition?,
                                                                               limit: Int?,
                                                                               exclusiveStartKey: String?,
                                                                               consistentRead: Bool) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        // if this is querying an index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == self.gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }

            // query on the index
            return try await self.gsiDataStore.polymorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                                limit: limit, exclusiveStartKey: exclusiveStartKey, consistentRead: consistentRead)
        }

        // query on the main table
        return try await self.primaryTable.polymorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
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
        // if this is querying an index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == self.gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }

            // query on the index
            return try await self.gsiDataStore.polymorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                                limit: limit, scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                                                consistentRead: consistentRead)
        }

        // query on the main table
        return try await self.primaryTable.polymorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                            limit: limit, scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                                            consistentRead: consistentRead)
    }

    public func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws -> [ReturnedType]
    {
        // if this is executing on index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == self.gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }

            // execute on the index
            return try await self.gsiDataStore.polymorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                                  additionalWhereClause: additionalWhereClause)
        }

        // execute on the main table
        return try await self.primaryTable.polymorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                              additionalWhereClause: additionalWhereClause)
    }

    public func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        // if this is executing on index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == self.gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }

            // execute on the index
            return try await self.gsiDataStore.polymorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                                  additionalWhereClause: additionalWhereClause, nextToken: nextToken)
        }

        // execute on the main table
        return try await self.primaryTable.polymorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                              additionalWhereClause: additionalWhereClause, nextToken: nextToken)
    }

    public func getItems<AttributesType, ItemType, TimeToLiveAttributesType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws
        -> [CompositePrimaryKey<AttributesType>: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        try await self.primaryTable.getItems(forKeys: keys)
    }

    public func query<AttributesType, ItemType, TimeToLiveAttributesType>(forPartitionKey partitionKey: String,
                                                                          sortKeyCondition: AttributeCondition?,
                                                                          consistentRead: Bool) async throws
        -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        // if this is querying an index
        if let indexName = AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == self.gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }

            // query on the index
            return try await self.gsiDataStore.query(forPartitionKey: partitionKey,
                                                     sortKeyCondition: sortKeyCondition,
                                                     consistentRead: consistentRead)
        }

        // query on the main table
        return try await self.primaryTable.query(forPartitionKey: partitionKey,
                                                 sortKeyCondition: sortKeyCondition,
                                                 consistentRead: consistentRead)
    }

    public func query<AttributesType, ItemType, TimeToLiveAttributesType>(forPartitionKey partitionKey: String,
                                                                          sortKeyCondition: AttributeCondition?,
                                                                          limit: Int?,
                                                                          scanIndexForward: Bool,
                                                                          exclusiveStartKey: String?,
                                                                          consistentRead: Bool) async throws
        -> (items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>], lastEvaluatedKey: String?)
    {
        // if this is querying an index
        if let indexName = AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == self.gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }

            // query on the index
            return try await self.gsiDataStore.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                     limit: limit, scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                                     consistentRead: consistentRead)
        }

        // query on the main table
        return try await self.primaryTable.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                 limit: limit, scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                                 consistentRead: consistentRead)
    }

    public func execute<AttributesType, ItemType, TimeToLiveAttributesType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        // if this is executing on index
        if let indexName = AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == self.gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }

            // execute on the index
            return try await self.gsiDataStore.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                       additionalWhereClause: additionalWhereClause)
        }

        // execute on the main table
        return try await self.primaryTable.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                   additionalWhereClause: additionalWhereClause)
    }

    public func execute<AttributesType, ItemType, TimeToLiveAttributesType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
        -> (items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>], lastEvaluatedKey: String?)
    {
        // if this is executing on index
        if let indexName = AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == self.gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }

            // execute on the index
            return try await self.gsiDataStore.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                       additionalWhereClause: additionalWhereClause, nextToken: nextToken)
        }

        // execute on the main table
        return try await self.primaryTable.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                   additionalWhereClause: additionalWhereClause, nextToken: nextToken)
    }
}
