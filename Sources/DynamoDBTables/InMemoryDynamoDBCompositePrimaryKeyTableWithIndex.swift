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

import Foundation
import AWSDynamoDB

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
                executeItemFilter: ExecuteItemFilterType? = nil) {
        self.gsiName = gsiName
        self.gsiLogic = gsiLogic
        self.primaryTable = InMemoryDynamoDBCompositePrimaryKeyTable(executeItemFilter: executeItemFilter)
        self.gsiDataStore = InMemoryDynamoDBCompositePrimaryKeyTable(executeItemFilter: executeItemFilter)
    }
    
    public func validateEntry<AttributesType, ItemType>(entry: WriteEntry<AttributesType, ItemType>) throws {
        let entryString = "\(entry)"
        if entryString.count > maxStatementLength {
            throw DynamoDBTableError.statementLengthExceeded(
                reason: "failed to satisfy constraint: Member must have length less than or equal to \(maxStatementLength). Actual length \(entryString.count)")
        }
    }
    
    public func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        try await self.primaryTable.insertItem(item)
        try await self.gsiLogic.onInsertItem(item, gsiDataStore: self.gsiDataStore)
    }
    
    public func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        try await self.primaryTable.clobberItem(item)
        try await self.gsiLogic.onClobberItem(item, gsiDataStore: self.gsiDataStore)
    }
    
    public func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     existingItem: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        try await self.primaryTable.updateItem(newItem: newItem, existingItem: existingItem)
        try await self.gsiLogic.onUpdateItem(newItem: newItem, existingItem: existingItem, gsiDataStore: self.gsiDataStore)
    }
    
    public func transactWrite<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws {
        return try await self.primaryTable.transactWrite(entries)
    }
    
    public func transactWrite<WriteEntryType: PolymorphicWriteEntry,
                              TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
                                _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType]) async throws {
        return try await self.primaryTable.transactWrite(entries, constraints: constraints)
    }
    
    public func bulkWrite<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws {
        return try await self.primaryTable.bulkWrite(entries)
    }
    
    public func monomorphicBulkWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws {
        try await entries.asyncForEach { entry in
            switch entry {
            case .update(new: let new, existing: let existing):
                return try await updateItem(newItem: new, existingItem: existing)
            case .insert(new: let new):
                return try await insertItem(new)
            case .deleteAtKey(key: let key):
                return try await deleteItem(forKey: key)
            case .deleteItem(existing: let existing):
                return try await deleteItem(existingItem: existing)
            }
        }
    }
    
    public func monomorphicBulkWriteWithFallback<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws {
        // fall back to single operation if the write entry exceeds the statement length limitation
        var nonBulkWriteEntries: [WriteEntry<AttributesType, ItemType>] = []
        
        let bulkWriteEntries = try entries.compactMap { entry in
            do {
                try self.validateEntry(entry: entry)
                return entry
            } catch DynamoDBTableError.statementLengthExceeded {
                nonBulkWriteEntries.append(entry)
                return nil
            }
        }
        
        try await self.monomorphicBulkWrite(bulkWriteEntries)
            
        try await nonBulkWriteEntries.asyncForEach { nonBulkWriteEntry in
            switch nonBulkWriteEntry {
            case .update(new: let new, existing: let existing):
                try await self.updateItem(newItem: new, existingItem: existing)
            case .insert(new: let new):
                try await self.insertItem(new)
            case .deleteAtKey(key: let key):
                try await self.deleteItem(forKey: key)
            case .deleteItem(existing: let existing):
                try await self.deleteItem(existingItem: existing)
            }
        }
    }
    
    public func monomorphicBulkWriteWithoutThrowing<AttributesType, ItemType>(
        _ entries: [WriteEntry<AttributesType, ItemType>]) async throws
    -> Set<DynamoDBClientTypes.BatchStatementErrorCodeEnum> {
        let results = await entries.asyncMap { entry -> DynamoDBClientTypes.BatchStatementErrorCodeEnum? in
            switch entry {
            case .update(new: let new, existing: let existing):
                do {
                    try await updateItem(newItem: new, existingItem: existing)
                    
                    return nil
                } catch {
                    return .duplicateitem
                }
            case .insert(new: let new):
                do {
                    try await insertItem(new)
                    
                    return nil
                } catch {
                    return .duplicateitem
                }
            case .deleteAtKey(key: let key):
                do {
                    try await deleteItem(forKey: key)
                    
                    return nil
                } catch {
                    return .duplicateitem
                }
            case .deleteItem(existing: let existing):
                do {
                    try await deleteItem(existingItem: existing)
                    
                    return nil
                } catch {
                    return .duplicateitem
                }
            }
        }
        
        var errors: Set<DynamoDBClientTypes.BatchStatementErrorCodeEnum> = Set()
        results.forEach { result in
            if let result {
                errors.insert(result)
            }
        }
        return errors
    }
    
    public func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) async throws
    -> TypedDatabaseItem<AttributesType, ItemType>? {
        return try await self.primaryTable.getItem(forKey: key)
    }
    
    public func getItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>]) async throws
    -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType] {
        return try await self.primaryTable.getItems(forKeys: keys)
    }
    
    public func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) async throws {
        try await self.primaryTable.deleteItem(forKey: key)
        try await self.gsiLogic.onDeleteItem(forKey: key, gsiDataStore: self.gsiDataStore)
    }
    
    public func deleteItem<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        try await self.primaryTable.deleteItem(existingItem: existingItem)
        try await self.gsiLogic.onDeleteItem(forKey: existingItem.compositePrimaryKey, gsiDataStore: self.gsiDataStore)
    }
    
    public func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws {
        try await self.primaryTable.deleteItems(forKeys: keys)

        try await keys.asyncForEach { key in
            try await self.gsiLogic.onDeleteItem(forKey: key, gsiDataStore: self.gsiDataStore)
        }
    }
    
    public func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType]) async throws {
        
        try await self.primaryTable.deleteItems(existingItems: existingItems)
        
        try await existingItems.asyncForEach { existingItem in
            try await self.gsiLogic.onDeleteItem(forKey: existingItem.compositePrimaryKey, gsiDataStore: self.gsiDataStore)
        }
    }
    
    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    consistentRead: Bool) async throws
    -> [ReturnedType] {
        // if this is querying an index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
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
    
    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    limit: Int?,
                                                                    exclusiveStartKey: String?,
                                                                    consistentRead: Bool) async throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        // if this is querying an index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }
            
            // query on the index
            return try await self.gsiDataStore.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                     limit: limit, exclusiveStartKey: exclusiveStartKey, consistentRead: consistentRead)
        }
        
        // query on the main table
        return try await self.primaryTable.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                 limit: limit, exclusiveStartKey: exclusiveStartKey, consistentRead: consistentRead)
    }
    
    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    limit: Int?,
                                                                    scanIndexForward: Bool,
                                                                    exclusiveStartKey: String?,
                                                                    consistentRead: Bool) async throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        // if this is querying an index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
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
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
    -> [ReturnedType] {
        // if this is executing on index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
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
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        // if this is executing on index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
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
    
    public func monomorphicGetItems<AttributesType, ItemType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws
    -> [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>] {
        return try await self.primaryTable.monomorphicGetItems(forKeys: keys)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                           sortKeyCondition: AttributeCondition?,
                                                           consistentRead: Bool) async throws
           -> [TypedDatabaseItem<AttributesType, ItemType>] {
        // if this is querying an index
        if let indexName = AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }
            
            // query on the index
            return try await self.gsiDataStore.monomorphicQuery(forPartitionKey: partitionKey,
                                                                sortKeyCondition: sortKeyCondition,
                                                                consistentRead: consistentRead)
        }
        
        // query on the main table
        return try await self.primaryTable.monomorphicQuery(forPartitionKey: partitionKey,
                                                            sortKeyCondition: sortKeyCondition,
                                                            consistentRead: consistentRead)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                           sortKeyCondition: AttributeCondition?,
                                                           limit: Int?,
                                                           scanIndexForward: Bool,
                                                           exclusiveStartKey: String?,
                                                           consistentRead: Bool) async throws
       -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?) {
        // if this is querying an index
        if let indexName = AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }
            
            // query on the index
            return try await self.gsiDataStore.monomorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                                limit: limit, scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                                                consistentRead: consistentRead)
        }
        
        // query on the main table
        return try await self.primaryTable.monomorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                            limit: limit, scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                                            consistentRead: consistentRead)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        // if this is executing on index
        if let indexName = AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }
            
            // execute on the index
            return try await self.gsiDataStore.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                                  additionalWhereClause: additionalWhereClause)
        }
        
        // execute on the main table
        return try await self.primaryTable.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                              additionalWhereClause: additionalWhereClause)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
    -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?) {
        // if this is executing on index
        if let indexName = AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }
            
            // execute on the index
            return try await self.gsiDataStore.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                                  additionalWhereClause: additionalWhereClause, nextToken: nextToken)
        }
        
        // execute on the main table
        return try await self.primaryTable.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                              additionalWhereClause: additionalWhereClause, nextToken: nextToken)
    }
}