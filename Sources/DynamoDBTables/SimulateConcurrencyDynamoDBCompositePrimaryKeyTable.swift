//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/main/Sources/SmokeDynamoDB/SimulateConcurrencyDynamoDBCompositePrimaryKeyTable.swift
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
//  SimulateConcurrencyDynamoDBCompositePrimaryKeyTable.swift
//  DynamoDBTables
//

import AWSDynamoDB
import Foundation

private let maxStatementLength = 8192

/**
 Implementation of the DynamoDBTable protocol that simulates concurrent access
 to a database by incrementing a row's version every time it is added for
 a specified number of requests.
 */
public class SimulateConcurrencyDynamoDBCompositePrimaryKeyTable: DynamoDBCompositePrimaryKeyTable {
    let wrappedDynamoDBTable: DynamoDBCompositePrimaryKeyTable
    let simulateConcurrencyModifications: Int
    var previousConcurrencyModifications: Int
    let simulateOnInsertItem: Bool
    let simulateOnUpdateItem: Bool

    /**
     Initializer.

     - Parameters:
        - wrappedDynamoDBTable: The underlying DynamoDBTable used by this implementation.
        - simulateConcurrencyModifications: the number of get requests to simulate concurrency for.
        - simulateOnInsertItem: if this instance should simulate concurrency on insertItem.
        - simulateOnUpdateItem: if this instance should simulate concurrency on updateItem.
     */
    public init(wrappedDynamoDBTable: DynamoDBCompositePrimaryKeyTable, simulateConcurrencyModifications: Int,
                simulateOnInsertItem: Bool = true, simulateOnUpdateItem: Bool = true)
    {
        self.wrappedDynamoDBTable = wrappedDynamoDBTable
        self.simulateConcurrencyModifications = simulateConcurrencyModifications
        self.previousConcurrencyModifications = 0
        self.simulateOnInsertItem = simulateOnInsertItem
        self.simulateOnUpdateItem = simulateOnUpdateItem
    }

    public func validateEntry(entry: WriteEntry<some Any, some Any>) throws {
        let entryString = "\(entry)"
        if entryString.count > maxStatementLength {
            throw DynamoDBTableError.statementLengthExceeded(
                reason: "failed to satisfy constraint: Member must have length less than or equal to \(maxStatementLength). Actual length \(entryString.count)")
        }
    }

    public func insertItem(_ item: TypedDatabaseItem<some Any, some Any>) async throws {
        // if there are still modifications to be made and there is an existing row
        if self.simulateOnInsertItem, self.previousConcurrencyModifications < self.simulateConcurrencyModifications {
            // insert an item so the conditional check will fail
            try await self.wrappedDynamoDBTable.insertItem(item)

            self.previousConcurrencyModifications += 1

            // then delegate to the wrapped implementation
            try await self.wrappedDynamoDBTable.insertItem(item)
        }

        // otherwise just delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.insertItem(item)
    }

    public func clobberItem(_ item: TypedDatabaseItem<some Any, some Any>) async throws {
        try await self.wrappedDynamoDBTable.clobberItem(item)
    }

    public func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     existingItem: TypedDatabaseItem<AttributesType, ItemType>) async throws
    {
        // if there are still modifications to be made and there is an existing row
        if self.simulateOnUpdateItem, self.previousConcurrencyModifications < self.simulateConcurrencyModifications {
            try await self.wrappedDynamoDBTable.updateItem(newItem: existingItem.createUpdatedItem(withValue: existingItem.rowValue),
                                                           existingItem: existingItem)

            self.previousConcurrencyModifications += 1

            // then delegate to the wrapped implementation
            try await self.wrappedDynamoDBTable.updateItem(newItem: newItem, existingItem: existingItem)
        }

        // otherwise just delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.updateItem(newItem: newItem, existingItem: existingItem)
    }

    public func transactWrite(_ entries: [WriteEntry<some Any, some Any>]) async throws {
        try await self.wrappedDynamoDBTable.transactWrite(entries)
    }

    public func transactWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>],
                                                        constraints: [TransactionConstraintEntry<AttributesType, ItemType>]) async throws
    {
        try await self.wrappedDynamoDBTable.transactWrite(entries, constraints: constraints)
    }

    public func polymorphicTransactWrite(_ entries: sending [some PolymorphicWriteEntry]) async throws {
        try await self.wrappedDynamoDBTable.polymorphicTransactWrite(entries)
    }

    public func polymorphicTransactWrite(
        _ entries: sending [some PolymorphicWriteEntry], constraints: sending [some PolymorphicTransactionConstraintEntry]) async throws
    {
        try await self.wrappedDynamoDBTable.polymorphicTransactWrite(entries, constraints: constraints)
    }

    public func polymorphicBulkWrite(_ entries: sending [some PolymorphicWriteEntry]) async throws {
        try await self.wrappedDynamoDBTable.polymorphicBulkWrite(entries)
    }

    public func bulkWrite(_ entries: [WriteEntry<some Any, some Any>]) async throws {
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

    public func bulkWriteWithFallback(_ entries: [WriteEntry<some Any, some Any>]) async throws {
        try await self.wrappedDynamoDBTable.bulkWriteWithFallback(entries)
    }

    public func bulkWriteWithoutThrowing(
        _ entries: [WriteEntry<some Any, some Any>]) async throws
        -> Set<DynamoDBClientTypes.BatchStatementErrorCodeEnum>
    {
        try await self.wrappedDynamoDBTable.bulkWriteWithoutThrowing(entries)
    }

    public func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) async throws
        -> TypedDatabaseItem<AttributesType, ItemType>?
    {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.getItem(forKey: key)
    }

    public func polymorphicGetItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>]) async throws
        -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]
    {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.polymorphicGetItems(forKeys: keys)
    }

    public func deleteItem(forKey key: CompositePrimaryKey<some Any>) async throws {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.deleteItem(forKey: key)
    }

    public func deleteItem(existingItem: TypedDatabaseItem<some PrimaryKeyAttributes, some Decodable & Encodable>) async throws {
        try await self.wrappedDynamoDBTable.deleteItem(existingItem: existingItem)
    }

    public func deleteItems(forKeys keys: [CompositePrimaryKey<some Any>]) async throws {
        try await self.wrappedDynamoDBTable.deleteItems(forKeys: keys)
    }

    public func deleteItems(existingItems: [some DatabaseItem]) async throws {
        try await self.wrappedDynamoDBTable.deleteItems(existingItems: existingItems)
    }

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                               sortKeyCondition: AttributeCondition?,
                                                                               consistentRead: Bool) async throws
        -> [ReturnedType]
    {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.polymorphicQuery(forPartitionKey: partitionKey,
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
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.polymorphicQuery(forPartitionKey: partitionKey,
                                                             sortKeyCondition: sortKeyCondition,
                                                             limit: limit,
                                                             exclusiveStartKey: exclusiveStartKey,
                                                             consistentRead: consistentRead)
    }

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                               sortKeyCondition: AttributeCondition?,
                                                                               limit: Int?,
                                                                               scanIndexForward: Bool,
                                                                               exclusiveStartKey: String?,
                                                                               consistentRead: Bool) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.polymorphicQuery(forPartitionKey: partitionKey,
                                                             sortKeyCondition: sortKeyCondition,
                                                             limit: limit,
                                                             scanIndexForward: scanIndexForward,
                                                             exclusiveStartKey: exclusiveStartKey,
                                                             consistentRead: consistentRead)
    }

    public func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
        -> [ReturnedType]
    {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.polymorphicExecute(partitionKeys: partitionKeys,
                                                               attributesFilter: attributesFilter,
                                                               additionalWhereClause: additionalWhereClause)
    }

    public func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.polymorphicExecute(partitionKeys: partitionKeys,
                                                               attributesFilter: attributesFilter,
                                                               additionalWhereClause: additionalWhereClause,
                                                               nextToken: nextToken)
    }

    public func getItems<AttributesType, ItemType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws
        -> [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>]
    {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.getItems(forKeys: keys)
    }

    public func execute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
        -> [TypedDatabaseItem<AttributesType, ItemType>]
    {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.execute(partitionKeys: partitionKeys,
                                                    attributesFilter: attributesFilter,
                                                    additionalWhereClause: additionalWhereClause)
    }

    public func execute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
        -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?)
    {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.execute(partitionKeys: partitionKeys,
                                                    attributesFilter: attributesFilter,
                                                    additionalWhereClause: additionalWhereClause,
                                                    nextToken: nextToken)
    }

    public func query<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                sortKeyCondition: AttributeCondition?,
                                                consistentRead: Bool) async throws
        -> [TypedDatabaseItem<AttributesType, ItemType>]
    {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.query(forPartitionKey: partitionKey,
                                                  sortKeyCondition: sortKeyCondition,
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
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.query(forPartitionKey: partitionKey,
                                                  sortKeyCondition: sortKeyCondition,
                                                  limit: limit,
                                                  scanIndexForward: scanIndexForward,
                                                  exclusiveStartKey: exclusiveStartKey,
                                                  consistentRead: consistentRead)
    }
}
