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

private let maxStatementLength = 8192

/**
 Implementation of the DynamoDBTable protocol that simulates concurrent access
 to a database by incrementing a row's version every time it is added for
 a specified number of requests.
 */
public actor SimulateConcurrencyDynamoDBCompositePrimaryKeyTable<Wrapped: DynamoDBCompositePrimaryKeyTable & Sendable>:
    DynamoDBCompositePrimaryKeyTable, Sendable
{
    let wrappedDynamoDBTable: Wrapped
    let simulateConcurrencyModifications: Int
    var previousConcurrencyModifications: Int
    let simulateOnInsertItem: Bool
    let simulateOnUpdateItem: Bool

    /// Initializer.
    ///
    /// - Parameters:
    ///    - wrappedDynamoDBTable: The underlying DynamoDBTable used by this implementation.
    ///    - simulateConcurrencyModifications: the number of get requests to simulate concurrency for.
    ///    - simulateOnInsertItem: if this instance should simulate concurrency on insertItem.
    ///    - simulateOnUpdateItem: if this instance should simulate concurrency on updateItem.
    public init(
        wrappedDynamoDBTable: Wrapped,
        simulateConcurrencyModifications: Int,
        simulateOnInsertItem: Bool = true,
        simulateOnUpdateItem: Bool = true
    ) {
        self.wrappedDynamoDBTable = wrappedDynamoDBTable
        self.simulateConcurrencyModifications = simulateConcurrencyModifications
        self.previousConcurrencyModifications = 0
        self.simulateOnInsertItem = simulateOnInsertItem
        self.simulateOnUpdateItem = simulateOnUpdateItem
    }

    public func validateEntry(entry: WriteEntry<some Any, some Any, some Any>) throws {
        let entryString = "\(entry)"
        if entryString.count > maxStatementLength {
            throw DynamoDBTableError.statementLengthExceeded(
                reason:
                    "failed to satisfy constraint: Member must have length less than or equal to \(maxStatementLength). Actual length \(entryString.count)"
            )
        }
    }

    public func insertItem(_ item: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
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

    public func clobberItem(_ item: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
        try await self.wrappedDynamoDBTable.clobberItem(item)
    }

    public func updateItem<AttributesType, ItemType, TimeToLiveAttributesType>(
        newItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        existingItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>
    ) async throws {
        // if there are still modifications to be made and there is an existing row
        if self.simulateOnUpdateItem, self.previousConcurrencyModifications < self.simulateConcurrencyModifications {
            try await self.wrappedDynamoDBTable.updateItem(
                newItem: existingItem.createUpdatedItem(withValue: existingItem.rowValue),
                existingItem: existingItem
            )

            self.previousConcurrencyModifications += 1

            // then delegate to the wrapped implementation
            try await self.wrappedDynamoDBTable.updateItem(newItem: newItem, existingItem: existingItem)
        }

        // otherwise just delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.updateItem(newItem: newItem, existingItem: existingItem)
    }

    public func transactWrite(_ entries: [WriteEntry<some Any, some Any, some Any>]) async throws {
        if self.previousConcurrencyModifications < self.simulateConcurrencyModifications {
            simulateLoop: for entry in entries {
                switch entry {
                case let .update(new: _, existing: existing) where self.simulateOnUpdateItem:
                    try await self.wrappedDynamoDBTable.updateItem(
                        newItem: existing.createUpdatedItem(withValue: existing.rowValue),
                        existingItem: existing
                    )
                    self.previousConcurrencyModifications += 1
                    break simulateLoop
                case let .insert(new: new) where self.simulateOnInsertItem:
                    try await self.wrappedDynamoDBTable.insertItem(new)
                    self.previousConcurrencyModifications += 1
                    break simulateLoop
                default:
                    continue
                }
            }
        }
        try await self.wrappedDynamoDBTable.transactWrite(entries)
    }

    public func transactWrite<AttributesType, ItemType, TimeToLiveAttributesType>(
        _ entries: [WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>],
        constraints: [TransactionConstraintEntry<AttributesType, ItemType, TimeToLiveAttributesType>]
    ) async throws {
        if self.previousConcurrencyModifications < self.simulateConcurrencyModifications {
            simulateLoop: for entry in entries {
                switch entry {
                case let .update(new: _, existing: existing) where self.simulateOnUpdateItem:
                    try await self.wrappedDynamoDBTable.updateItem(
                        newItem: existing.createUpdatedItem(withValue: existing.rowValue),
                        existingItem: existing
                    )
                    self.previousConcurrencyModifications += 1
                    break simulateLoop
                case let .insert(new: new) where self.simulateOnInsertItem:
                    try await self.wrappedDynamoDBTable.insertItem(new)
                    self.previousConcurrencyModifications += 1
                    break simulateLoop
                default:
                    continue
                }
            }
        }
        try await self.wrappedDynamoDBTable.transactWrite(entries, constraints: constraints)
    }

    public func polymorphicTransactWrite(_ entries: [some PolymorphicWriteEntry]) async throws {
        try await self.wrappedDynamoDBTable.polymorphicTransactWrite(entries)
    }

    public func polymorphicTransactWrite<
        WriteEntryType: PolymorphicWriteEntry,
        TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry
    >(
        _ entries: [WriteEntryType],
        constraints: [TransactionConstraintEntryType]
    ) async throws
    where WriteEntryType.AttributesType == TransactionConstraintEntryType.AttributesType {
        try await self.wrappedDynamoDBTable.polymorphicTransactWrite(entries, constraints: constraints)
    }

    public func polymorphicBulkWrite(_ entries: [some PolymorphicWriteEntry]) async throws {
        try await self.wrappedDynamoDBTable.polymorphicBulkWrite(entries)
    }

    public func bulkWrite(_ entries: [WriteEntry<some Any, some Any, some Any>]) async throws {
        try await entries.asyncForEach { entry in
            switch entry {
            case let .update(new: new, existing: existing):
                try await self.updateItem(newItem: new, existingItem: existing)
            case let .insert(new: new):
                try await self.insertItem(new)
            case let .deleteAtKey(key: key):
                try await self.deleteItem(forKey: key)
            case let .deleteItem(existing: existing):
                try await self.deleteItem(existingItem: existing)
            }
        }
    }

    public func bulkWriteWithFallback(_ entries: [WriteEntry<some Any, some Any, some Any>]) async throws {
        try await self.wrappedDynamoDBTable.bulkWriteWithFallback(entries)
    }

    public func getItem<AttributesType, ItemType, TimeToLiveAttributesType>(
        forKey key: CompositePrimaryKey<AttributesType>
    ) async throws
        -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>?
    {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.getItem(forKey: key)
    }

    public func polymorphicGetItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>]
    ) async throws
        -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]
    {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.polymorphicGetItems(forKeys: keys)
    }

    public func deleteItem(forKey key: CompositePrimaryKey<some Any>) async throws {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.deleteItem(forKey: key)
    }

    public func deleteItem(existingItem: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
        try await self.wrappedDynamoDBTable.deleteItem(existingItem: existingItem)
    }

    public func deleteItems(forKeys keys: [CompositePrimaryKey<some Any>]) async throws {
        try await self.wrappedDynamoDBTable.deleteItems(forKeys: keys)
    }

    public func deleteItems(existingItems: [TypedTTLDatabaseItem<some Any, some Any, some Any>]) async throws {
        try await self.wrappedDynamoDBTable.deleteItems(existingItems: existingItems)
    }

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?
    ) async throws
        -> [ReturnedType]
    {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.polymorphicQuery(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition
        )
    }

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        exclusiveStartKey: String?
    ) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.polymorphicQuery(
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
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.polymorphicQuery(
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
    ) async throws
        -> [ReturnedType]
    {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.polymorphicExecute(
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
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.polymorphicExecute(
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
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.getItems(forKeys: keys)
    }

    public func execute<AttributesType, ItemType, TimeToLiveAttributesType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?
    ) async throws
        -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.execute(
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
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.execute(
            partitionKeys: partitionKeys,
            attributesFilter: attributesFilter,
            additionalWhereClause: additionalWhereClause,
            nextToken: nextToken
        )
    }

    public func query<AttributesType, ItemType, TimeToLiveAttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?
    ) async throws
        -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.query(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition
        )
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
        // simply delegate to the wrapped implementation
        try await self.wrappedDynamoDBTable.query(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition,
            limit: limit,
            scanIndexForward: scanIndexForward,
            exclusiveStartKey: exclusiveStartKey
        )
    }
}
