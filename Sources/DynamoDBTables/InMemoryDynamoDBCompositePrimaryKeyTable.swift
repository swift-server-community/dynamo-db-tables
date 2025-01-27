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

public extension TypedTTLDatabaseItem {
    var rowTypeIdentifier: String {
        getTypeRowIdentifier(type: RowType.self)
    }
}

public typealias ExecuteItemFilterType = @Sendable (String, String, String, InMemoryDatabaseItem)
    -> Bool

public protocol InMemoryTransactionDelegate: Sendable {
    /**
      Inject errors into a `transactWrite` or `polymorphicTransactWrite` call.
     */
    func injectErrors<AttributesType>(
        inputKeys: [CompositePrimaryKey<AttributesType>?], table: InMemoryDynamoDBCompositePrimaryKeyTable) async throws -> [DynamoDBTableError]
}

public struct InMemoryDynamoDBCompositePrimaryKeyTable: DynamoDBCompositePrimaryKeyTable, Sendable {
    public let escapeSingleQuoteInPartiQL: Bool
    public let transactionDelegate: InMemoryTransactionDelegate?
    public let executeItemFilter: ExecuteItemFilterType?
    let storeWrapper: InMemoryDynamoDBCompositePrimaryKeyTableStore

    public init(executeItemFilter: ExecuteItemFilterType? = nil,
                escapeSingleQuoteInPartiQL: Bool = false,
                transactionDelegate: InMemoryTransactionDelegate? = nil)
    {
        self.storeWrapper = InMemoryDynamoDBCompositePrimaryKeyTableStore()
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.transactionDelegate = transactionDelegate
        self.executeItemFilter = executeItemFilter
    }

    public var store: [String: [String: InMemoryDatabaseItem]] {
        get async {
            await self.storeWrapper.store
        }
    }

    public func validateEntry(entry: WriteEntry<some Any, some Any, some Any>) throws {
        let entryString = "\(entry)"
        if entryString.count > AWSDynamoDBLimits.maxStatementLength {
            throw DynamoDBTableError.statementLengthExceeded(
                reason: "failed to satisfy constraint: Member must have length less than or equal to "
                    + "\(AWSDynamoDBLimits.maxStatementLength). Actual length \(entryString.count)")
        }
    }

    public func insertItem(_ item: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
        let inMemoryDatabaseItem = try item.inMemoryFormWithKey()
        try await self.storeWrapper.execute { store in
            try self.insertItem(inMemoryDatabaseItem, store: &store)
        }
    }

    public func clobberItem(_ item: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
        let inMemoryDatabaseItem = try item.inMemoryForm()
        let compositePrimaryKey = item.compositePrimaryKey

        await self.storeWrapper.execute { store in
            let partition = store[compositePrimaryKey.partitionKey]

            // if there is already a partition
            var updatedPartition: [String: InMemoryDatabaseItem]
            if let partition {
                updatedPartition = partition

                updatedPartition[compositePrimaryKey.sortKey] = inMemoryDatabaseItem
            } else {
                updatedPartition = [compositePrimaryKey.sortKey: inMemoryDatabaseItem]
            }

            store[compositePrimaryKey.partitionKey] = updatedPartition
        }
    }

    public func updateItem<AttributesType, ItemType, TimeToLiveAttributesType>(
        newItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        existingItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) async throws
    {
        let inMemoryDatabaseItem = try newItem.inMemoryForm()
        let existingItemMetadata = existingItem.asMetadataWithKey()
        try await self.storeWrapper.execute { store in
            try self.updateItem(newItem: inMemoryDatabaseItem, existingItemMetadata: existingItemMetadata, store: &store)
        }
    }

    public func getItem<AttributesType, ItemType, TimeToLiveAttributesType>(forKey key: CompositePrimaryKey<AttributesType>) async throws
        -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>?
    {
        if let partition = await self.store[key.partitionKey] {
            guard let value = partition[key.sortKey] else {
                return nil
            }

            return try value.getItem()
        }

        return nil
    }

    public func deleteItem(forKey key: CompositePrimaryKey<some Any>) async throws {
        try await self.storeWrapper.execute { store in
            try self.deleteItem(forKey: key, store: &store)
        }
    }

    public func deleteItems(forKeys keys: [CompositePrimaryKey<some Any>]) async throws {
        try await self.storeWrapper.execute { store in
            try keys.forEach { key in
                try self.deleteItem(forKey: key, store: &store)
            }
        }
    }

    public func deleteItems(existingItems: [TypedTTLDatabaseItem<some Any, some Any, some Any>]) async throws {
        let itemMetadataList = existingItems.map { $0.asMetadataWithKey() }
        try await self.storeWrapper.execute { store in
            try itemMetadataList.forEach { itemMetadata in
                try self.deleteItem(itemMetadata: itemMetadata, store: &store)
            }
        }
    }

    public func deleteItem(existingItem: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
        let itemMetadata = existingItem.asMetadataWithKey()
        try await self.storeWrapper.execute { store in
            try self.deleteItem(itemMetadata: itemMetadata, store: &store)
        }
    }
}
