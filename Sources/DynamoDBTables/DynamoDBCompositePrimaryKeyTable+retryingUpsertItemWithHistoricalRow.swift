//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/DynamoDBCompositePrimaryKeyTableHistoricalItemExtensions
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
//  DynamoDBCompositePrimaryKeyTable+retryingUpsertItemWithHistoricalRow.swift
//  DynamoDBTables
//

extension DynamoDBCompositePrimaryKeyTable {
    /**
     * This operation will attempt to insert or update the primary item, repeatedly retrying
     * on concurrency errors until the appropriate `insert` or `update` operation succeeds.
     * The `newItemProvider` is called when the item does not yet exist. The `updatedItemProvider`
     * is called with the existing item when it already exists. Once this operation has succeeded,
     * the `historicalItemProvider` is called to provide the historical item based on the primary
     * item that was written to the database table.
     *
     * - Parameters:
     *   - forKey: the composite primary key of the item to upsert.
     *   - withRetries: the number of times to attempt to retry the upsert before failing.
     *   - newItemProvider: provider called to create a new item when none exists.
     *   - updatedItemProvider: provider called with the existing item to produce an updated item.
     *   - historicalItemProvider: provider called with the written item to produce a historical record.
     * - Returns: the item that was written to the database.
     */
    @discardableResult
    public func retryingUpsertItemWithHistoricalRow<AttributesType, ItemType, TimeToLiveAttributesType>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        newItemProvider:
            @escaping () async throws
            -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        updatedItemProvider:
            @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>)
            async throws
            -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        historicalItemProvider:
            @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>)
            -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>
    ) async throws -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType> {
        guard retries > 0 else {
            throw DynamoDBTableError.concurrencyError(
                partitionKey: key.partitionKey,
                sortKey: key.sortKey,
                message: "Unable to complete request to upsert versioned item in specified number of attempts"
            )
        }

        let existingItemOptional: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>? =
            try await getItem(forKey: key)

        do {
            if let existingItem = existingItemOptional {
                let updatedItem = try await updatedItemProvider(existingItem)

                try await self.updateItemWithHistoricalRow(
                    primaryItem: updatedItem,
                    existingItem: existingItem,
                    historicalItem: historicalItemProvider(updatedItem)
                )

                return updatedItem
            } else {
                let newItem = try await newItemProvider()

                try await self.insertItemWithHistoricalRow(
                    primaryItem: newItem,
                    historicalItem: historicalItemProvider(newItem)
                )

                return newItem
            }
        } catch DynamoDBTableError.transactionCanceled {
            return try await self.retryingUpsertItemWithHistoricalRow(
                forKey: key,
                withRetries: retries - 1,
                newItemProvider: newItemProvider,
                updatedItemProvider: updatedItemProvider,
                historicalItemProvider: historicalItemProvider
            )
        }
    }
}
