//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
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
//  DynamoDBCompositePrimaryKeyTable+retryingUpsertItem.swift
//  DynamoDBTables
//

import AWSDynamoDB
import Foundation
import Logging

extension DynamoDBCompositePrimaryKeyTable {
    /**
     * This operation will attempt to insert or update the primary item, repeatedly retrying
     * on concurrency errors until the appropriate `insert` or `update` operation succeeds.
     * The `newItemProvider` is called when the item does not yet exist. The `updatedItemProvider`
     * is called with the existing item when it already exists.
     *
     * - Parameters:
     *   - forKey: the composite primary key of the item to upsert.
     *   - withRetries: the number of times to attempt to retry the upsert before failing.
     *   - newItemProvider: provider called to create a new item when none exists.
     *   - updatedItemProvider: provider called with the existing item to produce an updated item.
     * - Returns: the item that was written to the database.
     */
    @discardableResult
    public func retryingUpsertItem<
        AttributesType,
        ItemType: Codable,
        TimeToLiveAttributesType: TimeToLiveAttributes
    >(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        newItemProvider:
            @escaping () async throws
            -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        updatedItemProvider:
            @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>)
            async throws
            -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>
    ) async throws -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType> {
        guard retries > 0 else {
            throw DynamoDBTableError.concurrencyError(
                partitionKey: key.partitionKey,
                sortKey: key.sortKey,
                message: "Unable to complete request to upsert item in specified number of attempts"
            )
        }

        let existingItemOptional: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>? =
            try await getItem(forKey: key)

        do {
            if let existingItem = existingItemOptional {
                let updatedItem = try await updatedItemProvider(existingItem)

                try await self.updateItem(newItem: updatedItem, existingItem: existingItem)

                return updatedItem
            } else {
                let newItem = try await newItemProvider()

                try await self.insertItem(newItem)

                return newItem
            }
        } catch DynamoDBTableError.conditionalCheckFailed {
            return try await self.retryingUpsertItem(
                forKey: key,
                withRetries: retries - 1,
                newItemProvider: newItemProvider,
                updatedItemProvider: updatedItemProvider
            )
        }
    }
}
