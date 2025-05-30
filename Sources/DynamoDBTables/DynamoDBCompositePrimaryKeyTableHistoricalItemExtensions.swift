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
//  DynamoDBCompositePrimaryKeyTableHistoricalItemExtensions.swift
//      Extensions which enable historical item multi-row update usecases.
//  DynamoDBTables
//

import AWSDynamoDB
import Foundation
import Logging

public extension DynamoDBCompositePrimaryKeyTable {
    /**
     * Historical items exist across multiple rows. This method provides an interface to record all
     * rows in a single call.
     */
    func insertItemWithHistoricalRow<AttributesType, ItemType, TimeToLiveAttributesType>(
        primaryItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        historicalItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) async throws
    {
        try await insertItem(primaryItem)
        try await insertItem(historicalItem)
    }

    func updateItemWithHistoricalRow<AttributesType, ItemType, TimeToLiveAttributesType>(
        primaryItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        existingItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        historicalItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) async throws
    {
        try await updateItem(newItem: primaryItem, existingItem: existingItem)
        try await insertItem(historicalItem)
    }

    /**
     * This operation will attempt to update the primary item, repeatedly calling the
     * `primaryItemProvider` to retrieve an updated version of the current row (if it
     * exists) until the appropriate `insert` or  `update` operation succeeds. Once this
     * operation has succeeded, the `historicalItemProvider` is called to provide
     * the historical item based on the primary item that was inserted into the
     * database table. The primary item may not exist in the database table to
     * begin with.
     *
     * Clobbering a historical item requires knowledge of existing rows to accurately record
     * historical data.
     */
    func clobberItemWithHistoricalRow<AttributesType, ItemType, TimeToLiveAttributesType>(
        primaryItemProvider: @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>?) ->
            TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        historicalItemProvider: @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) ->
            TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        withRetries retries: Int = 10) async throws
    {
        let primaryItem = primaryItemProvider(nil)

        guard retries > 0 else {
            throw DynamoDBTableError.concurrencyError(partitionKey: primaryItem.compositePrimaryKey.partitionKey,
                                                      sortKey: primaryItem.compositePrimaryKey.sortKey,
                                                      message: "Unable to complete request to clobber versioned item in specified number of attempts")
        }

        let existingItemOptional: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>? =
            try await getItem(forKey: primaryItem.compositePrimaryKey)

        if let existingItem = existingItemOptional {
            let newItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType> = primaryItemProvider(existingItem)

            do {
                try await self.updateItemWithHistoricalRow(primaryItem: newItem, existingItem: existingItem,
                                                           historicalItem: historicalItemProvider(newItem))
            } catch {
                try await self.clobberItemWithHistoricalRow(primaryItemProvider: primaryItemProvider,
                                                            historicalItemProvider: historicalItemProvider,
                                                            withRetries: retries - 1)
                return
            }
        } else {
            do {
                try await self.insertItemWithHistoricalRow(primaryItem: primaryItem,
                                                           historicalItem: historicalItemProvider(primaryItem))
            } catch {
                try await self.clobberItemWithHistoricalRow(primaryItemProvider: primaryItemProvider,
                                                            historicalItemProvider: historicalItemProvider,
                                                            withRetries: retries - 1)
                return
            }
        }
    }

    /**
      Operations will attempt to update the primary item, repeatedly calling the
      `primaryItemProvider` to retrieve an updated version of the current row
      until the appropriate  `update` operation succeeds. The
      `primaryItemProvider` can thrown an exception to indicate that the current
      row is unable to be updated. The `historicalItemProvider` is called to
      provide the historical item based on the primary item that was
      inserted into the database table.

     - Parameters:
        - compositePrimaryKey: The composite key for the version to update.
        - primaryItemProvider: Function to provide the updated item or throw if the current item can't be updated.
        - historicalItemProvider: Function to provide the historical item for the primary item.
     */
    func conditionallyUpdateItemWithHistoricalRow<AttributesType, ItemType, TimeToLiveAttributesType>(
        compositePrimaryKey: CompositePrimaryKey<AttributesType>,
        primaryItemProvider: @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) async throws ->
            TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        historicalItemProvider: @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) ->
            TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        withRetries retries: Int = 10) async throws -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>
    {
        try await self.conditionallyUpdateItemWithHistoricalRowInternal(
            compositePrimaryKey: compositePrimaryKey,
            primaryItemProvider: primaryItemProvider,
            historicalItemProvider: historicalItemProvider,
            withRetries: retries)
    }

    // Explicitly specify an overload with sync updatedPayloadProvider
    // to avoid the compiler matching a call site with such a provider with the EventLoopFuture-returning overload.
    func conditionallyUpdateItemWithHistoricalRow<AttributesType, ItemType, TimeToLiveAttributesType>(
        compositePrimaryKey: CompositePrimaryKey<AttributesType>,
        primaryItemProvider: @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) throws ->
            TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        historicalItemProvider: @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) ->
            TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        withRetries retries: Int = 10) async throws -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>
    {
        try await self.conditionallyUpdateItemWithHistoricalRowInternal(
            compositePrimaryKey: compositePrimaryKey,
            primaryItemProvider: primaryItemProvider,
            historicalItemProvider: historicalItemProvider,
            withRetries: retries)
    }

    private func conditionallyUpdateItemWithHistoricalRowInternal<AttributesType, ItemType, TimeToLiveAttributesType>(
        compositePrimaryKey: CompositePrimaryKey<AttributesType>,
        primaryItemProvider: @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) async throws ->
            TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        historicalItemProvider: @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) ->
            TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        withRetries retries: Int = 10) async throws -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>
    {
        guard retries > 0 else {
            throw DynamoDBTableError.concurrencyError(partitionKey: compositePrimaryKey.partitionKey,
                                                      sortKey: compositePrimaryKey.sortKey,
                                                      message: "Unable to complete request to update versioned item in specified number of attempts")
        }

        let existingItemOptional: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>? =
            try await getItem(forKey: compositePrimaryKey)

        guard let existingItem = existingItemOptional else {
            throw DynamoDBTableError.conditionalCheckFailed(partitionKey: compositePrimaryKey.partitionKey,
                                                            sortKey: compositePrimaryKey.sortKey,
                                                            message: "Item not present in database.")
        }

        let updatedItem = try await primaryItemProvider(existingItem)
        let historicalItem = historicalItemProvider(updatedItem)

        do {
            try await self.updateItemWithHistoricalRow(primaryItem: updatedItem,
                                                       existingItem: existingItem,
                                                       historicalItem: historicalItem)
        } catch DynamoDBTableError.conditionalCheckFailed {
            // try again
            return try await self.conditionallyUpdateItemWithHistoricalRow(compositePrimaryKey: compositePrimaryKey,
                                                                           primaryItemProvider: primaryItemProvider,
                                                                           historicalItemProvider: historicalItemProvider, withRetries: retries - 1)
        }

        return updatedItem
    }
}
