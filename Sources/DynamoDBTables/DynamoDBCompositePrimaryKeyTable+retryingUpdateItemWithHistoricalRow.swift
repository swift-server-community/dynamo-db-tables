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
// Copyright (c) 2026 the DynamoDBTables authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of DynamoDBTables authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

//
//  DynamoDBCompositePrimaryKeyTable+retryingUpdateItemWithHistoricalRow.swift
//  DynamoDBTables
//

extension DynamoDBCompositePrimaryKeyTable {
    /**
      Operations will attempt to update the primary item, repeatedly calling the
      `primaryItemProvider` to retrieve an updated version of the current row
      until the appropriate  `update` operation succeeds. The
      `primaryItemProvider` can thrown an exception to indicate that the current
      row is unable to be updated. The `historicalItemProvider` is called to
      provide the historical item based on the primary item that was
      inserted into the database table.
    
     - Parameters:
        - forKey: The composite key for the version to update.
        - primaryItemProvider: Function to provide the updated item or throw if the current item can't be updated.
        - historicalItemProvider: Function to provide the historical item for the primary item.
     - Returns: the updated database item.
     */
    @discardableResult
    public func retryingUpdateItemWithHistoricalRow<AttributesType, ItemType, TimeToLiveAttributesType>(
        forKey key: CompositePrimaryKey<AttributesType>,
        primaryItemProvider:
            @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>)
            async throws ->
            TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        historicalItemProvider:
            @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) ->
            TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        withRetries retries: Int = 10
    ) async throws -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType> {
        try await self.retryingUpdateItemWithHistoricalRowInternal(
            forKey: key,
            primaryItemProvider: primaryItemProvider,
            historicalItemProvider: historicalItemProvider,
            withRetries: retries
        )
    }

    private func retryingUpdateItemWithHistoricalRowInternal<AttributesType, ItemType, TimeToLiveAttributesType>(
        forKey key: CompositePrimaryKey<AttributesType>,
        primaryItemProvider:
            @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>)
            async throws ->
            TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        historicalItemProvider:
            @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) ->
            TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        withRetries retries: Int = 10
    ) async throws -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType> {
        guard retries > 0 else {
            throw DynamoDBTableError.concurrencyError(
                partitionKey: key.partitionKey,
                sortKey: key.sortKey,
                message: "Unable to complete request to update versioned item in specified number of attempts"
            )
        }

        let existingItemOptional: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>? =
            try await getItem(forKey: key)

        guard let existingItem = existingItemOptional else {
            throw DynamoDBTableError.conditionalCheckFailed(
                partitionKey: key.partitionKey,
                sortKey: key.sortKey,
                message: "Item not present in database."
            )
        }

        let updatedItem = try await primaryItemProvider(existingItem)
        let historicalItem = historicalItemProvider(updatedItem)

        do {
            try await self.updateItemWithHistoricalRow(
                primaryItem: updatedItem,
                existingItem: existingItem,
                historicalItem: historicalItem
            )

            return updatedItem
        } catch DynamoDBTableError.transactionCanceled {
            // try again
            return try await self.retryingUpdateItemWithHistoricalRowInternal(
                forKey: key,
                primaryItemProvider: primaryItemProvider,
                historicalItemProvider: historicalItemProvider,
                withRetries: retries - 1
            )
        }
    }
}
