//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/DynamoDBCompositePrimaryKeyTable+clobberVersionedItemWithHistoricalRow.swift
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
//  DynamoDBCompositePrimaryKeyTable+retryingUpsertVersionedItemWithHistoricalRow.swift
//  DynamoDBTables
//

import Foundation

extension DynamoDBCompositePrimaryKeyTable {
    /**
     * This operation provide a mechanism for managing mutable database rows
     * and storing all previous versions of that row in a historical partition.
     * This operation store the primary item under a "version zero" sort key
     * with a payload that replicates the current version of the row. This
     * historical partition contains rows for each version, including the
     * current version under a sort key for that version.

     - Parameters:
        - partitionKey: the partition key to use for the primary (v0) item
        - historicalKey: the partition key to use for the historical items
        - item: the payload for the new version of the primary item row
        - AttributesType: the row identity type
        - generateSortKey: generator to provide a sort key for a provided
                           version number.
     - Returns: the item that was written to the database.
     */
    @discardableResult
    public func retryingUpsertVersionedItemWithHistoricalRow<
        AttributesType: PrimaryKeyAttributes,
        ItemType: Codable & Sendable,
        TimeToLiveAttributesType: TimeToLiveAttributes
    >(
        forPrimaryKey partitionKey: String,
        andHistoricalKey historicalKey: String,
        item: ItemType,
        primaryKeyType _: AttributesType.Type = StandardPrimaryKeyAttributes.self,
        timeToLiveAttributesType _: TimeToLiveAttributesType.Type = StandardTimeToLiveAttributes.self,
        generateSortKey: @escaping (Int) -> String
    ) async throws -> TypedTTLDatabaseItem<
        AttributesType, RowWithItemVersion<ItemType>, TimeToLiveAttributesType
    > {
        let key = CompositePrimaryKey<AttributesType>(
            partitionKey: partitionKey,
            sortKey: generateSortKey(0)
        )

        func newItemProvider()
            -> TypedTTLDatabaseItem<AttributesType, RowWithItemVersion<ItemType>, TimeToLiveAttributesType>
        {
            let newItemRowValue = RowWithItemVersion.newItem(withValue: item)
            return TypedTTLDatabaseItem.newItem(withKey: key, andValue: newItemRowValue)
        }

        func updatedItemProvider(
            existingItem: TypedTTLDatabaseItem<
                AttributesType, RowWithItemVersion<ItemType>, TimeToLiveAttributesType
            >
        )
            -> TypedTTLDatabaseItem<AttributesType, RowWithItemVersion<ItemType>, TimeToLiveAttributesType>
        {
            let overWrittenItemRowValue = existingItem.rowValue.createUpdatedItem(
                withVersion: existingItem.rowValue.itemVersion + 1,
                withValue: item
            )
            return existingItem.createUpdatedItem(withValue: overWrittenItemRowValue)
        }

        func historicalItemProvider(
            _ primaryItem: TypedTTLDatabaseItem<
                AttributesType, RowWithItemVersion<ItemType>, TimeToLiveAttributesType
            >
        )
            -> TypedTTLDatabaseItem<AttributesType, RowWithItemVersion<ItemType>, TimeToLiveAttributesType>
        {
            let sortKey = generateSortKey(primaryItem.rowValue.itemVersion)
            let historicalKey = CompositePrimaryKey<AttributesType>(
                partitionKey: historicalKey,
                sortKey: sortKey
            )
            return TypedTTLDatabaseItem.newItem(withKey: historicalKey, andValue: primaryItem.rowValue)
        }

        return try await retryingUpsertItemWithHistoricalRow(
            forKey: key,
            newItemProvider: newItemProvider,
            updatedItemProvider: updatedItemProvider,
            historicalItemProvider: historicalItemProvider
        )
    }
}
