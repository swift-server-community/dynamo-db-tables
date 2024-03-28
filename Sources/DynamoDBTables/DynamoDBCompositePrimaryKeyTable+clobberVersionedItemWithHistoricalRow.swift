//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from https://github.com/amzn/smoke-dynamodb. Any commits
// prior to February 2024
// Copyright (c) 2021-2021 the DynamoDBTables authors
// Licensed under Apache License v2.0
//
// Subsequent commits
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
//  DynamoDBCompositePrimaryKeyTable+clobberVersionedItemWithHistoricalRow.swift
//  DynamoDBTables
//

import Foundation
import NIO

public extension DynamoDBCompositePrimaryKeyTable {
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
     - completion: completion handler providing an error that was thrown or nil
     */
    func clobberVersionedItemWithHistoricalRow<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(
        forPrimaryKey partitionKey: String,
        andHistoricalKey historicalKey: String,
        item: ItemType,
        primaryKeyType: AttributesType.Type,
        generateSortKey: @escaping (Int) -> String) -> EventLoopFuture<Void> {
            func primaryItemProvider(_ existingItem: TypedDatabaseItem<AttributesType, RowWithItemVersion<ItemType>>?)
                -> TypedDatabaseItem<AttributesType, RowWithItemVersion<ItemType>> {
                    if let existingItem = existingItem {
                        // If an item already exists, the inserted item should be created
                        // from that item (to get an accurate version number)
                        // with the payload from the default item.
                        let overWrittenItemRowValue = existingItem.rowValue.createUpdatedItem(
                            withVersion: existingItem.rowValue.itemVersion + 1,
                            withValue: item)
                        return existingItem.createUpdatedItem(withValue: overWrittenItemRowValue)
                    }
                    
                    // If there is no existing item to be overwritten, a new item should be constructed.
                    let newItemRowValue = RowWithItemVersion.newItem(withValue: item)
                    let defaultKey = CompositePrimaryKey<AttributesType>(partitionKey: partitionKey, sortKey: generateSortKey(0))
                    return TypedDatabaseItem.newItem(withKey: defaultKey, andValue: newItemRowValue)
            }
        
            func historicalItemProvider(_ primaryItem: TypedDatabaseItem<AttributesType, RowWithItemVersion<ItemType>>)
                -> TypedDatabaseItem<AttributesType, RowWithItemVersion<ItemType>> {
                    let sortKey = generateSortKey(primaryItem.rowValue.itemVersion)
                    let key = CompositePrimaryKey<AttributesType>(partitionKey: historicalKey,
                                                               sortKey: sortKey)
                    return TypedDatabaseItem.newItem(withKey: key, andValue: primaryItem.rowValue)
            }
        
            return clobberItemWithHistoricalRow(primaryItemProvider: primaryItemProvider,
                                                historicalItemProvider: historicalItemProvider)
    }
    
#if (os(Linux) && compiler(>=5.5)) || (!os(Linux) && compiler(>=5.5.2)) && canImport(_Concurrency)
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
     - completion: completion handler providing an error that was thrown or nil
     */
    func clobberVersionedItemWithHistoricalRow<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(
        forPrimaryKey partitionKey: String,
        andHistoricalKey historicalKey: String,
        item: ItemType,
        primaryKeyType: AttributesType.Type,
        generateSortKey: @escaping (Int) -> String) async throws {
            func primaryItemProvider(_ existingItem: TypedDatabaseItem<AttributesType, RowWithItemVersion<ItemType>>?)
                -> TypedDatabaseItem<AttributesType, RowWithItemVersion<ItemType>> {
                    if let existingItem = existingItem {
                        // If an item already exists, the inserted item should be created
                        // from that item (to get an accurate version number)
                        // with the payload from the default item.
                        let overWrittenItemRowValue = existingItem.rowValue.createUpdatedItem(
                            withVersion: existingItem.rowValue.itemVersion + 1,
                            withValue: item)
                        return existingItem.createUpdatedItem(withValue: overWrittenItemRowValue)
                    }
                    
                    // If there is no existing item to be overwritten, a new item should be constructed.
                    let newItemRowValue = RowWithItemVersion.newItem(withValue: item)
                    let defaultKey = CompositePrimaryKey<AttributesType>(partitionKey: partitionKey, sortKey: generateSortKey(0))
                    return TypedDatabaseItem.newItem(withKey: defaultKey, andValue: newItemRowValue)
            }
        
            func historicalItemProvider(_ primaryItem: TypedDatabaseItem<AttributesType, RowWithItemVersion<ItemType>>)
                -> TypedDatabaseItem<AttributesType, RowWithItemVersion<ItemType>> {
                    let sortKey = generateSortKey(primaryItem.rowValue.itemVersion)
                    let key = CompositePrimaryKey<AttributesType>(partitionKey: historicalKey,
                                                               sortKey: sortKey)
                    return TypedDatabaseItem.newItem(withKey: key, andValue: primaryItem.rowValue)
            }
        
            return try await clobberItemWithHistoricalRow(primaryItemProvider: primaryItemProvider,
                                                          historicalItemProvider: historicalItemProvider)
    }
#endif
}
