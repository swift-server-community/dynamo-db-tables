//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/DynamoDBCompositePrimaryKeyTable+conditionallyUpdateItem.swift
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
//  DynamoDBCompositePrimaryKeyTable+conditionallyUpdateItem.swift
//  DynamoDBTables
//

import AWSDynamoDB
import Foundation
import Logging

public extension DynamoDBCompositePrimaryKeyTable {
    /**
     Method to conditionally update an item at the specified key for a number of retries.
     This method is useful for database rows that may be updated simultaneously by different clients
     and each client will only attempt to update based on the current row value.
     On each attempt, the updatedPayloadProvider will be passed the current row value. It can either
     generate an updated payload or fail with an error if an updated payload is not valid. If an updated
     payload is returned, this method will attempt to update the row. This update may fail due to
     concurrency, in which case the process will repeat until the retry limit has been reached.

     - Parameters:
         _: the key of the item to update
         withRetries: the number of times to attempt to retry the update before failing.
         updatedPayloadProvider: the provider that will return updated payloads.
     */
    func conditionallyUpdateItem<AttributesType, ItemType: Codable, TimeToLiveAttributesType: TimeToLiveAttributes>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        timeToLiveAttributesType _: TimeToLiveAttributesType.Type = StandardTimeToLiveAttributes.self,
        updatedPayloadProvider: @escaping (ItemType) async throws -> ItemType) async throws
    {
        let updatedItemProvider: (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) async throws
            -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType> = { existingItem in
                let updatedPayload = try await updatedPayloadProvider(existingItem.rowValue)
                return existingItem.createUpdatedItem(withValue: updatedPayload)
            }
        try await self.conditionallyUpdateItemInternal(
            forKey: key,
            withRetries: retries,
            updatedItemProvider: updatedItemProvider)
    }

    // Explicitly specify an overload with sync updatedPayloadProvider
    // to avoid the compiler matching a call site with such a provider with the EventLoopFuture-returning overload.
    func conditionallyUpdateItem<AttributesType, ItemType: Codable, TimeToLiveAttributesType: TimeToLiveAttributes>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        timeToLiveAttributesType _: TimeToLiveAttributesType.Type = StandardTimeToLiveAttributes.self,
        updatedPayloadProvider: @escaping (ItemType) throws -> ItemType) async throws
    {
        let updatedItemProvider: (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) async throws
            -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType> = { existingItem in
                let updatedPayload = try updatedPayloadProvider(existingItem.rowValue)
                return existingItem.createUpdatedItem(withValue: updatedPayload)
            }
        try await self.conditionallyUpdateItemInternal(
            forKey: key,
            withRetries: retries,
            updatedItemProvider: updatedItemProvider)
    }

    /**
     Method to conditionally update an item at the specified key for a number of retries.
     This method is useful for database rows that may be updated simultaneously by different clients
     and each client will only attempt to update based on the current row value.
     On each attempt, the updatedItemProvider will be passed the current row. It can either
     generate an updated row or fail with an error if an updated row is not valid. If an updated
     row is returned, this method will attempt to update the row. This update may fail due to
     concurrency, in which case the process will repeat until the retry limit has been reached.

     - Parameters:
         _: the key of the item to update
         withRetries: the number of times to attempt to retry the update before failing.
         updatedItemProvider: the provider that will return updated items.
     */
    func conditionallyUpdateItem<AttributesType, ItemType: Codable, TimeToLiveAttributesType: TimeToLiveAttributes>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        updatedItemProvider: @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) async throws
            -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) async throws
    {
        try await self.conditionallyUpdateItemInternal(
            forKey: key,
            withRetries: retries,
            updatedItemProvider: updatedItemProvider)
    }

    // Explicitly specify an overload with sync updatedItemProvider
    // to avoid the compiler matching a call site with such a provider with the EventLoopFuture-returning overload.
    func conditionallyUpdateItem<AttributesType, ItemType: Codable, TimeToLiveAttributesType: TimeToLiveAttributes>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        updatedItemProvider: @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) throws
            -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) async throws
    {
        try await self.conditionallyUpdateItemInternal(
            forKey: key,
            withRetries: retries,
            updatedItemProvider: updatedItemProvider)
    }

    private func conditionallyUpdateItemInternal<AttributesType, ItemType: Codable, TimeToLiveAttributesType: TimeToLiveAttributes>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        updatedItemProvider: @escaping (TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) async throws
            -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) async throws
    {
        guard retries > 0 else {
            throw DynamoDBTableError.concurrencyError(partitionKey: key.partitionKey,
                                                      sortKey: key.sortKey,
                                                      message: "Unable to complete request to update versioned item in specified number of attempts")
        }

        let databaseItemOptional: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>? = try await getItem(forKey: key)

        guard let databaseItem = databaseItemOptional else {
            throw DynamoDBTableError.conditionalCheckFailed(partitionKey: key.partitionKey,
                                                            sortKey: key.sortKey,
                                                            message: "Item not present in database.")
        }

        let updatedDatabaseItem = try await updatedItemProvider(databaseItem)

        do {
            try await self.updateItem(newItem: updatedDatabaseItem, existingItem: databaseItem)
        } catch DynamoDBTableError.conditionalCheckFailed {
            // try again
            return try await self.conditionallyUpdateItem(forKey: key,
                                                          withRetries: retries - 1,
                                                          updatedItemProvider: updatedItemProvider)
        }
    }
}
