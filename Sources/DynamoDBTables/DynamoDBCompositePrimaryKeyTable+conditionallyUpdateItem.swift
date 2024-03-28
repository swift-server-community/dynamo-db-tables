//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from https://github.com/amzn/smoke-dynamodb. Any commits
// prior to February 2024
// Copyright 2018-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
//  DynamoDBCompositePrimaryKeyTable+conditionallyUpdateItem.swift
//  DynamoDBTables
//

import Foundation
import SmokeHTTPClient
import Logging
import DynamoDBModel
import NIO

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
    func conditionallyUpdateItem<AttributesType, ItemType: Codable>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        updatedPayloadProvider: @escaping (ItemType) throws -> ItemType) -> EventLoopFuture<Void> {
            let updatedItemProvider: (TypedDatabaseItem<AttributesType, ItemType>) throws -> TypedDatabaseItem<AttributesType, ItemType> = { existingItem in
                let updatedPayload = try updatedPayloadProvider(existingItem.rowValue)
                return existingItem.createUpdatedItem(withValue: updatedPayload)
            }
            return self.conditionallyUpdateItem(
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
    func conditionallyUpdateItem<AttributesType, ItemType: Codable>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        updatedItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) throws -> TypedDatabaseItem<AttributesType, ItemType>)
    -> EventLoopFuture<Void> {
        guard retries > 0 else {
            let error = DynamoDBTableError.concurrencyError(partitionKey: key.partitionKey,
                                                            sortKey: key.sortKey,
                                                            message: "Unable to complete request to update versioned item in specified number of attempts")
            return self.eventLoop.makeFailedFuture(error)
        }
        
        return getItem(forKey: key).flatMap { (databaseItemOptional: TypedDatabaseItem<AttributesType, ItemType>?) in
            guard let databaseItem = databaseItemOptional else {
                let error = DynamoDBTableError.conditionalCheckFailed(partitionKey: key.partitionKey,
                                                                      sortKey: key.sortKey,
                                                                      message: "Item not present in database.")
                return self.eventLoop.makeFailedFuture(error)
            }
            
            let updatedDatabaseItem: TypedDatabaseItem<AttributesType, ItemType>
            
            do {
                updatedDatabaseItem = try updatedItemProvider(databaseItem)
            } catch {
                return self.eventLoop.makeFailedFuture(error)
            }
            
            return self.updateItem(newItem: updatedDatabaseItem, existingItem: databaseItem).flatMapError { error in
                if case DynamoDBTableError.conditionalCheckFailed = error {
                    // try again
                    return self.conditionallyUpdateItem(forKey: key,
                                                        withRetries: retries - 1,
                                                        updatedItemProvider: updatedItemProvider)
                } else {
                    // propagate the error as it's not an error causing a retry
                    return self.eventLoop.makeFailedFuture(error)
                }
            }
        }
    }
    
#if (os(Linux) && compiler(>=5.5)) || (!os(Linux) && compiler(>=5.5.2)) && canImport(_Concurrency)
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
    func conditionallyUpdateItem<AttributesType, ItemType: Codable>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        updatedPayloadProvider: @escaping (ItemType) async throws -> ItemType) async throws {
            let updatedItemProvider: (TypedDatabaseItem<AttributesType, ItemType>) async throws -> TypedDatabaseItem<AttributesType, ItemType> = { existingItem in
                let updatedPayload = try await updatedPayloadProvider(existingItem.rowValue)
                return existingItem.createUpdatedItem(withValue: updatedPayload)
            }
            try await conditionallyUpdateItemInternal(
                forKey: key,
                withRetries: retries,
                updatedItemProvider: updatedItemProvider)
        }
    
    // Explicitly specify an overload with sync updatedPayloadProvider
    // to avoid the compiler matching a call site with such a provider with the EventLoopFuture-returning overload.
    func conditionallyUpdateItem<AttributesType, ItemType: Codable>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        updatedPayloadProvider: @escaping (ItemType) throws -> ItemType) async throws {
            let updatedItemProvider: (TypedDatabaseItem<AttributesType, ItemType>) throws -> TypedDatabaseItem<AttributesType, ItemType> = { existingItem in
                let updatedPayload = try updatedPayloadProvider(existingItem.rowValue)
                return existingItem.createUpdatedItem(withValue: updatedPayload)
            }
            try await conditionallyUpdateItemInternal(
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
    func conditionallyUpdateItem<AttributesType, ItemType: Codable>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        updatedItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) async throws -> TypedDatabaseItem<AttributesType, ItemType>) async throws {
            try await conditionallyUpdateItemInternal(
                forKey: key,
                withRetries: retries,
                updatedItemProvider: updatedItemProvider)
        }
    
    // Explicitly specify an overload with sync updatedItemProvider
    // to avoid the compiler matching a call site with such a provider with the EventLoopFuture-returning overload.
    func conditionallyUpdateItem<AttributesType, ItemType: Codable>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        updatedItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) throws -> TypedDatabaseItem<AttributesType, ItemType>) async throws {
            try await conditionallyUpdateItemInternal(
                forKey: key,
                withRetries: retries,
                updatedItemProvider: updatedItemProvider)
        }
    
    private func conditionallyUpdateItemInternal<AttributesType, ItemType: Codable>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        updatedItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) async throws -> TypedDatabaseItem<AttributesType, ItemType>) async throws {
            guard retries > 0 else {
                throw DynamoDBTableError.concurrencyError(partitionKey: key.partitionKey,
                                                          sortKey: key.sortKey,
                                                          message: "Unable to complete request to update versioned item in specified number of attempts")
            }
            
            let databaseItemOptional: TypedDatabaseItem<AttributesType, ItemType>? = try await getItem(forKey: key)
            
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
#endif
    
    func conditionallyUpdateItem<AttributesType, ItemType: Codable>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        updatedPayloadProvider: @escaping (ItemType) -> EventLoopFuture<ItemType>) -> EventLoopFuture<Void> {
            let updatedItemProvider: (TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>> = { existingItem in
                return updatedPayloadProvider(existingItem.rowValue).map { updatedPayload in
                    return existingItem.createUpdatedItem(withValue: updatedPayload)
                }
            }
            return self.conditionallyUpdateItem(
                forKey: key,
                withRetries: retries,
                updatedItemProvider: updatedItemProvider)
        }
    
    func conditionallyUpdateItem<AttributesType, ItemType: Codable>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        updatedItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>>)
    -> EventLoopFuture<Void> {
        guard retries > 0 else {
            let error = DynamoDBTableError.concurrencyError(partitionKey: key.partitionKey,
                                                            sortKey: key.sortKey,
                                                            message: "Unable to complete request to update versioned item in specified number of attempts")
            return self.eventLoop.makeFailedFuture(error)
        }
        
        return getItem(forKey: key).flatMap { (databaseItemOptional: TypedDatabaseItem<AttributesType, ItemType>?) in
            guard let databaseItem = databaseItemOptional else {
                let error = DynamoDBTableError.conditionalCheckFailed(partitionKey: key.partitionKey,
                                                                      sortKey: key.sortKey,
                                                                      message: "Item not present in database.")
                return self.eventLoop.makeFailedFuture(error)
            }
            
            return updatedItemProvider(databaseItem).flatMap { updatedDatabaseItem in
                return self.updateItem(newItem: updatedDatabaseItem, existingItem: databaseItem).flatMapError { error in
                    if case DynamoDBTableError.conditionalCheckFailed = error {
                        // try again
                        return self.conditionallyUpdateItem(forKey: key,
                                                            withRetries: retries - 1,
                                                            updatedItemProvider: updatedItemProvider)
                    } else {
                        // propagate the error as it's not an error causing a retry
                        return self.eventLoop.makeFailedFuture(error)
                    }
                }
            }
        }
    }
}
