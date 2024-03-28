// swiftlint:disable cyclomatic_complexity
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
//  InMemoryDynamoDBCompositePrimaryKeyTableStore+execute.swift
//  DynamoDBTables
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel
import NIO

extension InMemoryDynamoDBCompositePrimaryKeyTableStore {
    
    func execute<ReturnedType: PolymorphicOperationReturnType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?,
            eventLoop: EventLoop) -> EventLoopFuture<[ReturnedType]> {
        let promise = eventLoop.makePromise(of: [ReturnedType].self)
        
        accessQueue.async {
            let items = self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)
               
            let returnedItems: [ReturnedType]
            do {
                returnedItems = try items.map { item in
                    return try self.convertToQueryableType(input: item)
                }
            } catch {
                promise.fail(error)
                return
            }
            
            promise.succeed(returnedItems)
        }
        
        return promise.futureResult
    }
    
    func execute<ReturnedType: PolymorphicOperationReturnType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?,
            nextToken: String?,
            eventLoop: EventLoop) -> EventLoopFuture<([ReturnedType], String?)> {
        let promise = eventLoop.makePromise(of: ([ReturnedType], String?).self)
        
        accessQueue.async {
            let items = self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)
               
            let returnedItems: [ReturnedType]
            do {
                returnedItems = try items.map { item in
                    return try self.convertToQueryableType(input: item)
                }
            } catch {
                promise.fail(error)
                return
            }
            
            promise.succeed((returnedItems, nil))
        }
        
        return promise.futureResult
    }
    
    func monomorphicExecute<AttributesType, ItemType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?,
            eventLoop: EventLoop)
    -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]> {
        let promise = eventLoop.makePromise(of: [TypedDatabaseItem<AttributesType, ItemType>].self)
        
        accessQueue.async {
            let items = self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)
               
            let returnedItems: [TypedDatabaseItem<AttributesType, ItemType>]
            do {
                returnedItems = try items.map { item in
                    guard let typedItem = item as? TypedDatabaseItem<AttributesType, ItemType> else {
                        let foundType = type(of: item)
                        let description = "Expected to decode \(TypedDatabaseItem<AttributesType, ItemType>.self). Instead found \(foundType)."
                        let context = DecodingError.Context(codingPath: [], debugDescription: description)
                        let error = DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
                        
                        throw error
                    }
                    
                    return typedItem
                }
            } catch {
                promise.fail(error)
                return
            }
            
            promise.succeed(returnedItems)
        }
        
        return promise.futureResult
    }
    
    func monomorphicExecute<AttributesType, ItemType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?,
            nextToken: String?,
            eventLoop: EventLoop)
    -> EventLoopFuture<([TypedDatabaseItem<AttributesType, ItemType>], String?)> {
        let promise = eventLoop.makePromise(of: ([TypedDatabaseItem<AttributesType, ItemType>], String?).self)
        
        accessQueue.async {
            let items = self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)
               
            let returnedItems: [TypedDatabaseItem<AttributesType, ItemType>]
            do {
                returnedItems = try items.map { item in
                    guard let typedItem = item as? TypedDatabaseItem<AttributesType, ItemType> else {
                        let foundType = type(of: item)
                        let description = "Expected to decode \(TypedDatabaseItem<AttributesType, ItemType>.self). Instead found \(foundType)."
                        let context = DecodingError.Context(codingPath: [], debugDescription: description)
                        let error = DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
                        
                        throw error
                    }
                    
                    return typedItem
                }
            } catch {
                promise.fail(error)
                return
            }
            
            promise.succeed((returnedItems, nil))
        }
        
        return promise.futureResult
    }
    
    func getExecuteItems(partitionKeys: [String],
                         additionalWhereClause: String?) -> [PolymorphicOperationReturnTypeConvertable] {
        var items: [PolymorphicOperationReturnTypeConvertable] = []
        partitionKeys.forEach { partitionKey in
            guard let partition = self.store[partitionKey] else {
                // no such partition, continue
                return
            }
            
            partition.forEach { (sortKey, databaseItem) in
                // if there is an additional where clause
                if let additionalWhereClause = additionalWhereClause {
                    // there must be an executeItemFilter
                    if let executeItemFilter = self.executeItemFilter {
                        if executeItemFilter(partitionKey, sortKey, additionalWhereClause, databaseItem) {
                            // add if the filter says yes
                            items.append(databaseItem)
                        }
                    } else {
                        fatalError("An executeItemFilter must be provided when an excute call includes an additionalWhereClause")
                    }
                } else {
                    // otherwise just add the item
                    items.append(databaseItem)
                }
            }
        }
        
        return items
    }
}
