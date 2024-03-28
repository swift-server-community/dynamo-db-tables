// swiftlint:disable cyclomatic_complexity
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
//  InMemoryDynamoDBCompositePrimaryKeyTableStore+monomorphicQuery.swift
//  DynamoDBTables
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel
import NIO

extension InMemoryDynamoDBCompositePrimaryKeyTableStore {
    
    func monomorphicGetItems<AttributesType, ItemType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>],
        eventLoop: EventLoop)
    -> EventLoopFuture<[CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>]> {
        let promise = eventLoop.makePromise(of: [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>].self)
        
        accessQueue.async {
            var map: [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>] = [:]
            
            keys.forEach { key in
                if let partition = self.store[key.partitionKey] {

                    guard let value = partition[key.sortKey] else {
                        return
                    }
                    
                    guard let item = value as? TypedDatabaseItem<AttributesType, ItemType> else {
                        let foundType = type(of: value)
                        let description = "Expected to decode \(TypedDatabaseItem<AttributesType, ItemType>.self). Instead found \(foundType)."
                        let context = DecodingError.Context(codingPath: [], debugDescription: description)
                        let error = DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
                        
                        promise.fail(error)
                        return
                    }
                    
                    map[key] = item
                }
            }
            
            promise.succeed(map)
        }
        
        return promise.futureResult
    }
    
    func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                    sortKeyCondition: AttributeCondition?,
                                                    eventLoop: EventLoop)
    -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]>
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        let promise = eventLoop.makePromise(of: [TypedDatabaseItem<AttributesType, ItemType>].self)
        
        accessQueue.async {
            var items: [TypedDatabaseItem<AttributesType, ItemType>] = []

            if let partition = self.store[partitionKey] {
                let sortedPartition = partition.sorted(by: { (left, right) -> Bool in
                    return left.key < right.key
                })
                
                sortKeyIteration: for (sortKey, value) in sortedPartition {
                    if let currentSortKeyCondition = sortKeyCondition {
                        switch currentSortKeyCondition {
                        case .equals(let value):
                            if !(value == sortKey) {
                                // don't include this in the results
                                continue sortKeyIteration
                            }
                        case .lessThan(let value):
                            if !(sortKey < value) {
                                // don't include this in the results
                                continue sortKeyIteration
                            }
                        case .lessThanOrEqual(let value):
                            if !(sortKey <= value) {
                                // don't include this in the results
                                continue sortKeyIteration
                            }
                        case .greaterThan(let value):
                            if !(sortKey > value) {
                                // don't include this in the results
                                continue sortKeyIteration
                            }
                        case .greaterThanOrEqual(let value):
                            if !(sortKey >= value) {
                                // don't include this in the results
                                continue sortKeyIteration
                            }
                        case .between(let value1, let value2):
                            if !(sortKey > value1 && sortKey < value2) {
                                // don't include this in the results
                                continue sortKeyIteration
                            }
                        case .beginsWith(let value):
                            if !(sortKey.hasPrefix(value)) {
                                // don't include this in the results
                                continue sortKeyIteration
                            }
                        }
                    }

                    if let typedValue = value as? TypedDatabaseItem<AttributesType, ItemType> {
                        items.append(typedValue)
                    } else {
                        let description = "Expected type \(TypedDatabaseItem<AttributesType, ItemType>.self), "
                            + " was \(type(of: value))."
                        let context = DecodingError.Context(codingPath: [], debugDescription: description)
                        let error = DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
                        
                        promise.fail(error)
                        return
                    }
                }
            }

            promise.succeed(items)
        }
        
        return promise.futureResult
    }
    
    func monomorphicQuery<AttributesType, ItemType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?,
            scanIndexForward: Bool,
            exclusiveStartKey: String?,
            eventLoop: EventLoop)
            -> EventLoopFuture<([TypedDatabaseItem<AttributesType, ItemType>], String?)>
            where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        // get all the results
        return monomorphicQuery(forPartitionKey: partitionKey,
                                sortKeyCondition: sortKeyCondition,
                                eventLoop: eventLoop)
            .map { (rawItems: [TypedDatabaseItem<AttributesType, ItemType>]) in
                let items: [TypedDatabaseItem<AttributesType, ItemType>]
                if !scanIndexForward {
                    items = rawItems.reversed()
                } else {
                    items = rawItems
                }

                let startIndex: Int
                // if there is an exclusiveStartKey
                if let exclusiveStartKey = exclusiveStartKey {
                    guard let storedStartIndex = Int(exclusiveStartKey) else {
                        fatalError("Unexpectedly encoded exclusiveStartKey '\(exclusiveStartKey)'")
                    }

                    startIndex = storedStartIndex
                } else {
                    startIndex = 0
                }

                let endIndex: Int
                let lastEvaluatedKey: String?
                if let limit = limit, startIndex + limit < items.count {
                    endIndex = startIndex + limit
                    lastEvaluatedKey = String(endIndex)
                } else {
                    endIndex = items.count
                    lastEvaluatedKey = nil
                }

                return (Array(items[startIndex..<endIndex]), lastEvaluatedKey)
            }
    }
}
