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
//  InMemoryDynamoDBCompositePrimaryKeysProjectionStore.swift
//  DynamoDBTables
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel
import NIO

public class InMemoryDynamoDBCompositePrimaryKeysProjectionStore {
    public var keys: [Any] = []
    private let accessQueue = DispatchQueue(
        label: "com.amazon.DynamoDBTables.InMemoryDynamoDBCompositePrimaryKeysProjection.accessQueue",
        target: DispatchQueue.global())

    public init(keys: [Any] = []) {
        self.keys = keys
    }
    
    func getKeys(eventLoop: EventLoop) -> EventLoopFuture<[Any]> {
        let promise = eventLoop.makePromise(of: [Any].self)
        
        accessQueue.async {
            promise.succeed(self.keys)
        }
        
        return promise.futureResult
    }

    public func query<AttributesType>(forPartitionKey partitionKey: String,
                                      sortKeyCondition: AttributeCondition?,
                                      eventLoop: EventLoop)
            -> EventLoopFuture<[CompositePrimaryKey<AttributesType>]> {
        let promise = eventLoop.makePromise(of: [CompositePrimaryKey<AttributesType>].self)
        
        accessQueue.async {
            var items: [CompositePrimaryKey<AttributesType>] = []
                
            let sortedKeys = self.keys.compactMap { $0 as? CompositePrimaryKey<AttributesType> }.sorted(by: { (left, right) -> Bool in
                return left.sortKey < right.sortKey
            })
                
            sortKeyIteration: for key in sortedKeys {
                if key.partitionKey != partitionKey {
                    // don't include this in the results
                    continue sortKeyIteration
                }
                
                let sortKey = key.sortKey

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

                items.append(key)
            }

            promise.succeed(items)
        }
        
        return promise.futureResult
    }
    
    public func query<AttributesType>(forPartitionKey partitionKey: String,
                                      sortKeyCondition: AttributeCondition?,
                                      limit: Int?,
                                      exclusiveStartKey: String?,
                                      eventLoop: EventLoop)
            -> EventLoopFuture<([CompositePrimaryKey<AttributesType>], String?)>
            where AttributesType: PrimaryKeyAttributes {
        return query(forPartitionKey: partitionKey,
                     sortKeyCondition: sortKeyCondition,
                     limit: limit,
                     scanIndexForward: true,
                     exclusiveStartKey: exclusiveStartKey,
                     eventLoop: eventLoop)
    }

    public func query<AttributesType>(forPartitionKey partitionKey: String,
                                      sortKeyCondition: AttributeCondition?,
                                      limit: Int?,
                                      scanIndexForward: Bool,
                                      exclusiveStartKey: String?,
                                      eventLoop: EventLoop)
            -> EventLoopFuture<([CompositePrimaryKey<AttributesType>], String?)>
            where AttributesType: PrimaryKeyAttributes {
        // get all the results
        return query(forPartitionKey: partitionKey,
                     sortKeyCondition: sortKeyCondition,
                     eventLoop: eventLoop)
            .map { (rawItems: [CompositePrimaryKey<AttributesType>]) in
                let items: [CompositePrimaryKey<AttributesType>]
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
