// swiftlint:disable cyclomatic_complexity
//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/main/Sources/SmokeDynamoDB/InMemoryDynamoDBCompositePrimaryKeyTableStore+monomorphicQuery.swift
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
//  InMemoryDynamoDBCompositePrimaryKeyTableStore+query.swift
//  DynamoDBTables
//

import AWSDynamoDB
import Foundation

extension InMemoryDynamoDBCompositePrimaryKeyTableStore {
    func getItems<AttributesType, ItemType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>]) throws
        -> [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>]
    {
        var map: [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>] = [:]

        try keys.forEach { key in
            if let partition = self.store[key.partitionKey] {
                guard let value = partition[key.sortKey] else {
                    return
                }

                guard let item = value as? TypedDatabaseItem<AttributesType, ItemType> else {
                    let foundType = type(of: value)
                    let description = "Expected to decode \(TypedDatabaseItem<AttributesType, ItemType>.self). Instead found \(foundType)."
                    let context = DecodingError.Context(codingPath: [], debugDescription: description)

                    throw DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
                }

                map[key] = item
            }
        }

        return map
    }

    func query<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                         sortKeyCondition: AttributeCondition?,
                                         consistentRead _: Bool) throws
        -> [TypedDatabaseItem<AttributesType, ItemType>]
    {
        var items: [TypedDatabaseItem<AttributesType, ItemType>] = []

        if let partition = self.store[partitionKey] {
            let sortedPartition = partition.sorted(by: { left, right -> Bool in
                left.key < right.key
            })

            sortKeyIteration: for (sortKey, value) in sortedPartition {
                if let currentSortKeyCondition = sortKeyCondition {
                    switch currentSortKeyCondition {
                    case let .equals(value):
                        if !(value == sortKey) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case let .lessThan(value):
                        if !(sortKey < value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case let .lessThanOrEqual(value):
                        if !(sortKey <= value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case let .greaterThan(value):
                        if !(sortKey > value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case let .greaterThanOrEqual(value):
                        if !(sortKey >= value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case let .between(value1, value2):
                        if !(sortKey > value1 && sortKey < value2) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case let .beginsWith(value):
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

                    throw DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
                }
            }
        }

        return items
    }

    func query<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                         sortKeyCondition: AttributeCondition?,
                                         limit: Int?,
                                         scanIndexForward: Bool,
                                         exclusiveStartKey: String?,
                                         consistentRead: Bool) throws
        -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?)
    {
        // get all the results
        let rawItems: [TypedDatabaseItem<AttributesType, ItemType>] = try query(forPartitionKey: partitionKey,
                                                                                sortKeyCondition: sortKeyCondition,
                                                                                consistentRead: consistentRead)

        let items: [TypedDatabaseItem<AttributesType, ItemType>]
        if !scanIndexForward {
            items = rawItems.reversed()
        } else {
            items = rawItems
        }

        let startIndex: Int
        // if there is an exclusiveStartKey
        if let exclusiveStartKey {
            guard let storedStartIndex = Int(exclusiveStartKey) else {
                fatalError("Unexpectedly encoded exclusiveStartKey '\(exclusiveStartKey)'")
            }

            startIndex = storedStartIndex
        } else {
            startIndex = 0
        }

        let endIndex: Int
        let lastEvaluatedKey: String?
        if let limit, startIndex + limit < items.count {
            endIndex = startIndex + limit
            lastEvaluatedKey = String(endIndex)
        } else {
            endIndex = items.count
            lastEvaluatedKey = nil
        }

        return (Array(items[startIndex ..< endIndex]), lastEvaluatedKey)
    }
}
