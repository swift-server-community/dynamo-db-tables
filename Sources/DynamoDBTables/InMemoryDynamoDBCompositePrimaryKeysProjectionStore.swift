// swiftlint:disable cyclomatic_complexity
//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/main/Sources/SmokeDynamoDB/InMemoryDynamoDBCompositePrimaryKeysProjectionStore.swift
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
//  InMemoryDynamoDBCompositePrimaryKeysProjectionStore.swift
//  DynamoDBTables
//

import AWSDynamoDB
import Foundation

// MARK: - Store implementation

actor InMemoryDynamoDBCompositePrimaryKeysProjectionStore {
    var keys: [TypeErasedCompositePrimaryKey] = []

    init(keys: [CompositePrimaryKey<some Any>] = []) {
        self.keys = keys.map { .init(partitionKey: $0.partitionKey, sortKey: $0.sortKey) }
    }

    func query<AttributesType>(forPartitionKey partitionKey: String,
                               sortKeyCondition: AttributeCondition?) async throws
        -> [CompositePrimaryKey<AttributesType>]
    {
        var items: [CompositePrimaryKey<AttributesType>] = []

        let sortedKeys: [CompositePrimaryKey<AttributesType>] = self.keys.compactMap { .init(partitionKey: $0.partitionKey, sortKey: $0.sortKey) }
            .sorted(by: { left, right -> Bool in left.sortKey < right.sortKey })

        sortKeyIteration: for key in sortedKeys {
            if key.partitionKey != partitionKey {
                // don't include this in the results
                continue sortKeyIteration
            }

            let sortKey = key.sortKey

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

            items.append(key)
        }

        return items
    }

    func query<AttributesType>(forPartitionKey partitionKey: String,
                               sortKeyCondition: AttributeCondition?,
                               limit: Int?,
                               exclusiveStartKey: String?) async throws
        -> (keys: [CompositePrimaryKey<AttributesType>], lastEvaluatedKey: String?)
    {
        try await self.query(forPartitionKey: partitionKey,
                             sortKeyCondition: sortKeyCondition,
                             limit: limit,
                             scanIndexForward: true,
                             exclusiveStartKey: exclusiveStartKey)
    }

    func query<AttributesType>(forPartitionKey partitionKey: String,
                               sortKeyCondition: AttributeCondition?,
                               limit: Int?,
                               scanIndexForward: Bool,
                               exclusiveStartKey: String?) async throws
        -> (keys: [CompositePrimaryKey<AttributesType>], lastEvaluatedKey: String?)
    {
        // get all the results
        let rawItems: [CompositePrimaryKey<AttributesType>] = try await query(forPartitionKey: partitionKey,
                                                                              sortKeyCondition: sortKeyCondition)
        let items: [CompositePrimaryKey<AttributesType>] = if !scanIndexForward {
            rawItems.reversed()
        } else {
            rawItems
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
