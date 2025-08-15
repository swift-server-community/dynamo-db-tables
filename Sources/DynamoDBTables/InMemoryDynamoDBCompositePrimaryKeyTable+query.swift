//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of DynamoDBTables authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

//
//  InMemoryDynamoDBCompositePrimaryKeyTable+query.swift
//  DynamoDBTables
//

@preconcurrency import AWSDynamoDB
import Foundation

// MARK: - Query implementations

public extension InMemoryDynamoDBCompositePrimaryKeyTable {
    func getItems<AttributesType, ItemType, TimeToLiveAttributesType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws
        -> [CompositePrimaryKey<AttributesType>: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        let items = await self.getInMemoryDatabaseItems(forKeys: keys)

        var map: [CompositePrimaryKey<AttributesType>: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] = [:]

        try items.forEach { key, value in
            map[key] = try DynamoDBDecoder().decode(DynamoDBClientTypes.AttributeValue.m(value.item))
        }

        return map
    }

    private func getInMemoryDatabaseItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) async
        -> [CompositePrimaryKey<AttributesType>: InMemoryDatabaseItem]
    {
        var map: [CompositePrimaryKey<AttributesType>: InMemoryDatabaseItem] = [:]

        let store = await self.storeWrapper.store

        for key in keys {
            if let partition = store[key.partitionKey] {
                guard let value = partition[key.sortKey] else {
                    continue
                }

                map[key] = value
            }
        }

        return map
    }

    func polymorphicGetItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>]) async throws
        -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]
    {
        let store = await self.store

        var resultMap: [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType] = [:]
        for key in keys {
            if let value = store[key.partitionKey]?[key.sortKey] {
                let itemAsReturnedType: ReturnedType = try self.convertToQueryableType(input: value)

                resultMap[key] = itemAsReturnedType
            }
        }
        return resultMap
    }

    func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                        sortKeyCondition: AttributeCondition?) async throws
        -> [ReturnedType]
    {
        var items: [ReturnedType] = []

        if let partition = await self.store[partitionKey] {
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

                try items.append(self.convertToQueryableType(input: value))
            }
        }

        return items
    }

    func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                        sortKeyCondition: AttributeCondition?,
                                                                        limit: Int?,
                                                                        exclusiveStartKey: String?) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        try await self.polymorphicQuery(forPartitionKey: partitionKey,
                                        sortKeyCondition: sortKeyCondition,
                                        limit: limit,
                                        scanIndexForward: true,
                                        exclusiveStartKey: exclusiveStartKey)
    }

    func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                        sortKeyCondition: AttributeCondition?,
                                                                        limit: Int?,
                                                                        scanIndexForward: Bool,
                                                                        exclusiveStartKey: String?) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        // get all the results
        let rawItems: [ReturnedType] = try await polymorphicQuery(forPartitionKey: partitionKey,
                                                                  sortKeyCondition: sortKeyCondition)

        let items: [ReturnedType] = if !scanIndexForward {
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

    func query<AttributesType, ItemType, TimeToLiveAttributesType>(forPartitionKey partitionKey: String,
                                                                   sortKeyCondition: AttributeCondition?) async throws
        -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        var items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] = []

        if let partition = await self.store[partitionKey] {
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

                if let typedValue: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType> = try value.getItem() {
                    items.append(typedValue)
                } else {
                    let description = "Expected type \(TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>.self), "
                        + " was \(type(of: value))."
                    let context = DecodingError.Context(codingPath: [], debugDescription: description)

                    throw DecodingError.typeMismatch(TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>.self, context)
                }
            }
        }

        return items
    }

    func query<AttributesType, ItemType, TimeToLiveAttributesType>(forPartitionKey partitionKey: String,
                                                                   sortKeyCondition: AttributeCondition?,
                                                                   limit: Int?,
                                                                   scanIndexForward: Bool,
                                                                   exclusiveStartKey: String?) async throws
        -> (items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>], lastEvaluatedKey: String?)
    {
        // get all the results
        let rawItems: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] = try await query(forPartitionKey: partitionKey,
                                                                                                                   sortKeyCondition: sortKeyCondition)

        let items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] = if !scanIndexForward {
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
