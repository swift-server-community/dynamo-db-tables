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

// MARK: - Execute implementations

public extension InMemoryDynamoDBCompositePrimaryKeyTable {
    func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter _: [String]?,
        additionalWhereClause: String?) async throws -> [ReturnedType]
    {
        let items = await self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)

        let returnedItems: [ReturnedType] = try items.map { item in
            try self.convertToQueryableType(input: item)
        }

        return returnedItems
    }

    func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter _: [String]?,
        additionalWhereClause: String?, nextToken _: String?) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        let items = await self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)

        let returnedItems: [ReturnedType] = try items.map { item in
            try self.convertToQueryableType(input: item)
        }

        return (returnedItems, nil)
    }

    func execute<AttributesType, ItemType, TimeToLiveAttributesType>(
        partitionKeys: [String],
        attributesFilter _: [String]?,
        additionalWhereClause: String?) async throws -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        let items = await self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)

        let returnedItems: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] = try items.map { item in
            guard let typedItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType> = try item.getItem() else {
                let foundType = type(of: item)
                let description = "Expected to decode \(TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>.self). Instead found \(foundType)."
                let context = DecodingError.Context(codingPath: [], debugDescription: description)
                let error = DecodingError.typeMismatch(TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>.self, context)

                throw error
            }

            return typedItem
        }

        return returnedItems
    }

    func execute<AttributesType, ItemType, TimeToLiveAttributesType>(
        partitionKeys: [String],
        attributesFilter _: [String]?,
        additionalWhereClause: String?, nextToken _: String?) async throws
        -> (items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>], lastEvaluatedKey: String?)
    {
        let items = await self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)

        let returnedItems: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] = try items.map { item in
            guard let typedItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType> = try item.getItem() else {
                let foundType = type(of: item)
                let description = "Expected to decode \(TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>.self). Instead found \(foundType)."
                let context = DecodingError.Context(codingPath: [], debugDescription: description)
                let error = DecodingError.typeMismatch(TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>.self, context)

                throw error
            }

            return typedItem
        }

        return (returnedItems, nil)
    }

    internal func getExecuteItems(partitionKeys: [String],
                                  additionalWhereClause: String?) async -> [InMemoryDatabaseItem]
    {
        let store = await self.store

        var items: [InMemoryDatabaseItem] = []
        for partitionKey in partitionKeys {
            guard let partition = store[partitionKey] else {
                // no such partition, continue
                continue
            }

            for (sortKey, databaseItem) in partition {
                // if there is an additional where clause
                if let additionalWhereClause {
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
