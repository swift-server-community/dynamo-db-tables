//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// Copyright (c) 2026 the DynamoDBTables authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of DynamoDBTables authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

//
//  InMemoryDynamoDBCompositePrimaryKeyTable+execute.swift
//  DynamoDBTables
//

// MARK: - Execute implementations

extension InMemoryDynamoDBCompositePrimaryKeyTable {
    public func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter _: [String]?,
        additionalWhereClause: String?
    ) async throws -> [ReturnedType] {
        let items = await self.getExecuteItems(
            partitionKeys: partitionKeys,
            additionalWhereClause: additionalWhereClause
        )

        let returnedItems: [ReturnedType] = try items.map { item in
            try self.convertToQueryableType(input: item)
        }

        return returnedItems
    }

    public func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter _: [String]?,
        additionalWhereClause: String?,
        nextToken _: String?
    ) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        let items = await self.getExecuteItems(
            partitionKeys: partitionKeys,
            additionalWhereClause: additionalWhereClause
        )

        let returnedItems: [ReturnedType] = try items.map { item in
            try self.convertToQueryableType(input: item)
        }

        return (returnedItems, nil)
    }

    public func execute<AttributesType, ItemType, TimeToLiveAttributesType>(
        partitionKeys: [String],
        attributesFilter _: [String]?,
        additionalWhereClause: String?
    ) async throws -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] {
        let items = await self.getExecuteItems(
            partitionKeys: partitionKeys,
            additionalWhereClause: additionalWhereClause
        )

        let returnedItems: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] = try items.map {
            item in
            try item.getItem()
        }

        return returnedItems
    }

    public func execute<AttributesType, ItemType, TimeToLiveAttributesType>(
        partitionKeys: [String],
        attributesFilter _: [String]?,
        additionalWhereClause: String?,
        nextToken _: String?
    ) async throws
        -> (
            items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>], lastEvaluatedKey: String?
        )
    {
        let items = await self.getExecuteItems(
            partitionKeys: partitionKeys,
            additionalWhereClause: additionalWhereClause
        )

        let returnedItems: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] = try items.map {
            item in
            try item.getItem()
        }

        return (returnedItems, nil)
    }

    internal func getExecuteItems(
        partitionKeys: [String],
        additionalWhereClause: String?
    ) async -> [InMemoryDatabaseItem] {
        let store = await self.store

        var items: [InMemoryDatabaseItem] = []
        for partitionKey in partitionKeys {
            guard let partition = store[partitionKey] else {
                // no such partition, continue
                continue
            }

            if additionalWhereClause != nil {
                fatalError(
                    "The in-memory table cannot evaluate PartiQL WHERE clauses. "
                        + "Use integration tests against LocalStack for queries with additionalWhereClause."
                )
            }

            for (_, databaseItem) in partition {
                items.append(databaseItem)
            }
        }

        return items
    }
}
