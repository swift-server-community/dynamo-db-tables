//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/AWSDynamoDBCompositePrimaryKeyTable+DynamoDBTableAsync.swift
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
//  AWSDynamoDBCompositePrimaryKeyTable+DynamoDBTableAsync.swift
//  DynamoDBTables
//

import AWSDynamoDB
#if canImport(Darwin)
    import Foundation
#endif
import Logging

/// DynamoDBTable conformance async functions
extension GenericAWSDynamoDBCompositePrimaryKeyTable {
    public func insertItem(_ item: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
        let putItemInput = try getInputForInsert(item)

        try await putItem(forInput: putItemInput, withKey: item.compositePrimaryKey)
    }

    public func clobberItem(_ item: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
        let attributes = try getAttributes(forItem: item)

        let putItemInput = AWSDynamoDB.PutItemInput(
            item: attributes,
            tableName: targetTableName
        )

        try await self.putItem(forInput: putItemInput, withKey: item.compositePrimaryKey)
    }

    public func updateItem<AttributesType, ItemType, TimeToLiveAttributesType>(
        newItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        existingItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>
    ) async throws {
        let putItemInput = try getInputForUpdateItem(newItem: newItem, existingItem: existingItem)

        try await putItem(forInput: putItemInput, withKey: newItem.compositePrimaryKey)
    }

    public func getItem<AttributesType, ItemType, TimeToLiveAttributesType>(
        forKey key: CompositePrimaryKey<AttributesType>
    ) async throws
        -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>?
    {
        let getItemInput = try getInputForGetItem(forKey: key)

        self.logger.trace("dynamodb.getItem with key: \(key) and table name \(targetTableName)")

        let attributeValue = try await self.dynamodb.getItem(input: getItemInput)

        if let item = attributeValue.item {
            self.logger.trace("Value returned from DynamoDB.")

            do {
                let decodedItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>? =
                    try DynamoDBDecoder().decode(DynamoDBClientTypes.AttributeValue.m(item))
                return decodedItem
            } catch {
                throw error.asUnrecognizedDynamoDBTableError()
            }
        } else {
            self.logger.trace("No item returned from DynamoDB.")

            return nil
        }
    }

    public func deleteItem(forKey key: CompositePrimaryKey<some Any>) async throws {
        let deleteItemInput = try getInputForDeleteItem(forKey: key)

        self.logger.trace("dynamodb.deleteItem with key: \(key) and table name \(targetTableName)")

        do {
            _ = try await self.dynamodb.deleteItem(input: deleteItemInput)
        } catch {
            throw error.asDynamoDBTableError(
                partitionKey: key.partitionKey,
                sortKey: key.sortKey
            )
        }
    }

    public func deleteItem(existingItem: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
        let deleteItemInput = try getInputForDeleteItem(existingItem: existingItem)

        let logMessage =
            "dynamodb.deleteItem with key: \(existingItem.compositePrimaryKey), "
            + " version \(existingItem.rowStatus.rowVersion) and table name \(targetTableName)"

        self.logger.trace("\(logMessage)")

        do {
            _ = try await self.dynamodb.deleteItem(input: deleteItemInput)
        } catch {
            throw error.asDynamoDBTableError(
                partitionKey: existingItem.compositePrimaryKey.partitionKey,
                sortKey: existingItem.compositePrimaryKey.sortKey
            )
        }
    }

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?
    ) async throws
        -> [ReturnedType]
    {
        try await self.polymorphicPartialQuery(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition,
            exclusiveStartKey: nil
        )
    }

    // function to return a future with the results of a query call and all future paginated calls
    private func polymorphicPartialQuery<ReturnedType: PolymorphicOperationReturnType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        exclusiveStartKey: String?
    ) async throws -> [ReturnedType] {
        let paginatedItems: ([ReturnedType], String?) =
            try await polymorphicQuery(
                forPartitionKey: partitionKey,
                sortKeyCondition: sortKeyCondition,
                limit: nil,
                scanIndexForward: true,
                exclusiveStartKey: exclusiveStartKey
            )

        // if there are more items
        if let lastEvaluatedKey = paginatedItems.1 {
            // returns a future with all the results from all later paginated calls
            let partialResult: [ReturnedType] = try await self.polymorphicPartialQuery(
                forPartitionKey: partitionKey,
                sortKeyCondition: sortKeyCondition,
                exclusiveStartKey: lastEvaluatedKey
            )

            // return the results from 'this' call and all later paginated calls
            return paginatedItems.0 + partialResult
        } else {
            // this is it, all results have been obtained
            return paginatedItems.0
        }
    }

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        exclusiveStartKey: String?
    ) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        try await self.polymorphicQuery(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition,
            limit: limit,
            scanIndexForward: true,
            exclusiveStartKey: exclusiveStartKey
        )
    }

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        scanIndexForward: Bool,
        exclusiveStartKey: String?
    ) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        let queryInput = try AWSDynamoDB.QueryInput.forSortKeyCondition(
            partitionKey: partitionKey,
            targetTableName: targetTableName,
            primaryKeyType: ReturnedType.AttributesType.self,
            sortKeyCondition: sortKeyCondition,
            limit: limit,
            scanIndexForward: scanIndexForward,
            exclusiveStartKey: exclusiveStartKey,
            consistentRead: self.tableConfiguration.consistentRead
        )

        let logMessage =
            "dynamodb.query with partitionKey: \(partitionKey), "
            + "sortKeyCondition: \(sortKeyCondition.debugDescription), and table name \(targetTableName)."
        self.logger.trace("\(logMessage)")

        let queryOutput = try await self.dynamodb.query(input: queryInput)

        let lastEvaluatedKey: String?
        if let returnedLastEvaluatedKey = queryOutput.lastEvaluatedKey {
            let encodedLastEvaluatedKey: Data

            do {
                encodedLastEvaluatedKey = try JSONEncoder().encode(returnedLastEvaluatedKey)
            } catch {
                throw error.asUnrecognizedDynamoDBTableError()
            }

            lastEvaluatedKey = String(data: encodedLastEvaluatedKey, encoding: .utf8)
        } else {
            lastEvaluatedKey = nil
        }

        if let outputAttributeValues = queryOutput.items {
            let items: [ReturnedType]

            do {
                items = try outputAttributeValues.map { values in
                    let attributeValue = DynamoDBClientTypes.AttributeValue.m(values)

                    let decodedItem: ReturnTypeDecodable<ReturnedType> = try DynamoDBDecoder().decode(attributeValue)

                    return decodedItem.decodedValue
                }
            } catch {
                throw error.asUnrecognizedDynamoDBTableError()
            }

            return (items, lastEvaluatedKey)
        } else {
            return ([], lastEvaluatedKey)
        }
    }

    private func putItem(
        forInput putItemInput: AWSDynamoDB.PutItemInput,
        withKey compositePrimaryKey: CompositePrimaryKey<some Any>
    ) async throws {
        let logMessage = "dynamodb.putItem with item: \(putItemInput) and table name \(targetTableName)."
        self.logger.trace("\(logMessage)")

        do {
            _ = try await self.dynamodb.putItem(input: putItemInput)
        } catch {
            throw error.asDynamoDBTableError(
                partitionKey: compositePrimaryKey.partitionKey,
                sortKey: compositePrimaryKey.sortKey
            )
        }
    }

    public func query<AttributesType, ItemType, TimeToLiveAttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?
    ) async throws
        -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        try await self.partialQuery(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition,
            exclusiveStartKey: nil
        )
    }

    // function to return a future with the results of a query call and all future paginated calls
    private func partialQuery<AttributesType, ItemType, TimeToLiveAttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        exclusiveStartKey _: String?
    ) async throws -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] {
        let paginatedItems: ([TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>], String?) =
            try await query(
                forPartitionKey: partitionKey,
                sortKeyCondition: sortKeyCondition,
                limit: nil,
                scanIndexForward: true,
                exclusiveStartKey: nil
            )

        // if there are more items
        if let lastEvaluatedKey = paginatedItems.1 {
            // returns a future with all the results from all later paginated calls
            let partialResult: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] =
                try await self.partialQuery(
                    forPartitionKey: partitionKey,
                    sortKeyCondition: sortKeyCondition,
                    exclusiveStartKey: lastEvaluatedKey
                )

            // return the results from 'this' call and all later paginated calls
            return paginatedItems.0 + partialResult
        } else {
            // this is it, all results have been obtained
            return paginatedItems.0
        }
    }

    public func query<AttributesType, ItemType, TimeToLiveAttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        scanIndexForward: Bool,
        exclusiveStartKey: String?
    ) async throws
        -> (
            items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>], lastEvaluatedKey: String?
        )
    {
        let queryInput = try AWSDynamoDB.QueryInput.forSortKeyCondition(
            partitionKey: partitionKey,
            targetTableName: targetTableName,
            primaryKeyType: AttributesType.self,
            sortKeyCondition: sortKeyCondition,
            limit: limit,
            scanIndexForward: scanIndexForward,
            exclusiveStartKey: exclusiveStartKey,
            consistentRead: self.tableConfiguration.consistentRead
        )

        let logMessage =
            "dynamodb.query with partitionKey: \(partitionKey), "
            + "sortKeyCondition: \(sortKeyCondition.debugDescription), and table name \(targetTableName)."
        self.logger.trace("\(logMessage)")

        let queryOutput = try await self.dynamodb.query(input: queryInput)

        let lastEvaluatedKey: String?
        if let returnedLastEvaluatedKey = queryOutput.lastEvaluatedKey {
            let encodedLastEvaluatedKey: Data

            do {
                encodedLastEvaluatedKey = try JSONEncoder().encode(returnedLastEvaluatedKey)
            } catch {
                throw error.asUnrecognizedDynamoDBTableError()
            }

            lastEvaluatedKey = String(data: encodedLastEvaluatedKey, encoding: .utf8)
        } else {
            lastEvaluatedKey = nil
        }

        if let outputAttributeValues = queryOutput.items {
            let items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]

            do {
                items = try outputAttributeValues.map { values in
                    let attributeValue = DynamoDBClientTypes.AttributeValue.m(values)

                    return try DynamoDBDecoder().decode(attributeValue)
                }
            } catch {
                throw error.asUnrecognizedDynamoDBTableError()
            }

            return (items, lastEvaluatedKey)
        } else {
            return ([], lastEvaluatedKey)
        }
    }
}
