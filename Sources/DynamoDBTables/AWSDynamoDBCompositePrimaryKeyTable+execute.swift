//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/AWSDynamoDBCompositePrimaryKeyTable+execute.swift
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
//  AWSDynamoDBCompositePrimaryKeyTable+execute.swift
//  DynamoDBTables
//

import AWSDynamoDB
import Foundation
import Logging

// ExecuteStatement has a maximum of 50 of decomposed read operations per request
private let maximumKeysPerExecuteStatement = 50

/// DynamoDBTable conformance execute function
public extension AWSDynamoDBCompositePrimaryKeyTable {
    private func getStatement(partitionKeys: [String],
                              attributesFilter: [String]?,
                              partitionKeyAttributeName: String,
                              additionalWhereClause: String?) -> String
    {
        let attributesFilterString = attributesFilter?.joined(separator: ", ") ?? "*"

        let partitionWhereClause: String
        if partitionKeys.count == 1 {
            partitionWhereClause = "\(partitionKeyAttributeName)='\(partitionKeys[0])'"
        } else {
            partitionWhereClause = "\(partitionKeyAttributeName) IN ['\(partitionKeys.joined(separator: "', '"))']"
        }

        let whereClausePostfix: String
        if let additionalWhereClause {
            whereClausePostfix = " \(additionalWhereClause)"
        } else {
            whereClausePostfix = ""
        }

        return """
        SELECT \(attributesFilterString) FROM "\(self.targetTableName)" WHERE \(partitionWhereClause)\(whereClausePostfix)
        """
    }

    func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        // if there are no partitions, there will be no results to return
        // succeed immediately with empty results
        guard partitionKeys.count > 0 else {
            return ([], nil)
        }

        // ExecuteStatement API has a maximum limit on the number of decomposed read operations per request.
        // Caller of this function needs to handle pagination on their side.
        guard partitionKeys.count <= maximumKeysPerExecuteStatement else {
            throw DynamoDBTableError.validationError(
                reason: "Execute API has a maximum limit of \(maximumKeysPerExecuteStatement) partition keys per request.")
        }

        let statement = self.getStatement(partitionKeys: partitionKeys,
                                          attributesFilter: attributesFilter,
                                          partitionKeyAttributeName: ReturnedType.AttributesType.partitionKeyAttributeName,
                                          additionalWhereClause: additionalWhereClause)
        let executeInput = ExecuteStatementInput(consistentRead: true, nextToken: nextToken, statement: statement)

        let executeOutput = try await self.dynamodb.executeStatement(input: executeInput)

        let nextToken = executeOutput.nextToken

        if let outputAttributeValues = executeOutput.items {
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

            return (items, nextToken)
        } else {
            return ([], nextToken)
        }
    }

    func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
        -> [ReturnedType]
    {
        // ExecuteStatement API has a maximum limit on the number of decomposed read operations per request.
        // This function handles pagination internally.
        let chunkedPartitionKeys = partitionKeys.chunked(by: maximumKeysPerExecuteStatement)
        let itemLists = try await chunkedPartitionKeys.concurrentMap { chunk -> [ReturnedType] in
            try await self.polymorphicPartialExecute(partitionKeys: chunk,
                                                     attributesFilter: attributesFilter,
                                                     additionalWhereClause: additionalWhereClause,
                                                     nextToken: nil)
        }

        return itemLists.flatMap { $0 }
    }

    func execute<AttributesType, ItemType, TimeToLiveAttributesType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
        -> (items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>], lastEvaluatedKey: String?)
    {
        // if there are no partitions, there will be no results to return
        // succeed immediately with empty results
        guard partitionKeys.count > 0 else {
            return ([], nil)
        }

        // ExecuteStatement API has a maximum limit on the number of decomposed read operations per request.
        // Caller of this function needs to handle pagination on their side.
        guard partitionKeys.count <= maximumKeysPerExecuteStatement else {
            throw DynamoDBTableError.validationError(
                reason: "Execute API has a maximum limit of \(maximumKeysPerExecuteStatement) partition keys per request.")
        }

        let statement = self.getStatement(partitionKeys: partitionKeys,
                                          attributesFilter: attributesFilter,
                                          partitionKeyAttributeName: AttributesType.partitionKeyAttributeName,
                                          additionalWhereClause: additionalWhereClause)
        let executeInput = ExecuteStatementInput(consistentRead: true, nextToken: nextToken, statement: statement)

        let executeOutput = try await self.dynamodb.executeStatement(input: executeInput)

        let nextToken = executeOutput.nextToken

        if let outputAttributeValues = executeOutput.items {
            let items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]

            do {
                items = try outputAttributeValues.map { values in
                    let attributeValue = DynamoDBClientTypes.AttributeValue.m(values)

                    return try DynamoDBDecoder().decode(attributeValue)
                }
            } catch {
                throw error.asUnrecognizedDynamoDBTableError()
            }

            return (items, nextToken)
        } else {
            return ([], nextToken)
        }
    }

    func execute<AttributesType, ItemType, TimeToLiveAttributesType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
        -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        // ExecuteStatement API has a maximum limit on the number of decomposed read operations per request.
        // This function handles pagination internally.
        let chunkedPartitionKeys = partitionKeys.chunked(by: maximumKeysPerExecuteStatement)
        let itemLists = try await chunkedPartitionKeys.concurrentMap { chunk
            -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] in
            try await self.partialExecute(partitionKeys: chunk,
                                          attributesFilter: attributesFilter,
                                          additionalWhereClause: additionalWhereClause,
                                          nextToken: nil)
        }

        return itemLists.flatMap { $0 }
    }

    // function to return a future with the results of an execute call and all future paginated calls
    private func polymorphicPartialExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?,
        nextToken: String?) async throws
        -> [ReturnedType]
    {
        let paginatedItems: ([ReturnedType], String?) =
            try await polymorphicExecute(partitionKeys: partitionKeys,
                                         attributesFilter: attributesFilter,
                                         additionalWhereClause: additionalWhereClause,
                                         nextToken: nextToken)

        // if there are more items
        if let returnedNextToken = paginatedItems.1 {
            // returns a future with all the results from all later paginated calls
            let partialResult: [ReturnedType] = try await self.polymorphicPartialExecute(partitionKeys: partitionKeys,
                                                                                         attributesFilter: attributesFilter,
                                                                                         additionalWhereClause: additionalWhereClause,
                                                                                         nextToken: returnedNextToken)

            // return the results from 'this' call and all later paginated calls
            return paginatedItems.0 + partialResult
        } else {
            // this is it, all results have been obtained
            return paginatedItems.0
        }
    }

    private func partialExecute<AttributesType, ItemType, TimeToLiveAttributesType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?,
        nextToken: String?) async throws
        -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        let paginatedItems: ([TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>], String?) =
            try await execute(partitionKeys: partitionKeys,
                              attributesFilter: attributesFilter,
                              additionalWhereClause: additionalWhereClause,
                              nextToken: nextToken)

        // if there are more items
        if let returnedNextToken = paginatedItems.1 {
            // returns a future with all the results from all later paginated calls
            let partialResult: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] = try await self.partialExecute(
                partitionKeys: partitionKeys,
                attributesFilter: attributesFilter,
                additionalWhereClause: additionalWhereClause,
                nextToken: returnedNextToken)

            // return the results from 'this' call and all later paginated calls
            return paginatedItems.0 + partialResult
        } else {
            // this is it, all results have been obtained
            return paginatedItems.0
        }
    }
}
