//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/AWSDynamoDBCompositePrimaryKeysProjection+DynamoDBKeysProjectionAsync.swift
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
//  AWSDynamoDBCompositePrimaryKeysProjection+DynamoDBKeysProjectionAsync.swift
//  DynamoDBTables
//

import AWSDynamoDB
import Foundation
import Logging

/// DynamoDBKeysProjection conformance async functions
extension AWSDynamoDBCompositePrimaryKeysProjection {
    public func query<AttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?
    ) async throws
        -> [CompositePrimaryKey<AttributesType>]
    {
        try await self.partialQuery(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition,
            exclusiveStartKey: nil
        )
    }

    // function to return a future with the results of a query call and all future paginated calls
    private func partialQuery<AttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        exclusiveStartKey: String?
    ) async throws
        -> [CompositePrimaryKey<AttributesType>]
    {
        let paginatedItems: ([CompositePrimaryKey<AttributesType>], String?) =
            try await query(
                forPartitionKey: partitionKey,
                sortKeyCondition: sortKeyCondition,
                limit: nil,
                scanIndexForward: true,
                exclusiveStartKey: exclusiveStartKey
            )

        // if there are more items
        if let lastEvaluatedKey = paginatedItems.1 {
            // returns a future with all the results from all later paginated calls
            let partialResult: [CompositePrimaryKey<AttributesType>] = try await self.partialQuery(
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

    public func query<AttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        exclusiveStartKey: String?
    ) async throws
        -> (keys: [CompositePrimaryKey<AttributesType>], lastEvaluatedKey: String?)
    {
        try await self.query(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition,
            limit: limit,
            scanIndexForward: true,
            exclusiveStartKey: exclusiveStartKey
        )
    }

    public func query<AttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        scanIndexForward: Bool,
        exclusiveStartKey: String?
    ) async throws
        -> (keys: [CompositePrimaryKey<AttributesType>], lastEvaluatedKey: String?)
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
            let items: [CompositePrimaryKey<AttributesType>]

            do {
                items = try outputAttributeValues.map { values in
                    let attributeValue: DynamoDBClientTypes.AttributeValue = .m(values)

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
