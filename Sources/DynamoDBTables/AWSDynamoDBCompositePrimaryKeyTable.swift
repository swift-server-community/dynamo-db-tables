//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/AWSDynamoDBCompositePrimaryKeyTable.swift
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
//  AWSDynamoDBCompositePrimaryKeyTable.swift
//  DynamoDBTables
//

import AWSClientRuntime
import AwsCommonRuntimeKit
import AWSDynamoDB
import ClientRuntime
import Foundation
import Logging
import Metrics

public struct AWSDynamoDBTableMetrics {
    // metric to record if the `TransactWrite` API is retried
    let transactWriteRetryCountRecorder: Metrics.Recorder?

    public init(transactWriteRetryCountRecorder: Metrics.Recorder? = nil) {
        self.transactWriteRetryCountRecorder = transactWriteRetryCountRecorder
    }
}

public struct AWSDynamoDBCompositePrimaryKeyTable: DynamoDBCompositePrimaryKeyTable {
    let dynamodb: AWSDynamoDB.DynamoDBClient
    let targetTableName: String
    public let consistentRead: Bool
    public let escapeSingleQuoteInPartiQL: Bool
    public let tableMetrics: AWSDynamoDBTableMetrics
    let retryConfiguration: RetryConfiguration
    let logger: Logging.Logger

    public init(tableName: String, region: Swift.String,
                awsCredentialIdentityResolver: (any AWSClientRuntime.AWSCredentialIdentityResolver)? = nil,
                httpClientConfiguration: ClientRuntime.HttpClientConfiguration? = nil,
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false,
                tableMetrics: AWSDynamoDBTableMetrics = .init(),
                retryConfiguration: RetryConfiguration = .default,
                logger: Logging.Logger? = nil) throws
    {
        self.logger = logger ?? Logging.Logger(label: "AWSDynamoDBCompositePrimaryKeyTable")
        let config = try DynamoDBClient.DynamoDBClientConfiguration(
            awsCredentialIdentityResolver: awsCredentialIdentityResolver,
            region: region,
            httpClientConfiguration: httpClientConfiguration)
        self.dynamodb = AWSDynamoDB.DynamoDBClient(config: config)
        self.targetTableName = tableName
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.tableMetrics = tableMetrics
        self.retryConfiguration = retryConfiguration

        self.logger.trace("AWSDynamoDBCompositePrimaryKeyTable created with region '\(region)'")
    }

    public init(tableName: String,
                client: AWSDynamoDB.DynamoDBClient,
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false,
                tableMetrics: AWSDynamoDBTableMetrics = .init(),
                retryConfiguration: RetryConfiguration = .default,
                logger: Logging.Logger? = nil)
    {
        self.logger = logger ?? Logging.Logger(label: "AWSDynamoDBCompositePrimaryKeyTable")
        self.dynamodb = client
        self.targetTableName = tableName
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.tableMetrics = tableMetrics
        self.retryConfiguration = retryConfiguration

        self.logger.trace("AWSDynamoDBCompositePrimaryKeyTable created with existing client")
    }
}

extension AWSDynamoDBCompositePrimaryKeyTable {
    func getInputForInsert<AttributesType>(_ item: TypedDatabaseItem<AttributesType, some Any>) throws
        -> AWSDynamoDB.PutItemInput
    {
        let attributes = try getAttributes(forItem: item)

        let expressionAttributeNames = ["#pk": AttributesType.partitionKeyAttributeName, "#sk": AttributesType.sortKeyAttributeName]
        let conditionExpression = "attribute_not_exists (#pk) AND attribute_not_exists (#sk)"

        return AWSDynamoDB.PutItemInput(conditionExpression: conditionExpression,
                                        expressionAttributeNames: expressionAttributeNames,
                                        item: attributes,
                                        tableName: self.targetTableName)
    }

    func getInputForUpdateItem<AttributesType, ItemType>(
        newItem: TypedDatabaseItem<AttributesType, ItemType>,
        existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws -> AWSDynamoDB.PutItemInput
    {
        let attributes = try getAttributes(forItem: newItem)

        let expressionAttributeNames = [
            "#rowversion": RowStatus.CodingKeys.rowVersion.stringValue,
            "#createdate": TypedDatabaseItem<AttributesType, ItemType>.CodingKeys.createDate.stringValue,
        ]
        let expressionAttributeValues = [
            ":versionnumber": DynamoDBClientTypes.AttributeValue.n(String(existingItem.rowStatus.rowVersion)),
            ":creationdate": DynamoDBClientTypes.AttributeValue.s(existingItem.createDate.iso8601),
        ]

        let conditionExpression = "#rowversion = :versionnumber AND #createdate = :creationdate"

        return AWSDynamoDB.PutItemInput(conditionExpression: conditionExpression,
                                        expressionAttributeNames: expressionAttributeNames,
                                        expressionAttributeValues: expressionAttributeValues,
                                        item: attributes,
                                        tableName: self.targetTableName)
    }

    func getInputForGetItem(forKey key: CompositePrimaryKey<some Any>) throws -> AWSDynamoDB.GetItemInput {
        let attributeValue = try DynamoDBEncoder().encode(key)

        if case let .m(keyAttributes) = attributeValue {
            return AWSDynamoDB.GetItemInput(consistentRead: self.consistentRead,
                                            key: keyAttributes,
                                            tableName: self.targetTableName)
        } else {
            throw DynamoDBTableError.unexpectedResponse(reason: "Expected a structure.")
        }
    }

    func getInputForBatchGetItem(forKeys keys: [CompositePrimaryKey<some Any>]) throws
        -> AWSDynamoDB.BatchGetItemInput
    {
        let keys = try keys.map { key -> [String: DynamoDBClientTypes.AttributeValue] in
            let attributeValue = try DynamoDBEncoder().encode(key)

            if case let .m(keyAttributes) = attributeValue {
                return keyAttributes
            } else {
                throw DynamoDBTableError.unexpectedResponse(reason: "Expected a structure.")
            }
        }

        let keysAndAttributes = DynamoDBClientTypes.KeysAndAttributes(consistentRead: self.consistentRead,
                                                                      keys: keys)

        return AWSDynamoDB.BatchGetItemInput(requestItems: [self.targetTableName: keysAndAttributes])
    }

    func getInputForDeleteItem(forKey key: CompositePrimaryKey<some Any>) throws -> AWSDynamoDB.DeleteItemInput {
        let attributeValue = try DynamoDBEncoder().encode(key)

        if case let .m(keyAttributes) = attributeValue {
            return AWSDynamoDB.DeleteItemInput(key: keyAttributes,
                                               tableName: self.targetTableName)
        } else {
            throw DynamoDBTableError.unexpectedResponse(reason: "Expected a structure.")
        }
    }

    func getInputForDeleteItem<AttributesType, ItemType>(
        existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws -> AWSDynamoDB.DeleteItemInput
    {
        let attributeValue = try DynamoDBEncoder().encode(existingItem.compositePrimaryKey)

        guard case let .m(keyAttributes) = attributeValue else {
            throw DynamoDBTableError.unexpectedResponse(reason: "Expected a structure.")
        }

        let expressionAttributeNames = [
            "#rowversion": RowStatus.CodingKeys.rowVersion.stringValue,
            "#createdate": TypedDatabaseItem<AttributesType, ItemType>.CodingKeys.createDate.stringValue,
        ]
        let expressionAttributeValues = [
            ":versionnumber": DynamoDBClientTypes.AttributeValue.n(String(existingItem.rowStatus.rowVersion)),
            ":creationdate": DynamoDBClientTypes.AttributeValue.s(existingItem.createDate.iso8601),
        ]

        let conditionExpression = "#rowversion = :versionnumber AND #createdate = :creationdate"

        return AWSDynamoDB.DeleteItemInput(conditionExpression: conditionExpression,
                                           expressionAttributeNames: expressionAttributeNames,
                                           expressionAttributeValues: expressionAttributeValues,
                                           key: keyAttributes,
                                           tableName: self.targetTableName)
    }
}

extension Array {
    func chunked(by chunkSize: Int) -> [[Element]] {
        stride(from: 0, to: self.count, by: chunkSize).map {
            Array(self[$0 ..< Swift.min($0 + chunkSize, self.count)])
        }
    }
}
