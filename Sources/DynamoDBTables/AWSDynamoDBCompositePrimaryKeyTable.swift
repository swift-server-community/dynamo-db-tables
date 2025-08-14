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

import AwsCommonRuntimeKit
import AWSDynamoDB
import ClientRuntime
import Foundation
import Logging
import Metrics
import SmithyIdentity

public struct AWSDynamoDBTableMetrics {
    // metric to record if the `TransactWrite` API is retried
    let transactWriteRetryCountRecorder: Metrics.Recorder?

    public init(transactWriteRetryCountRecorder: Metrics.Recorder? = nil) {
        self.transactWriteRetryCountRecorder = transactWriteRetryCountRecorder
    }
}

public struct AWSDynamoDBTableConfiguration {
    public let consistentRead: Bool
    public let escapeSingleQuoteInPartiQL: Bool
    public let retry: RetryConfiguration

    public init(consistentRead: Bool = true, escapeSingleQuoteInPartiQL: Bool = false,
                retry: RetryConfiguration = .default)
    {
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.retry = retry
    }
}

/// A type alias for `GenericAWSDynamoDBCompositePrimaryKeyTable` specialized with the AWS DynamoDB client.
///
/// This provides a convenient way to use the DynamoDB table implementation with the standard AWS DynamoDB client
/// without needing to specify the generic parameter explicitly.
///
/// ## Usage
///
/// Use this type alias when working with the real AWS DynamoDB service:
///
/// ```swift
/// // Create a table using region-based initialization
/// let table = try AWSDynamoDBCompositePrimaryKeyTable(
///     tableName: "MyTable",
///     region: "us-east-1"
/// )
///
/// // Create a table with an existing AWS client
/// let awsClient = AWSDynamoDB.DynamoDBClient(config: config)
/// let table = AWSDynamoDBCompositePrimaryKeyTable(
///     tableName: "MyTable",
///     client: awsClient
/// )
/// ```
public typealias AWSDynamoDBCompositePrimaryKeyTable = GenericAWSDynamoDBCompositePrimaryKeyTable<AWSDynamoDB.DynamoDBClient>

public struct GenericAWSDynamoDBCompositePrimaryKeyTable<Client: DynamoDBClientProtocol>: DynamoDBCompositePrimaryKeyTable {
    let dynamodb: Client
    let targetTableName: String
    public let tableConfiguration: AWSDynamoDBTableConfiguration
    public let tableMetrics: AWSDynamoDBTableMetrics
    let logger: Logging.Logger

    public init(tableName: String, region: Swift.String,
                awsCredentialIdentityResolver: (any SmithyIdentity.AWSCredentialIdentityResolver)? = nil,
                httpClientConfiguration: ClientRuntime.HttpClientConfiguration? = nil,
                tableConfiguration: AWSDynamoDBTableConfiguration = .init(),
                tableMetrics: AWSDynamoDBTableMetrics = .init(),
                logger: Logging.Logger? = nil) throws where Client == AWSDynamoDB.DynamoDBClient
    {
        self.logger = logger ?? Logging.Logger(label: "AWSDynamoDBCompositePrimaryKeyTable")
        let config = try DynamoDBClient.DynamoDBClientConfiguration(
            awsCredentialIdentityResolver: awsCredentialIdentityResolver,
            region: region,
            httpClientConfiguration: httpClientConfiguration)
        self.dynamodb = AWSDynamoDB.DynamoDBClient(config: config)
        self.targetTableName = tableName
        self.tableConfiguration = tableConfiguration
        self.tableMetrics = tableMetrics

        self.logger.trace("AWSDynamoDBCompositePrimaryKeyTable created with region '\(region)'")
    }

    public init(tableName: String,
                client: Client,
                tableConfiguration: AWSDynamoDBTableConfiguration = .init(),
                tableMetrics: AWSDynamoDBTableMetrics = .init(),
                logger: Logging.Logger? = nil)
    {
        self.logger = logger ?? Logging.Logger(label: "AWSDynamoDBCompositePrimaryKeyTable")
        self.dynamodb = client
        self.targetTableName = tableName
        self.tableConfiguration = tableConfiguration
        self.tableMetrics = tableMetrics

        self.logger.trace("AWSDynamoDBCompositePrimaryKeyTable created with existing client")
    }
}

extension GenericAWSDynamoDBCompositePrimaryKeyTable {
    func getInputForInsert<AttributesType>(
        _ item: TypedTTLDatabaseItem<AttributesType, some Any, some Any>) throws
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

    func getInputForUpdateItem<AttributesType, ItemType, TimeToLiveAttributesType>(
        newItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        existingItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) throws -> AWSDynamoDB.PutItemInput
    {
        let attributes = try getAttributes(forItem: newItem)

        let expressionAttributeNames = [
            "#rowversion": RowStatus.CodingKeys.rowVersion.stringValue,
            "#createdate": TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>.CodingKeys.createDate.stringValue,
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
            return AWSDynamoDB.GetItemInput(consistentRead: self.tableConfiguration.consistentRead,
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

        let keysAndAttributes = DynamoDBClientTypes.KeysAndAttributes(consistentRead: self.tableConfiguration.consistentRead,
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

    func getInputForDeleteItem<AttributesType, ItemType, TimeToLiveAttributesType>(
        existingItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) throws -> AWSDynamoDB.DeleteItemInput
    {
        let attributeValue = try DynamoDBEncoder().encode(existingItem.compositePrimaryKey)

        guard case let .m(keyAttributes) = attributeValue else {
            throw DynamoDBTableError.unexpectedResponse(reason: "Expected a structure.")
        }

        let expressionAttributeNames = [
            "#rowversion": RowStatus.CodingKeys.rowVersion.stringValue,
            "#createdate": TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>.CodingKeys.createDate.stringValue,
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
