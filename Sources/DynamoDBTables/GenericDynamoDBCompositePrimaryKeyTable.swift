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
//  GenericDynamoDBCompositePrimaryKeyTable.swift
//  DynamoDBTables
//

import Configuration
import Logging
import Metrics

public struct DynamoDBTableMetrics: Sendable {
    // metric to record if the `TransactWrite` API is retried
    let transactWriteRetryCountRecorder: Metrics.Recorder?

    public init(transactWriteRetryCountRecorder: Metrics.Recorder? = nil) {
        self.transactWriteRetryCountRecorder = transactWriteRetryCountRecorder
    }
}

public struct DynamoDBTableConfiguration: Sendable {
    public let consistentRead: Bool
    public let escapeSingleQuoteInPartiQL: Bool
    public let retry: RetryConfiguration

    public init(
        consistentRead: Bool = true,
        escapeSingleQuoteInPartiQL: Bool = false,
        retry: RetryConfiguration = .default
    ) {
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.retry = retry
    }

    public init(from config: ConfigReader) {
        self.init(
            consistentRead: config.bool(forKey: "consistentRead", default: true),
            escapeSingleQuoteInPartiQL: config.bool(forKey: "escapeSingleQuoteInPartiQL", default: false),
            retry: RetryConfiguration(from: config.scoped(to: "retry"))
        )
    }
}

package struct GenericDynamoDBCompositePrimaryKeyTable<Client: DynamoDBClientProtocol & Sendable>:
    DynamoDBCompositePrimaryKeyTable, Sendable
{
    let dynamodb: Client
    let targetTableName: String
    package let tableConfiguration: DynamoDBTableConfiguration
    package let tableMetrics: DynamoDBTableMetrics
    let logger: Logging.Logger

    package init(
        tableName: String,
        client: Client,
        tableConfiguration: DynamoDBTableConfiguration = .init(),
        tableMetrics: DynamoDBTableMetrics = .init(),
        logger: Logging.Logger? = nil
    ) {
        self.logger = logger ?? Logging.Logger(label: "GenericDynamoDBCompositePrimaryKeyTable")
        self.dynamodb = client
        self.targetTableName = tableName
        self.tableConfiguration = tableConfiguration
        self.tableMetrics = tableMetrics

        self.logger.trace("GenericDynamoDBCompositePrimaryKeyTable created with existing client")
    }
}

extension GenericDynamoDBCompositePrimaryKeyTable {
    func getInputForInsert<AttributesType>(
        _ item: TypedTTLDatabaseItem<AttributesType, some Any, some Any>
    ) throws
        -> DynamoDBModel.PutItemInput
    {
        let attributes = try getAttributes(forItem: item)

        let expressionAttributeNames = [
            "#pk": AttributesType.partitionKeyAttributeName, "#sk": AttributesType.sortKeyAttributeName,
        ]
        let conditionExpression = "attribute_not_exists (#pk) AND attribute_not_exists (#sk)"

        return DynamoDBModel.PutItemInput(
            conditionExpression: conditionExpression,
            expressionAttributeNames: expressionAttributeNames,
            item: attributes,
            tableName: self.targetTableName
        )
    }

    func getInputForUpdateItem<AttributesType, ItemType, TimeToLiveAttributesType>(
        newItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        existingItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>
    ) throws -> DynamoDBModel.PutItemInput {
        let attributes = try getAttributes(forItem: newItem)

        let expressionAttributeNames = [
            "#rowversion": RowStatus.CodingKeys.rowVersion.stringValue,
            "#createdate": TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>.CodingKeys
                .createDate.stringValue,
        ]
        let expressionAttributeValues = [
            ":versionnumber": DynamoDBModel.AttributeValue.n(String(existingItem.rowStatus.rowVersion)),
            ":creationdate": DynamoDBModel.AttributeValue.s(existingItem.createDate.iso8601),
        ]

        let conditionExpression = "#rowversion = :versionnumber AND #createdate = :creationdate"

        return DynamoDBModel.PutItemInput(
            conditionExpression: conditionExpression,
            expressionAttributeNames: expressionAttributeNames,
            expressionAttributeValues: expressionAttributeValues,
            item: attributes,
            tableName: self.targetTableName
        )
    }

    func getInputForGetItem(forKey key: CompositePrimaryKey<some Any>) throws -> DynamoDBModel.GetItemInput {
        let attributeValue = try DynamoDBEncoder().encode(key)

        if case let .m(keyAttributes) = attributeValue {
            return DynamoDBModel.GetItemInput(
                consistentRead: self.tableConfiguration.consistentRead,
                key: keyAttributes,
                tableName: self.targetTableName
            )
        } else {
            throw DynamoDBTableError.unexpectedResponse(reason: "Expected a structure.")
        }
    }

    func getInputForBatchGetItem(
        forKeys keys: [CompositePrimaryKey<some Any>]
    ) throws
        -> DynamoDBModel.BatchGetItemInput
    {
        let keys = try keys.map { key -> [String: DynamoDBModel.AttributeValue] in
            let attributeValue = try DynamoDBEncoder().encode(key)

            if case let .m(keyAttributes) = attributeValue {
                return keyAttributes
            } else {
                throw DynamoDBTableError.unexpectedResponse(reason: "Expected a structure.")
            }
        }

        let keysAndAttributes = DynamoDBModel.KeysAndAttributes(
            consistentRead: self.tableConfiguration.consistentRead,
            keys: keys
        )

        return DynamoDBModel.BatchGetItemInput(requestItems: [self.targetTableName: keysAndAttributes])
    }

    func getInputForDeleteItem(forKey key: CompositePrimaryKey<some Any>) throws -> DynamoDBModel.DeleteItemInput {
        let attributeValue = try DynamoDBEncoder().encode(key)

        if case let .m(keyAttributes) = attributeValue {
            return DynamoDBModel.DeleteItemInput(
                key: keyAttributes,
                tableName: self.targetTableName
            )
        } else {
            throw DynamoDBTableError.unexpectedResponse(reason: "Expected a structure.")
        }
    }

    func getInputForDeleteItem<AttributesType, ItemType, TimeToLiveAttributesType>(
        existingItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>
    ) throws -> DynamoDBModel.DeleteItemInput {
        let attributeValue = try DynamoDBEncoder().encode(existingItem.compositePrimaryKey)

        guard case let .m(keyAttributes) = attributeValue else {
            throw DynamoDBTableError.unexpectedResponse(reason: "Expected a structure.")
        }

        let expressionAttributeNames = [
            "#rowversion": RowStatus.CodingKeys.rowVersion.stringValue,
            "#createdate": TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>.CodingKeys
                .createDate.stringValue,
        ]
        let expressionAttributeValues = [
            ":versionnumber": DynamoDBModel.AttributeValue.n(String(existingItem.rowStatus.rowVersion)),
            ":creationdate": DynamoDBModel.AttributeValue.s(existingItem.createDate.iso8601),
        ]

        let conditionExpression = "#rowversion = :versionnumber AND #createdate = :creationdate"

        return DynamoDBModel.DeleteItemInput(
            conditionExpression: conditionExpression,
            expressionAttributeNames: expressionAttributeNames,
            expressionAttributeValues: expressionAttributeValues,
            key: keyAttributes,
            tableName: self.targetTableName
        )
    }
}

extension Array {
    func chunked(by chunkSize: Int) -> [[Element]] {
        stride(from: 0, to: self.count, by: chunkSize).map {
            Array(self[$0..<Swift.min($0 + chunkSize, self.count)])
        }
    }
}
