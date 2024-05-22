//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/aws-sdk-swift-main/Sources/SmokeDynamoDB/QueryInput+forSortKeyCondition.swift
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
//  QueryInput+forSortKeyCondition.swift
//  DynamoDBTables
//

import AWSDynamoDB
import Foundation

extension QueryInput {
    static func forSortKeyCondition<AttributesType>(partitionKey: String,
                                                    targetTableName: String,
                                                    primaryKeyType: AttributesType.Type,
                                                    sortKeyCondition: AttributeCondition?,
                                                    limit: Int?,
                                                    scanIndexForward: Bool,
                                                    exclusiveStartKey: String?,
                                                    consistentRead: Bool?) throws
        -> AWSDynamoDB.QueryInput where AttributesType: PrimaryKeyAttributes
    {
        let expressionAttributeValues: [String: DynamoDBClientTypes.AttributeValue]
        let expressionAttributeNames: [String: String]
        let keyConditionExpression: String
        if let currentSortKeyCondition = sortKeyCondition {
            var withSortConditionAttributeValues: [String: DynamoDBClientTypes.AttributeValue] = [
                ":pk": DynamoDBClientTypes.AttributeValue.s(partitionKey),
            ]

            let sortKeyExpression: String
            switch currentSortKeyCondition {
            case let .equals(value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBClientTypes.AttributeValue.s(value)
                sortKeyExpression = "#sk = :sortkeyval"
            case let .lessThan(value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBClientTypes.AttributeValue.s(value)
                sortKeyExpression = "#sk < :sortkeyval"
            case let .lessThanOrEqual(value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBClientTypes.AttributeValue.s(value)
                sortKeyExpression = "#sk <= :sortkeyval"
            case let .greaterThan(value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBClientTypes.AttributeValue.s(value)
                sortKeyExpression = "#sk > :sortkeyval"
            case let .greaterThanOrEqual(value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBClientTypes.AttributeValue.s(value)
                sortKeyExpression = "#sk >= :sortkeyval"
            case let .between(value1, value2):
                withSortConditionAttributeValues[":sortkeyval1"] = DynamoDBClientTypes.AttributeValue.s(value1)
                withSortConditionAttributeValues[":sortkeyval2"] = DynamoDBClientTypes.AttributeValue.s(value2)
                sortKeyExpression = "#sk BETWEEN :sortkeyval1 AND :sortkeyval2"
            case let .beginsWith(value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBClientTypes.AttributeValue.s(value)
                sortKeyExpression = "begins_with ( #sk, :sortkeyval )"
            }

            keyConditionExpression = "#pk= :pk AND \(sortKeyExpression)"

            expressionAttributeNames = ["#pk": AttributesType.partitionKeyAttributeName,
                                        "#sk": AttributesType.sortKeyAttributeName]
            expressionAttributeValues = withSortConditionAttributeValues
        } else {
            keyConditionExpression = "#pk= :pk"

            expressionAttributeNames = ["#pk": AttributesType.partitionKeyAttributeName]
            expressionAttributeValues = [":pk": DynamoDBClientTypes.AttributeValue.s(partitionKey)]
        }

        let inputExclusiveStartKey: [String: DynamoDBClientTypes.AttributeValue]?
        if let exclusiveStartKey = exclusiveStartKey?.data(using: .utf8) {
            inputExclusiveStartKey = try JSONDecoder().decode([String: DynamoDBClientTypes.AttributeValue].self,
                                                              from: exclusiveStartKey)
        } else {
            inputExclusiveStartKey = nil
        }

        return AWSDynamoDB.QueryInput(consistentRead: consistentRead,
                                      exclusiveStartKey: inputExclusiveStartKey,
                                      expressionAttributeNames: expressionAttributeNames,
                                      expressionAttributeValues: expressionAttributeValues,
                                      indexName: primaryKeyType.indexName,
                                      keyConditionExpression: keyConditionExpression,
                                      limit: limit,
                                      scanIndexForward: scanIndexForward,
                                      tableName: targetTableName)
    }
}
