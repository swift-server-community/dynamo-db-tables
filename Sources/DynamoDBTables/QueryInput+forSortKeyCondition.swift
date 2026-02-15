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
//  QueryInput+forSortKeyCondition.swift
//  DynamoDBTables
//

// swiftlint:disable:next unused_import
import Foundation

extension DynamoDBModel.QueryInput {
    static func forSortKeyCondition<AttributesType>(
        partitionKey: String,
        targetTableName: String,
        primaryKeyType: AttributesType.Type,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        scanIndexForward: Bool,
        exclusiveStartKey: String?,
        consistentRead: Bool?
    ) throws
        -> DynamoDBModel.QueryInput where AttributesType: PrimaryKeyAttributes
    {
        let expressionAttributeValues: [String: DynamoDBModel.AttributeValue]
        let expressionAttributeNames: [String: String]
        let keyConditionExpression: String
        if let currentSortKeyCondition = sortKeyCondition {
            var withSortConditionAttributeValues: [String: DynamoDBModel.AttributeValue] = [
                ":pk": DynamoDBModel.AttributeValue.s(partitionKey)
            ]

            let sortKeyExpression: String
            switch currentSortKeyCondition {
            case let .equals(value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue.s(value)
                sortKeyExpression = "#sk = :sortkeyval"
            case let .lessThan(value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue.s(value)
                sortKeyExpression = "#sk < :sortkeyval"
            case let .lessThanOrEqual(value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue.s(value)
                sortKeyExpression = "#sk <= :sortkeyval"
            case let .greaterThan(value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue.s(value)
                sortKeyExpression = "#sk > :sortkeyval"
            case let .greaterThanOrEqual(value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue.s(value)
                sortKeyExpression = "#sk >= :sortkeyval"
            case let .between(value1, value2):
                withSortConditionAttributeValues[":sortkeyval1"] = DynamoDBModel.AttributeValue.s(value1)
                withSortConditionAttributeValues[":sortkeyval2"] = DynamoDBModel.AttributeValue.s(value2)
                sortKeyExpression = "#sk BETWEEN :sortkeyval1 AND :sortkeyval2"
            case let .beginsWith(value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue.s(value)
                sortKeyExpression = "begins_with ( #sk, :sortkeyval )"
            }

            keyConditionExpression = "#pk= :pk AND \(sortKeyExpression)"

            expressionAttributeNames = [
                "#pk": AttributesType.partitionKeyAttributeName,
                "#sk": AttributesType.sortKeyAttributeName,
            ]
            expressionAttributeValues = withSortConditionAttributeValues
        } else {
            keyConditionExpression = "#pk= :pk"

            expressionAttributeNames = ["#pk": AttributesType.partitionKeyAttributeName]
            expressionAttributeValues = [":pk": DynamoDBModel.AttributeValue.s(partitionKey)]
        }

        let inputExclusiveStartKey: [String: DynamoDBModel.AttributeValue]? =
            if let exclusiveStartKey = exclusiveStartKey?.data(using: .utf8) {
                try JSONDecoder().decode(
                    [String: DynamoDBModel.AttributeValue].self,
                    from: exclusiveStartKey
                )
            } else {
                nil
            }

        return DynamoDBModel.QueryInput(
            consistentRead: consistentRead,
            exclusiveStartKey: inputExclusiveStartKey,
            expressionAttributeNames: expressionAttributeNames,
            expressionAttributeValues: expressionAttributeValues,
            indexName: primaryKeyType.indexName,
            keyConditionExpression: keyConditionExpression,
            limit: limit,
            scanIndexForward: scanIndexForward,
            tableName: targetTableName
        )
    }
}
