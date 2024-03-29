//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/QueryInput+forSortKeyCondition.swift
// Copyright 2018-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// Licensed under Apache License v2.0
//
// Changes specified by
// https://github.com/swift-server-community/dynamo-db-tables/compare/6fec4c8..main
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

import Foundation
import DynamoDBModel

extension QueryInput {
        internal static func forSortKeyCondition<AttributesType>(partitionKey: String,
                                                                 targetTableName: String,
                                                                 primaryKeyType: AttributesType.Type,
                                                                 sortKeyCondition: AttributeCondition?,
                                                                 limit: Int?,
                                                                 scanIndexForward: Bool,
                                                                 exclusiveStartKey: String?,
                                                                 consistentRead: Bool?) throws
        -> DynamoDBModel.QueryInput where AttributesType: PrimaryKeyAttributes {
        let expressionAttributeValues: [String: DynamoDBModel.AttributeValue]
        let expressionAttributeNames: [String: String]
        let keyConditionExpression: String
        if let currentSortKeyCondition = sortKeyCondition {
            var withSortConditionAttributeValues: [String: DynamoDBModel.AttributeValue] = [
                ":pk": DynamoDBModel.AttributeValue(S: partitionKey)]

            let sortKeyExpression: String
            switch currentSortKeyCondition {
            case .equals(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue(S: value)
                sortKeyExpression = "#sk = :sortkeyval"
            case .lessThan(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue(S: value)
                sortKeyExpression = "#sk < :sortkeyval"
            case .lessThanOrEqual(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue(S: value)
                sortKeyExpression = "#sk <= :sortkeyval"
            case .greaterThan(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue(S: value)
                sortKeyExpression = "#sk > :sortkeyval"
            case .greaterThanOrEqual(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue(S: value)
                sortKeyExpression = "#sk >= :sortkeyval"
            case .between(let value1, let value2):
                withSortConditionAttributeValues[":sortkeyval1"] = DynamoDBModel.AttributeValue(S: value1)
                withSortConditionAttributeValues[":sortkeyval2"] = DynamoDBModel.AttributeValue(S: value2)
                sortKeyExpression = "#sk BETWEEN :sortkeyval1 AND :sortkeyval2"
            case .beginsWith(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue(S: value)
                sortKeyExpression = "begins_with ( #sk, :sortkeyval )"
            }

            keyConditionExpression = "#pk= :pk AND \(sortKeyExpression)"

            expressionAttributeNames = ["#pk": AttributesType.partitionKeyAttributeName,
                                        "#sk": AttributesType.sortKeyAttributeName]
            expressionAttributeValues = withSortConditionAttributeValues
        } else {
            keyConditionExpression = "#pk= :pk"

            expressionAttributeNames = ["#pk": AttributesType.partitionKeyAttributeName]
            expressionAttributeValues = [":pk": DynamoDBModel.AttributeValue(S: partitionKey)]
        }

        let inputExclusiveStartKey: [String: DynamoDBModel.AttributeValue]?
        if let exclusiveStartKey = exclusiveStartKey?.data(using: .utf8) {
            inputExclusiveStartKey = try JSONDecoder().decode([String: DynamoDBModel.AttributeValue].self,
                                                              from: exclusiveStartKey)
        } else {
            inputExclusiveStartKey = nil
        }

        return DynamoDBModel.QueryInput(consistentRead: consistentRead,
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
