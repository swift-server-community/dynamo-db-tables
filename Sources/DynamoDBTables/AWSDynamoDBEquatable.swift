//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// Copyright (c) 2025 the DynamoDBTables authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of DynamoDBTables authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

//
//  AWSDynamoDBEquatable.swift
//  DynamoDBTables
//

import AWSDynamoDB
import Foundation

// MARK: - Equatable Conformances for AWS DynamoDB Input Types

extension AWSDynamoDB.PutItemInput: @retroactive Equatable {
    public static func == (lhs: AWSDynamoDB.PutItemInput, rhs: AWSDynamoDB.PutItemInput) -> Bool {
        return lhs.tableName == rhs.tableName &&
               lhs.conditionExpression == rhs.conditionExpression &&
               lhs.item == rhs.item &&
               lhs.expressionAttributeNames == rhs.expressionAttributeNames &&
               lhs.expressionAttributeValues == rhs.expressionAttributeValues
    }
}

extension AWSDynamoDB.GetItemInput: @retroactive Equatable {
    public static func == (lhs: AWSDynamoDB.GetItemInput, rhs: AWSDynamoDB.GetItemInput) -> Bool {
        return lhs.tableName == rhs.tableName &&
               lhs.consistentRead == rhs.consistentRead &&
               lhs.key == rhs.key &&
               lhs.projectionExpression == rhs.projectionExpression &&
               lhs.expressionAttributeNames == rhs.expressionAttributeNames
    }
}

extension AWSDynamoDB.DeleteItemInput: @retroactive Equatable {
    public static func == (lhs: AWSDynamoDB.DeleteItemInput, rhs: AWSDynamoDB.DeleteItemInput) -> Bool {
        return lhs.tableName == rhs.tableName &&
               lhs.conditionExpression == rhs.conditionExpression &&
               lhs.key == rhs.key &&
               lhs.expressionAttributeNames == rhs.expressionAttributeNames &&
               lhs.expressionAttributeValues == rhs.expressionAttributeValues
    }
}

extension AWSDynamoDB.QueryInput: @retroactive Equatable {
    public static func == (lhs: AWSDynamoDB.QueryInput, rhs: AWSDynamoDB.QueryInput) -> Bool {
        return lhs.tableName == rhs.tableName &&
               lhs.keyConditionExpression == rhs.keyConditionExpression &&
               lhs.limit == rhs.limit &&
               lhs.scanIndexForward == rhs.scanIndexForward &&
               lhs.consistentRead == rhs.consistentRead &&
               lhs.exclusiveStartKey == rhs.exclusiveStartKey &&
               lhs.expressionAttributeNames == rhs.expressionAttributeNames &&
               lhs.expressionAttributeValues == rhs.expressionAttributeValues
    }
}

extension AWSDynamoDB.BatchGetItemInput: @retroactive Equatable {
    public static func == (lhs: AWSDynamoDB.BatchGetItemInput, rhs: AWSDynamoDB.BatchGetItemInput) -> Bool {
        return lhs.requestItems == rhs.requestItems
    }
}

extension AWSDynamoDB.BatchExecuteStatementInput: @retroactive Equatable {
    public static func == (lhs: AWSDynamoDB.BatchExecuteStatementInput, rhs: AWSDynamoDB.BatchExecuteStatementInput) -> Bool {
        return lhs.statements == rhs.statements
    }
}

extension AWSDynamoDB.ExecuteStatementInput: @retroactive Equatable {
    public static func == (lhs: AWSDynamoDB.ExecuteStatementInput, rhs: AWSDynamoDB.ExecuteStatementInput) -> Bool {
        return lhs.statement == rhs.statement &&
               lhs.consistentRead == rhs.consistentRead &&
               lhs.nextToken == rhs.nextToken &&
               lhs.parameters == rhs.parameters
    }
}

extension AWSDynamoDB.ExecuteTransactionInput: @retroactive Equatable {
    public static func == (lhs: AWSDynamoDB.ExecuteTransactionInput, rhs: AWSDynamoDB.ExecuteTransactionInput) -> Bool {
        return lhs.transactStatements == rhs.transactStatements &&
               lhs.clientRequestToken == rhs.clientRequestToken
    }
}
/*
// MARK: - Equatable Conformances for AWS DynamoDB Output Types

extension AWSDynamoDB.PutItemOutput: @retroactive Equatable {
    public static func == (lhs: AWSDynamoDB.PutItemOutput, rhs: AWSDynamoDB.PutItemOutput) -> Bool {
        return lhs.attributes == rhs.attributes &&
               lhs.consumedCapacity == rhs.consumedCapacity &&
               lhs.itemCollectionMetrics == rhs.itemCollectionMetrics
    }
}

extension AWSDynamoDB.GetItemOutput: @retroactive Equatable {
    public static func == (lhs: AWSDynamoDB.GetItemOutput, rhs: AWSDynamoDB.GetItemOutput) -> Bool {
        return lhs.item == rhs.item &&
               lhs.consumedCapacity == rhs.consumedCapacity
    }
}

extension AWSDynamoDB.DeleteItemOutput: @retroactive Equatable {
    public static func == (lhs: AWSDynamoDB.DeleteItemOutput, rhs: AWSDynamoDB.DeleteItemOutput) -> Bool {
        return lhs.attributes == rhs.attributes &&
               lhs.consumedCapacity == rhs.consumedCapacity &&
               lhs.itemCollectionMetrics == rhs.itemCollectionMetrics
    }
}

extension AWSDynamoDB.QueryOutput: @retroactive Equatable {
    public static func == (lhs: AWSDynamoDB.QueryOutput, rhs: AWSDynamoDB.QueryOutput) -> Bool {
        return lhs.count == rhs.count &&
               lhs.scannedCount == rhs.scannedCount &&
               compareItemArrays(lhs.items, rhs.items) &&
               lhs.lastEvaluatedKey == rhs.lastEvaluatedKey &&
               lhs.consumedCapacity == rhs.consumedCapacity
    }
}

extension AWSDynamoDB.BatchGetItemOutput: @retroactive Equatable {
    public static func == (lhs: AWSDynamoDB.BatchGetItemOutput, rhs: AWSDynamoDB.BatchGetItemOutput) -> Bool {
        return compareResponseMaps(lhs.responses, rhs.responses) &&
               compareUnprocessedKeyMaps(lhs.unprocessedKeys, rhs.unprocessedKeys) &&
               compareConsumedCapacityArrays(lhs.consumedCapacity, rhs.consumedCapacity)
    }
}

extension AWSDynamoDB.BatchExecuteStatementOutput: @retroactive Equatable {
    public static func == (lhs: AWSDynamoDB.BatchExecuteStatementOutput, rhs: AWSDynamoDB.BatchExecuteStatementOutput) -> Bool {
        return compareBatchStatementResponseArrays(lhs.responses, rhs.responses) &&
               compareConsumedCapacityArrays(lhs.consumedCapacity, rhs.consumedCapacity)
    }
}

extension AWSDynamoDB.ExecuteStatementOutput: @retroactive Equatable {
    public static func == (lhs: AWSDynamoDB.ExecuteStatementOutput, rhs: AWSDynamoDB.ExecuteStatementOutput) -> Bool {
        return compareItemArrays(lhs.items, rhs.items) &&
               lhs.nextToken == rhs.nextToken &&
               lhs.consumedCapacity == rhs.consumedCapacity &&
               lhs.lastEvaluatedKey == rhs.lastEvaluatedKey
    }
}

extension AWSDynamoDB.ExecuteTransactionOutput: @retroactive Equatable {
    public static func == (lhs: AWSDynamoDB.ExecuteTransactionOutput, rhs: AWSDynamoDB.ExecuteTransactionOutput) -> Bool {
        return compareItemTransactionResponseArrays(lhs.responses, rhs.responses) &&
               compareConsumedCapacityArrays(lhs.consumedCapacity, rhs.consumedCapacity)
    }
}

// MARK: - Supporting Type Conformances

extension DynamoDBClientTypes.ConsumedCapacity: @retroactive Equatable {
    public static func == (lhs: DynamoDBClientTypes.ConsumedCapacity, rhs: DynamoDBClientTypes.ConsumedCapacity) -> Bool {
        return lhs.tableName == rhs.tableName &&
               lhs.capacityUnits == rhs.capacityUnits &&
               lhs.readCapacityUnits == rhs.readCapacityUnits &&
               lhs.writeCapacityUnits == rhs.writeCapacityUnits
    }
}

extension DynamoDBClientTypes.ItemCollectionMetrics: @retroactive Equatable {
    public static func == (lhs: DynamoDBClientTypes.ItemCollectionMetrics, rhs: DynamoDBClientTypes.ItemCollectionMetrics) -> Bool {
        return lhs.itemCollectionKey == rhs.itemCollectionKey &&
               lhs.sizeEstimateRangeGb == rhs.sizeEstimateRangeGb
    }
}
*/
extension DynamoDBClientTypes.KeysAndAttributes: @retroactive Equatable {
    public static func == (lhs: DynamoDBClientTypes.KeysAndAttributes, rhs: DynamoDBClientTypes.KeysAndAttributes) -> Bool {
        return lhs.keys == rhs.keys &&
               lhs.consistentRead == rhs.consistentRead &&
               lhs.attributesToGet == rhs.attributesToGet &&
               lhs.projectionExpression == rhs.projectionExpression &&
               lhs.expressionAttributeNames == rhs.expressionAttributeNames
    }
}

extension DynamoDBClientTypes.BatchStatementRequest: @retroactive Equatable {
    public static func == (lhs: DynamoDBClientTypes.BatchStatementRequest, rhs: DynamoDBClientTypes.BatchStatementRequest) -> Bool {
        return lhs.statement == rhs.statement &&
               lhs.consistentRead == rhs.consistentRead &&
               lhs.parameters == rhs.parameters
    }
}

extension DynamoDBClientTypes.ParameterizedStatement: @retroactive Equatable {
    public static func == (lhs: DynamoDBClientTypes.ParameterizedStatement, rhs: DynamoDBClientTypes.ParameterizedStatement) -> Bool {
        return lhs.statement == rhs.statement &&
               lhs.parameters == rhs.parameters
    }
}
/*
extension DynamoDBClientTypes.BatchStatementResponse: @retroactive Equatable {
    public static func == (lhs: DynamoDBClientTypes.BatchStatementResponse, rhs: DynamoDBClientTypes.BatchStatementResponse) -> Bool {
        return lhs.error == rhs.error &&
               lhs.tableName == rhs.tableName &&
               lhs.item == rhs.item
    }
}

extension DynamoDBClientTypes.BatchStatementError: @retroactive Equatable {
    public static func == (lhs: DynamoDBClientTypes.BatchStatementError, rhs: DynamoDBClientTypes.BatchStatementError) -> Bool {
        return lhs.code == rhs.code &&
               lhs.message == rhs.message &&
               lhs.item == rhs.item
    }
}

extension DynamoDBClientTypes.ItemResponse: @retroactive Equatable {
    public static func == (lhs: DynamoDBClientTypes.ItemResponse, rhs: DynamoDBClientTypes.ItemResponse) -> Bool {
        return lhs.item == rhs.item
    }
}

// MARK: - Helper Functions

private func compareItemArrays(
    _ lhs: [[String: DynamoDBClientTypes.AttributeValue]]?,
    _ rhs: [[String: DynamoDBClientTypes.AttributeValue]]?
) -> Bool {
    switch (lhs, rhs) {
    case (nil, nil):
        return true
    case (nil, _), (_, nil):
        return false
    case let (lhsArray?, rhsArray?):
        guard lhsArray.count == rhsArray.count else { return false }
        
        for (lhsItem, rhsItem) in zip(lhsArray, rhsArray) {
            if !compareAttributeValueMaps(lhsItem, rhsItem) {
                return false
            }
        }
        return true
    }
}

private func compareKeyArrays(
    _ lhs: [[String: DynamoDBClientTypes.AttributeValue]]?,
    _ rhs: [[String: DynamoDBClientTypes.AttributeValue]]?
) -> Bool {
    return compareItemArrays(lhs, rhs)
}

private func compareResponseMaps(
    _ lhs: [String: [[String: DynamoDBClientTypes.AttributeValue]]]?,
    _ rhs: [String: [[String: DynamoDBClientTypes.AttributeValue]]]?
) -> Bool {
    switch (lhs, rhs) {
    case (nil, nil):
        return true
    case (nil, _), (_, nil):
        return false
    case let (lhsMap?, rhsMap?):
        guard lhsMap.count == rhsMap.count else { return false }
        
        for (tableName, lhsItems) in lhsMap {
            guard let rhsItems = rhsMap[tableName],
                  compareItemArrays(lhsItems, rhsItems) else {
                return false
            }
        }
        return true
    }
}

private func compareUnprocessedKeyMaps(
    _ lhs: [String: DynamoDBClientTypes.KeysAndAttributes]?,
    _ rhs: [String: DynamoDBClientTypes.KeysAndAttributes]?
) -> Bool {
    switch (lhs, rhs) {
    case (nil, nil):
        return true
    case (nil, _), (_, nil):
        return false
    case let (lhsMap?, rhsMap?):
        guard lhsMap.count == rhsMap.count else { return false }
        
        for (tableName, lhsKeys) in lhsMap {
            guard let rhsKeys = rhsMap[tableName],
                  lhsKeys == rhsKeys else {
                return false
            }
        }
        return true
    }
}

private func compareConsumedCapacityArrays(
    _ lhs: [DynamoDBClientTypes.ConsumedCapacity]?,
    _ rhs: [DynamoDBClientTypes.ConsumedCapacity]?
) -> Bool {
    switch (lhs, rhs) {
    case (nil, nil):
        return true
    case (nil, _), (_, nil):
        return false
    case let (lhsArray?, rhsArray?):
        guard lhsArray.count == rhsArray.count else { return false }
        
        for (lhsCapacity, rhsCapacity) in zip(lhsArray, rhsArray) {
            if lhsCapacity != rhsCapacity {
                return false
            }
        }
        return true
    }
}

private func compareBatchStatementResponseArrays(
    _ lhs: [DynamoDBClientTypes.BatchStatementResponse]?,
    _ rhs: [DynamoDBClientTypes.BatchStatementResponse]?
) -> Bool {
    switch (lhs, rhs) {
    case (nil, nil):
        return true
    case (nil, _), (_, nil):
        return false
    case let (lhsArray?, rhsArray?):
        guard lhsArray.count == rhsArray.count else { return false }
        
        for (lhsResponse, rhsResponse) in zip(lhsArray, rhsArray) {
            if lhsResponse != rhsResponse {
                return false
            }
        }
        return true
    }
}

private func compareItemTransactionResponseArrays(
    _ lhs: [DynamoDBClientTypes.ItemResponse]?,
    _ rhs: [DynamoDBClientTypes.ItemResponse]?
) -> Bool {
    switch (lhs, rhs) {
    case (nil, nil):
        return true
    case (nil, _), (_, nil):
        return false
    case let (lhsArray?, rhsArray?):
        guard lhsArray.count == rhsArray.count else { return false }
        
        for (lhsResponse, rhsResponse) in zip(lhsArray, rhsArray) {
            if lhsResponse != rhsResponse {
                return false
            }
        }
        return true
    }
}

private func compareRequestItemMaps(
    _ lhs: [String: DynamoDBClientTypes.KeysAndAttributes]?,
    _ rhs: [String: DynamoDBClientTypes.KeysAndAttributes]?
) -> Bool {
    switch (lhs, rhs) {
    case (nil, nil):
        return true
    case (nil, _), (_, nil):
        return false
    case let (lhsMap?, rhsMap?):
        guard lhsMap.count == rhsMap.count else { return false }
        
        for (tableName, lhsKeys) in lhsMap {
            guard let rhsKeys = rhsMap[tableName],
                  lhsKeys == rhsKeys else {
                return false
            }
        }
        return true
    }
}

private func compareBatchStatementRequestArrays(
    _ lhs: [DynamoDBClientTypes.BatchStatementRequest]?,
    _ rhs: [DynamoDBClientTypes.BatchStatementRequest]?
) -> Bool {
    switch (lhs, rhs) {
    case (nil, nil):
        return true
    case (nil, _), (_, nil):
        return false
    case let (lhsArray?, rhsArray?):
        guard lhsArray.count == rhsArray.count else { return false }
        
        for (lhsRequest, rhsRequest) in zip(lhsArray, rhsArray) {
            if lhsRequest != rhsRequest {
                return false
            }
        }
        return true
    }
}

private func compareParameterizedStatementArrays(
    _ lhs: [DynamoDBClientTypes.ParameterizedStatement]?,
    _ rhs: [DynamoDBClientTypes.ParameterizedStatement]?
) -> Bool {
    switch (lhs, rhs) {
    case (nil, nil):
        return true
    case (nil, _), (_, nil):
        return false
    case let (lhsArray?, rhsArray?):
        guard lhsArray.count == rhsArray.count else { return false }
        
        for (lhsStatement, rhsStatement) in zip(lhsArray, rhsArray) {
            if lhsStatement != rhsStatement {
                return false
            }
        }
        return true
    }
}

private func compareAttributeValueArrays(
    _ lhs: [DynamoDBClientTypes.AttributeValue]?,
    _ rhs: [DynamoDBClientTypes.AttributeValue]?
) -> Bool {
    switch (lhs, rhs) {
    case (nil, nil):
        return true
    case (nil, _), (_, nil):
        return false
    case let (lhsArray?, rhsArray?):
        guard lhsArray.count == rhsArray.count else { return false }
        
        for (lhsValue, rhsValue) in zip(lhsArray, rhsArray) {
            if lhsValue != rhsValue {
                return false
            }
        }
        return true
    }
}*/