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
//  DynamoDBModel.swift
//  DynamoDBTables
//

import AWSDynamoDB

public enum DynamoDBModel {
    // MARK: - Input Types

    public struct PutItemInput: Sendable, Equatable {
        public let conditionExpression: String?
        public let expressionAttributeNames: [String: String]?
        public let expressionAttributeValues: [String: DynamoDBClientTypes.AttributeValue]?
        public let item: [String: DynamoDBClientTypes.AttributeValue]
        public let tableName: String

        public init(
            conditionExpression: String? = nil,
            expressionAttributeNames: [String: String]? = nil,
            expressionAttributeValues: [String: DynamoDBClientTypes.AttributeValue]? = nil,
            item: [String: DynamoDBClientTypes.AttributeValue],
            tableName: String
        ) {
            self.conditionExpression = conditionExpression
            self.expressionAttributeNames = expressionAttributeNames
            self.expressionAttributeValues = expressionAttributeValues
            self.item = item
            self.tableName = tableName
        }
    }

    public struct GetItemInput: Sendable, Equatable {
        public let consistentRead: Bool?
        public let key: [String: DynamoDBClientTypes.AttributeValue]
        public let tableName: String

        public init(
            consistentRead: Bool? = nil,
            key: [String: DynamoDBClientTypes.AttributeValue],
            tableName: String
        ) {
            self.consistentRead = consistentRead
            self.key = key
            self.tableName = tableName
        }
    }

    public struct DeleteItemInput: Sendable, Equatable {
        public let conditionExpression: String?
        public let expressionAttributeNames: [String: String]?
        public let expressionAttributeValues: [String: DynamoDBClientTypes.AttributeValue]?
        public let key: [String: DynamoDBClientTypes.AttributeValue]
        public let tableName: String

        public init(
            conditionExpression: String? = nil,
            expressionAttributeNames: [String: String]? = nil,
            expressionAttributeValues: [String: DynamoDBClientTypes.AttributeValue]? = nil,
            key: [String: DynamoDBClientTypes.AttributeValue],
            tableName: String
        ) {
            self.conditionExpression = conditionExpression
            self.expressionAttributeNames = expressionAttributeNames
            self.expressionAttributeValues = expressionAttributeValues
            self.key = key
            self.tableName = tableName
        }
    }

    public struct QueryInput: Sendable, Equatable {
        public let consistentRead: Bool?
        public let exclusiveStartKey: [String: DynamoDBClientTypes.AttributeValue]?
        public let expressionAttributeNames: [String: String]?
        public let expressionAttributeValues: [String: DynamoDBClientTypes.AttributeValue]?
        public let indexName: String?
        public let keyConditionExpression: String?
        public let limit: Int?
        public let scanIndexForward: Bool?
        public let tableName: String

        public init(
            consistentRead: Bool? = nil,
            exclusiveStartKey: [String: DynamoDBClientTypes.AttributeValue]? = nil,
            expressionAttributeNames: [String: String]? = nil,
            expressionAttributeValues: [String: DynamoDBClientTypes.AttributeValue]? = nil,
            indexName: String? = nil,
            keyConditionExpression: String? = nil,
            limit: Int? = nil,
            scanIndexForward: Bool? = nil,
            tableName: String
        ) {
            self.consistentRead = consistentRead
            self.exclusiveStartKey = exclusiveStartKey
            self.expressionAttributeNames = expressionAttributeNames
            self.expressionAttributeValues = expressionAttributeValues
            self.indexName = indexName
            self.keyConditionExpression = keyConditionExpression
            self.limit = limit
            self.scanIndexForward = scanIndexForward
            self.tableName = tableName
        }
    }

    public struct BatchGetItemInput: Sendable, Equatable {
        public let requestItems: [String: DynamoDBClientTypes.KeysAndAttributes]?

        public init(
            requestItems: [String: DynamoDBClientTypes.KeysAndAttributes]? = nil
        ) {
            self.requestItems = requestItems
        }
    }

    public struct BatchExecuteStatementInput: Sendable, Equatable {
        public let statements: [DynamoDBClientTypes.BatchStatementRequest]?

        public init(
            statements: [DynamoDBClientTypes.BatchStatementRequest]? = nil
        ) {
            self.statements = statements
        }
    }

    public struct ExecuteStatementInput: Sendable, Equatable {
        public let consistentRead: Bool?
        public let nextToken: String?
        public let statement: String

        public init(
            consistentRead: Bool? = nil,
            nextToken: String? = nil,
            statement: String
        ) {
            self.consistentRead = consistentRead
            self.nextToken = nextToken
            self.statement = statement
        }
    }

    public struct ExecuteTransactionInput: Sendable, Equatable {
        public let transactStatements: [DynamoDBClientTypes.ParameterizedStatement]?

        public init(
            transactStatements: [DynamoDBClientTypes.ParameterizedStatement]? = nil
        ) {
            self.transactStatements = transactStatements
        }
    }

    // MARK: - Output Types

    public struct GetItemOutput: Sendable {
        public let item: [String: DynamoDBClientTypes.AttributeValue]?

        public init(
            item: [String: DynamoDBClientTypes.AttributeValue]? = nil
        ) {
            self.item = item
        }
    }

    public struct QueryOutput: Sendable {
        public let items: [[String: DynamoDBClientTypes.AttributeValue]]?
        public let lastEvaluatedKey: [String: DynamoDBClientTypes.AttributeValue]?

        public init(
            items: [[String: DynamoDBClientTypes.AttributeValue]]? = nil,
            lastEvaluatedKey: [String: DynamoDBClientTypes.AttributeValue]? = nil
        ) {
            self.items = items
            self.lastEvaluatedKey = lastEvaluatedKey
        }
    }

    public struct BatchGetItemOutput: Sendable {
        public let responses: [String: [[String: DynamoDBClientTypes.AttributeValue]]]?
        public let unprocessedKeys: [String: DynamoDBClientTypes.KeysAndAttributes]?

        public init(
            responses: [String: [[String: DynamoDBClientTypes.AttributeValue]]]? = nil,
            unprocessedKeys: [String: DynamoDBClientTypes.KeysAndAttributes]? = nil
        ) {
            self.responses = responses
            self.unprocessedKeys = unprocessedKeys
        }
    }

    public struct BatchExecuteStatementOutput: Sendable {
        public let responses: [DynamoDBClientTypes.BatchStatementResponse]?

        public init(
            responses: [DynamoDBClientTypes.BatchStatementResponse]? = nil
        ) {
            self.responses = responses
        }
    }

    public struct ExecuteStatementOutput: Sendable {
        public let items: [[String: DynamoDBClientTypes.AttributeValue]]?
        public let nextToken: String?

        public init(
            items: [[String: DynamoDBClientTypes.AttributeValue]]? = nil,
            nextToken: String? = nil
        ) {
            self.items = items
            self.nextToken = nextToken
        }
    }
}
