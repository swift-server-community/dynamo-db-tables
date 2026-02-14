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

// MARK: - Equatable Conformances for AWS DynamoDB Supporting Types

extension DynamoDBClientTypes.KeysAndAttributes: @retroactive Equatable {
    public static func == (
        lhs: DynamoDBClientTypes.KeysAndAttributes,
        rhs: DynamoDBClientTypes.KeysAndAttributes
    ) -> Bool {
        return lhs.keys == rhs.keys && lhs.consistentRead == rhs.consistentRead
            && lhs.attributesToGet == rhs.attributesToGet && lhs.projectionExpression == rhs.projectionExpression
            && lhs.expressionAttributeNames == rhs.expressionAttributeNames
    }
}

extension DynamoDBClientTypes.BatchStatementRequest: @retroactive Equatable {
    public static func == (
        lhs: DynamoDBClientTypes.BatchStatementRequest,
        rhs: DynamoDBClientTypes.BatchStatementRequest
    ) -> Bool {
        return lhs.statement == rhs.statement && lhs.consistentRead == rhs.consistentRead
            && lhs.parameters == rhs.parameters
    }
}

extension DynamoDBClientTypes.ParameterizedStatement: @retroactive Equatable {
    public static func == (
        lhs: DynamoDBClientTypes.ParameterizedStatement,
        rhs: DynamoDBClientTypes.ParameterizedStatement
    ) -> Bool {
        return lhs.statement == rhs.statement && lhs.parameters == rhs.parameters
    }
}
