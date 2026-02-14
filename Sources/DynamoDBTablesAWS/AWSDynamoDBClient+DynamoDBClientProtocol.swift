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
//  AWSDynamoDBClient+DynamoDBClientProtocol.swift
//  DynamoDBTablesAWS
//

#if AWSSDK
import AWSDynamoDB
import DynamoDBTables

extension DynamoDBClient: DynamoDBClientProtocol {
    public func putItem(input: DynamoDBModel.PutItemInput) async throws {
        let sdkInput = AWSDynamoDB.PutItemInput(
            conditionExpression: input.conditionExpression,
            expressionAttributeNames: input.expressionAttributeNames,
            expressionAttributeValues: input.expressionAttributeValues,
            item: input.item,
            tableName: input.tableName
        )
        _ = try await self.putItem(input: sdkInput)
    }

    public func getItem(input: DynamoDBModel.GetItemInput) async throws -> DynamoDBModel.GetItemOutput {
        let sdkInput = AWSDynamoDB.GetItemInput(
            consistentRead: input.consistentRead,
            key: input.key,
            tableName: input.tableName
        )
        let sdkOutput = try await self.getItem(input: sdkInput)
        return DynamoDBModel.GetItemOutput(item: sdkOutput.item)
    }

    public func deleteItem(input: DynamoDBModel.DeleteItemInput) async throws {
        let sdkInput = AWSDynamoDB.DeleteItemInput(
            conditionExpression: input.conditionExpression,
            expressionAttributeNames: input.expressionAttributeNames,
            expressionAttributeValues: input.expressionAttributeValues,
            key: input.key,
            tableName: input.tableName
        )
        _ = try await self.deleteItem(input: sdkInput)
    }

    public func query(input: DynamoDBModel.QueryInput) async throws -> DynamoDBModel.QueryOutput {
        let sdkInput = AWSDynamoDB.QueryInput(
            consistentRead: input.consistentRead,
            exclusiveStartKey: input.exclusiveStartKey,
            expressionAttributeNames: input.expressionAttributeNames,
            expressionAttributeValues: input.expressionAttributeValues,
            indexName: input.indexName,
            keyConditionExpression: input.keyConditionExpression,
            limit: input.limit,
            scanIndexForward: input.scanIndexForward,
            tableName: input.tableName
        )
        let sdkOutput = try await self.query(input: sdkInput)
        return DynamoDBModel.QueryOutput(
            items: sdkOutput.items,
            lastEvaluatedKey: sdkOutput.lastEvaluatedKey
        )
    }

    public func batchGetItem(input: DynamoDBModel.BatchGetItemInput) async throws -> DynamoDBModel.BatchGetItemOutput {
        let sdkInput = AWSDynamoDB.BatchGetItemInput(
            requestItems: input.requestItems
        )
        let sdkOutput = try await self.batchGetItem(input: sdkInput)
        return DynamoDBModel.BatchGetItemOutput(
            responses: sdkOutput.responses,
            unprocessedKeys: sdkOutput.unprocessedKeys
        )
    }

    public func batchExecuteStatement(
        input: DynamoDBModel.BatchExecuteStatementInput
    ) async throws -> DynamoDBModel.BatchExecuteStatementOutput {
        let sdkInput = AWSDynamoDB.BatchExecuteStatementInput(
            statements: input.statements
        )
        let sdkOutput = try await self.batchExecuteStatement(input: sdkInput)
        return DynamoDBModel.BatchExecuteStatementOutput(
            responses: sdkOutput.responses
        )
    }

    public func executeStatement(
        input: DynamoDBModel.ExecuteStatementInput
    ) async throws -> DynamoDBModel.ExecuteStatementOutput {
        let sdkInput = AWSDynamoDB.ExecuteStatementInput(
            consistentRead: input.consistentRead,
            nextToken: input.nextToken,
            statement: input.statement
        )
        let sdkOutput = try await self.executeStatement(input: sdkInput)
        return DynamoDBModel.ExecuteStatementOutput(
            items: sdkOutput.items,
            nextToken: sdkOutput.nextToken
        )
    }

    public func executeTransaction(input: DynamoDBModel.ExecuteTransactionInput) async throws {
        let sdkInput = AWSDynamoDB.ExecuteTransactionInput(
            transactStatements: input.transactStatements
        )
        _ = try await self.executeTransaction(input: sdkInput)
    }
}
#endif
