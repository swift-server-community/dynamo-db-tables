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

// MARK: - AttributeValue Conversions

extension DynamoDBModel.AttributeValue {
    var toSDK: DynamoDBClientTypes.AttributeValue {
        switch self {
        case .s(let value): .s(value)
        case .n(let value): .n(value)
        case .b(let value): .b(value)
        case .ss(let value): .ss(value)
        case .ns(let value): .ns(value)
        case .bs(let value): .bs(value)
        case .m(let value): .m(value.mapValues(\.toSDK))
        case .l(let value): .l(value.map(\.toSDK))
        case .null(let value): .null(value)
        case .bool(let value): .bool(value)
        case .sdkUnknown(let value): .sdkUnknown(value)
        }
    }
}

extension DynamoDBClientTypes.AttributeValue {
    var toDynamoDBModel: DynamoDBModel.AttributeValue {
        switch self {
        case .s(let value): .s(value)
        case .n(let value): .n(value)
        case .b(let value): .b(value)
        case .ss(let value): .ss(value)
        case .ns(let value): .ns(value)
        case .bs(let value): .bs(value)
        case .m(let value): .m(value.mapValues(\.toDynamoDBModel))
        case .l(let value): .l(value.map(\.toDynamoDBModel))
        case .null(let value): .null(value)
        case .bool(let value): .bool(value)
        case .sdkUnknown(let value): .sdkUnknown(value)
        }
    }
}

// MARK: - Dictionary Conversions

extension [String: DynamoDBModel.AttributeValue] {
    var toSDK: [String: DynamoDBClientTypes.AttributeValue] {
        self.mapValues(\.toSDK)
    }
}

extension [String: DynamoDBClientTypes.AttributeValue] {
    var toDynamoDBModel: [String: DynamoDBModel.AttributeValue] {
        self.mapValues(\.toDynamoDBModel)
    }
}

// MARK: - KeysAndAttributes Conversions

extension DynamoDBModel.KeysAndAttributes {
    var toSDK: DynamoDBClientTypes.KeysAndAttributes {
        DynamoDBClientTypes.KeysAndAttributes(
            attributesToGet: self.attributesToGet,
            consistentRead: self.consistentRead,
            expressionAttributeNames: self.expressionAttributeNames,
            keys: self.keys.map(\.toSDK),
            projectionExpression: self.projectionExpression
        )
    }
}

extension DynamoDBClientTypes.KeysAndAttributes {
    var toDynamoDBModel: DynamoDBModel.KeysAndAttributes {
        DynamoDBModel.KeysAndAttributes(
            attributesToGet: self.attributesToGet,
            consistentRead: self.consistentRead,
            expressionAttributeNames: self.expressionAttributeNames,
            keys: (self.keys ?? []).map(\.toDynamoDBModel),
            projectionExpression: self.projectionExpression
        )
    }
}

// MARK: - BatchStatementRequest Conversions

extension DynamoDBModel.BatchStatementRequest {
    var toSDK: DynamoDBClientTypes.BatchStatementRequest {
        DynamoDBClientTypes.BatchStatementRequest(
            consistentRead: self.consistentRead,
            parameters: self.parameters?.map(\.toSDK),
            statement: self.statement
        )
    }
}

// MARK: - BatchStatementResponse Conversions

extension DynamoDBClientTypes.BatchStatementResponse {
    var toDynamoDBModel: DynamoDBModel.BatchStatementResponse {
        DynamoDBModel.BatchStatementResponse(
            error: self.error?.toDynamoDBModel,
            item: self.item?.toDynamoDBModel,
            tableName: self.tableName
        )
    }
}

// MARK: - BatchStatementError Conversions

extension DynamoDBClientTypes.BatchStatementError {
    var toDynamoDBModel: DynamoDBModel.BatchStatementError {
        DynamoDBModel.BatchStatementError(
            code: self.code?.toDynamoDBModel,
            message: self.message
        )
    }
}

// MARK: - BatchStatementErrorCode Conversions

extension DynamoDBClientTypes.BatchStatementErrorCodeEnum {
    var toDynamoDBModel: DynamoDBModel.BatchStatementErrorCode {
        switch self {
        case .accessdenied: .accessdenied
        case .conditionalcheckfailed: .conditionalcheckfailed
        case .duplicateitem: .duplicateitem
        case .internalservererror: .internalservererror
        case .itemcollectionsizelimitexceeded: .itemcollectionsizelimitexceeded
        case .provisionedthroughputexceeded: .provisionedthroughputexceeded
        case .requestlimitexceeded: .requestlimitexceeded
        case .resourcenotfound: .resourcenotfound
        case .throttlingerror: .throttlingerror
        case .transactionconflict: .transactionconflict
        case .validationerror: .validationerror
        case .sdkUnknown(let value): .sdkUnknown(value)
        }
    }
}

// MARK: - ParameterizedStatement Conversions

extension DynamoDBModel.ParameterizedStatement {
    var toSDK: DynamoDBClientTypes.ParameterizedStatement {
        DynamoDBClientTypes.ParameterizedStatement(
            parameters: self.parameters?.map(\.toSDK),
            statement: self.statement
        )
    }
}

// MARK: - CancellationReason Conversions

extension DynamoDBClientTypes.CancellationReason {
    var toDynamoDBModel: DynamoDBModel.CancellationReason {
        DynamoDBModel.CancellationReason(
            code: self.code,
            item: self.item?.toDynamoDBModel,
            message: self.message
        )
    }
}

// MARK: - DynamoDBClientProtocol Conformance

extension DynamoDBClient: DynamoDBClientProtocol {
    public func putItem(input: DynamoDBModel.PutItemInput) async throws {
        let sdkInput = AWSDynamoDB.PutItemInput(
            conditionExpression: input.conditionExpression,
            expressionAttributeNames: input.expressionAttributeNames,
            expressionAttributeValues: input.expressionAttributeValues?.toSDK,
            item: input.item.toSDK,
            tableName: input.tableName
        )
        _ = try await self.putItem(input: sdkInput)
    }

    public func getItem(input: DynamoDBModel.GetItemInput) async throws -> DynamoDBModel.GetItemOutput {
        let sdkInput = AWSDynamoDB.GetItemInput(
            consistentRead: input.consistentRead,
            key: input.key.toSDK,
            tableName: input.tableName
        )
        let sdkOutput = try await self.getItem(input: sdkInput)
        return DynamoDBModel.GetItemOutput(item: sdkOutput.item?.toDynamoDBModel)
    }

    public func deleteItem(input: DynamoDBModel.DeleteItemInput) async throws {
        let sdkInput = AWSDynamoDB.DeleteItemInput(
            conditionExpression: input.conditionExpression,
            expressionAttributeNames: input.expressionAttributeNames,
            expressionAttributeValues: input.expressionAttributeValues?.toSDK,
            key: input.key.toSDK,
            tableName: input.tableName
        )
        _ = try await self.deleteItem(input: sdkInput)
    }

    public func query(input: DynamoDBModel.QueryInput) async throws -> DynamoDBModel.QueryOutput {
        let sdkInput = AWSDynamoDB.QueryInput(
            consistentRead: input.consistentRead,
            exclusiveStartKey: input.exclusiveStartKey?.toSDK,
            expressionAttributeNames: input.expressionAttributeNames,
            expressionAttributeValues: input.expressionAttributeValues?.toSDK,
            indexName: input.indexName,
            keyConditionExpression: input.keyConditionExpression,
            limit: input.limit,
            scanIndexForward: input.scanIndexForward,
            tableName: input.tableName
        )
        let sdkOutput = try await self.query(input: sdkInput)
        return DynamoDBModel.QueryOutput(
            items: sdkOutput.items?.map(\.toDynamoDBModel),
            lastEvaluatedKey: sdkOutput.lastEvaluatedKey?.toDynamoDBModel
        )
    }

    public func batchGetItem(input: DynamoDBModel.BatchGetItemInput) async throws -> DynamoDBModel.BatchGetItemOutput {
        let sdkInput = AWSDynamoDB.BatchGetItemInput(
            requestItems: input.requestItems?.mapValues(\.toSDK)
        )
        let sdkOutput = try await self.batchGetItem(input: sdkInput)
        return DynamoDBModel.BatchGetItemOutput(
            responses: sdkOutput.responses?.mapValues { items in items.map(\.toDynamoDBModel) },
            unprocessedKeys: sdkOutput.unprocessedKeys?.mapValues(\.toDynamoDBModel)
        )
    }

    public func batchExecuteStatement(
        input: DynamoDBModel.BatchExecuteStatementInput
    ) async throws -> DynamoDBModel.BatchExecuteStatementOutput {
        let sdkInput = AWSDynamoDB.BatchExecuteStatementInput(
            statements: input.statements?.map(\.toSDK)
        )
        let sdkOutput = try await self.batchExecuteStatement(input: sdkInput)
        return DynamoDBModel.BatchExecuteStatementOutput(
            responses: sdkOutput.responses?.map(\.toDynamoDBModel)
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
            items: sdkOutput.items?.map(\.toDynamoDBModel),
            nextToken: sdkOutput.nextToken
        )
    }

    public func executeTransaction(input: DynamoDBModel.ExecuteTransactionInput) async throws {
        let sdkInput = AWSDynamoDB.ExecuteTransactionInput(
            transactStatements: input.transactStatements?.map(\.toSDK)
        )
        _ = try await self.executeTransaction(input: sdkInput)
    }
}
#endif
