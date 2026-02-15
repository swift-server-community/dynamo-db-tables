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
//  SotoDynamoDB+DynamoDBClientProtocol.swift
//  DynamoDBTablesSoto
//

#if SOTOSDK
import DynamoDBTables
import SotoDynamoDB

// MARK: - AttributeValue Conversions

extension DynamoDBModel.AttributeValue {
    var toSoto: DynamoDB.AttributeValue {
        switch self {
        case .s(let value): .s(value)
        case .n(let value): .n(value)
        case .b(let value): .b(.data(value))
        case .ss(let value): .ss(value)
        case .ns(let value): .ns(value)
        case .bs(let value): .bs(value.map { .data($0) })
        case .m(let value): .m(value.mapValues(\.toSoto))
        case .l(let value): .l(value.map(\.toSoto))
        case .null(let value): .null(value)
        case .bool(let value): .bool(value)
        case .sdkUnknown(let value): .s(value)
        }
    }
}

extension DynamoDB.AttributeValue {
    var toDynamoDBModel: DynamoDBModel.AttributeValue {
        switch self {
        case .s(let value): .s(value)
        case .n(let value): .n(value)
        case .b(let value): .b(Data(value.decoded() ?? []))
        case .ss(let value): .ss(value)
        case .ns(let value): .ns(value)
        case .bs(let value): .bs(value.map { Data($0.decoded() ?? []) })
        case .m(let value): .m(value.mapValues(\.toDynamoDBModel))
        case .l(let value): .l(value.map(\.toDynamoDBModel))
        case .null(let value): .null(value)
        case .bool(let value): .bool(value)
        }
    }
}

// MARK: - Dictionary Conversions

extension [String: DynamoDBModel.AttributeValue] {
    var toSoto: [String: DynamoDB.AttributeValue] {
        self.mapValues(\.toSoto)
    }
}

extension [String: DynamoDB.AttributeValue] {
    var toDynamoDBModel: [String: DynamoDBModel.AttributeValue] {
        self.mapValues(\.toDynamoDBModel)
    }
}

// MARK: - KeysAndAttributes Conversions

extension DynamoDBModel.KeysAndAttributes {
    var toSoto: DynamoDB.KeysAndAttributes {
        DynamoDB.KeysAndAttributes(
            attributesToGet: self.attributesToGet,
            consistentRead: self.consistentRead,
            expressionAttributeNames: self.expressionAttributeNames,
            keys: self.keys.map(\.toSoto),
            projectionExpression: self.projectionExpression
        )
    }
}

extension DynamoDB.KeysAndAttributes {
    var toDynamoDBModel: DynamoDBModel.KeysAndAttributes {
        DynamoDBModel.KeysAndAttributes(
            attributesToGet: self.attributesToGet,
            consistentRead: self.consistentRead,
            expressionAttributeNames: self.expressionAttributeNames,
            keys: self.keys.map(\.toDynamoDBModel),
            projectionExpression: self.projectionExpression
        )
    }
}

// MARK: - BatchStatementRequest Conversions

extension DynamoDBModel.BatchStatementRequest {
    var toSoto: DynamoDB.BatchStatementRequest {
        DynamoDB.BatchStatementRequest(
            consistentRead: self.consistentRead,
            parameters: self.parameters?.map(\.toSoto),
            statement: self.statement ?? ""
        )
    }
}

// MARK: - BatchStatementResponse Conversions

extension DynamoDB.BatchStatementResponse {
    var toDynamoDBModel: DynamoDBModel.BatchStatementResponse {
        DynamoDBModel.BatchStatementResponse(
            error: self.error?.toDynamoDBModel,
            item: self.item?.toDynamoDBModel,
            tableName: self.tableName
        )
    }
}

// MARK: - BatchStatementError Conversions

extension DynamoDB.BatchStatementError {
    var toDynamoDBModel: DynamoDBModel.BatchStatementError {
        DynamoDBModel.BatchStatementError(
            code: self.code?.toDynamoDBModel,
            message: self.message
        )
    }
}

// MARK: - BatchStatementErrorCode Conversions

extension DynamoDB.BatchStatementErrorCodeEnum {
    var toDynamoDBModel: DynamoDBModel.BatchStatementErrorCode {
        switch self {
        case .accessDenied: .accessdenied
        case .conditionalCheckFailed: .conditionalcheckfailed
        case .duplicateItem: .duplicateitem
        case .internalServerError: .internalservererror
        case .itemCollectionSizeLimitExceeded: .itemcollectionsizelimitexceeded
        case .provisionedThroughputExceeded: .provisionedthroughputexceeded
        case .requestLimitExceeded: .requestlimitexceeded
        case .resourceNotFound: .resourcenotfound
        case .throttlingError: .throttlingerror
        case .transactionConflict: .transactionconflict
        case .validationError: .validationerror
        }
    }
}

// MARK: - ParameterizedStatement Conversions

extension DynamoDBModel.ParameterizedStatement {
    var toSoto: DynamoDB.ParameterizedStatement {
        DynamoDB.ParameterizedStatement(
            parameters: self.parameters?.map(\.toSoto),
            statement: self.statement ?? ""
        )
    }
}

// MARK: - CancellationReason Conversions

extension DynamoDB.CancellationReason {
    var toDynamoDBModel: DynamoDBModel.CancellationReason {
        DynamoDBModel.CancellationReason(
            code: self.code,
            item: self.item?.toDynamoDBModel,
            message: self.message
        )
    }
}

// MARK: - Soto Error Mapping

private func mapError(_ error: any Error) -> DynamoDBClientError {
    guard let dynamoError = error as? DynamoDBErrorType else {
        return .unknown(message: "\(error)")
    }
    let message = dynamoError.context?.message
    if dynamoError == .conditionalCheckFailedException {
        return .conditionalCheckFailed(message: message)
    } else if dynamoError == .duplicateItemException {
        return .duplicateItem(message: message)
    } else if dynamoError == .internalServerError {
        return .internalServerError(message: message)
    } else if dynamoError == .provisionedThroughputExceededException {
        return .provisionedThroughputExceeded(message: message)
    } else if dynamoError == .requestLimitExceeded {
        return .requestLimitExceeded(message: message)
    } else if dynamoError == .resourceNotFoundException {
        return .resourceNotFound(message: message)
    } else if dynamoError == .throttlingException {
        return .throttling(message: message)
    } else if dynamoError == .transactionConflictException {
        return .transactionConflict(message: message)
    } else if dynamoError == .transactionCanceledException {
        return .transactionCanceled(reasons: [], message: message)
    } else {
        return .unknown(message: "\(error)")
    }
}

// MARK: - DynamoDBClientProtocol Conformance

extension DynamoDB: DynamoDBClientProtocol {
    package func putItem(input: DynamoDBModel.PutItemInput) async throws(DynamoDBClientError) {
        do {
            _ = try await self.putItem(
                .init(
                    conditionExpression: input.conditionExpression,
                    expressionAttributeNames: input.expressionAttributeNames,
                    expressionAttributeValues: input.expressionAttributeValues?.toSoto,
                    item: input.item.toSoto,
                    tableName: input.tableName
                )
            )
        } catch {
            throw mapError(error)
        }
    }

    package func getItem(
        input: DynamoDBModel.GetItemInput
    ) async throws(DynamoDBClientError) -> DynamoDBModel.GetItemOutput {
        do {
            let sotoOutput = try await self.getItem(
                .init(
                    consistentRead: input.consistentRead,
                    key: input.key.toSoto,
                    tableName: input.tableName
                )
            )
            return DynamoDBModel.GetItemOutput(item: sotoOutput.item?.toDynamoDBModel)
        } catch {
            throw mapError(error)
        }
    }

    package func deleteItem(input: DynamoDBModel.DeleteItemInput) async throws(DynamoDBClientError) {
        do {
            _ = try await self.deleteItem(
                .init(
                    conditionExpression: input.conditionExpression,
                    expressionAttributeNames: input.expressionAttributeNames,
                    expressionAttributeValues: input.expressionAttributeValues?.toSoto,
                    key: input.key.toSoto,
                    tableName: input.tableName
                )
            )
        } catch {
            throw mapError(error)
        }
    }

    package func query(
        input: DynamoDBModel.QueryInput
    ) async throws(DynamoDBClientError) -> DynamoDBModel.QueryOutput {
        do {
            let sotoOutput = try await self.query(
                .init(
                    consistentRead: input.consistentRead,
                    exclusiveStartKey: input.exclusiveStartKey?.toSoto,
                    expressionAttributeNames: input.expressionAttributeNames,
                    expressionAttributeValues: input.expressionAttributeValues?.toSoto,
                    indexName: input.indexName,
                    keyConditionExpression: input.keyConditionExpression,
                    limit: input.limit,
                    scanIndexForward: input.scanIndexForward,
                    tableName: input.tableName
                )
            )
            return DynamoDBModel.QueryOutput(
                items: sotoOutput.items?.map(\.toDynamoDBModel),
                lastEvaluatedKey: sotoOutput.lastEvaluatedKey?.toDynamoDBModel
            )
        } catch {
            throw mapError(error)
        }
    }

    package func batchGetItem(
        input: DynamoDBModel.BatchGetItemInput
    ) async throws(DynamoDBClientError) -> DynamoDBModel.BatchGetItemOutput {
        do {
            let sotoOutput = try await self.batchGetItem(
                .init(
                    requestItems: (input.requestItems ?? [:]).mapValues(\.toSoto)
                )
            )
            return DynamoDBModel.BatchGetItemOutput(
                responses: sotoOutput.responses?.mapValues { items in items.map(\.toDynamoDBModel) },
                unprocessedKeys: sotoOutput.unprocessedKeys?.mapValues(\.toDynamoDBModel)
            )
        } catch {
            throw mapError(error)
        }
    }

    package func batchExecuteStatement(
        input: DynamoDBModel.BatchExecuteStatementInput
    ) async throws(DynamoDBClientError) -> DynamoDBModel.BatchExecuteStatementOutput {
        do {
            let sotoOutput = try await self.batchExecuteStatement(
                .init(
                    statements: (input.statements ?? []).map(\.toSoto)
                )
            )
            return DynamoDBModel.BatchExecuteStatementOutput(
                responses: sotoOutput.responses?.map(\.toDynamoDBModel)
            )
        } catch {
            throw mapError(error)
        }
    }

    package func executeStatement(
        input: DynamoDBModel.ExecuteStatementInput
    ) async throws(DynamoDBClientError) -> DynamoDBModel.ExecuteStatementOutput {
        do {
            let sotoOutput = try await self.executeStatement(
                .init(
                    consistentRead: input.consistentRead,
                    nextToken: input.nextToken,
                    statement: input.statement
                )
            )
            return DynamoDBModel.ExecuteStatementOutput(
                items: sotoOutput.items?.map(\.toDynamoDBModel),
                nextToken: sotoOutput.nextToken
            )
        } catch {
            throw mapError(error)
        }
    }

    package func executeTransaction(input: DynamoDBModel.ExecuteTransactionInput) async throws(DynamoDBClientError) {
        do {
            _ = try await self.executeTransaction(
                .init(
                    transactStatements: (input.transactStatements ?? []).map(\.toSoto)
                )
            )
        } catch {
            throw mapError(error)
        }
    }
}
#endif
