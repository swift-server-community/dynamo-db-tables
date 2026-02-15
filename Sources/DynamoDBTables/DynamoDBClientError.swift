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
//  DynamoDBClientError.swift
//  DynamoDBTables
//

package enum DynamoDBClientError: Error, Sendable {
    case conditionalCheckFailed(message: String?)
    case duplicateItem(message: String?)
    case internalServerError(message: String?)
    case provisionedThroughputExceeded(message: String?)
    case requestLimitExceeded(message: String?)
    case resourceNotFound(message: String?)
    case throttling(message: String?)
    case transactionConflict(message: String?)
    case transactionCanceled(reasons: [DynamoDBModel.CancellationReason], message: String?)
    case unknown(message: String?)
}

extension DynamoDBClientError {
    func asDynamoDBTableError(partitionKey: String, sortKey: String) -> DynamoDBTableError {
        switch self {
        case .conditionalCheckFailed(let message):
            .conditionalCheckFailed(partitionKey: partitionKey, sortKey: sortKey, message: message)
        case .duplicateItem(let message):
            .duplicateItem(partitionKey: partitionKey, sortKey: sortKey, message: message)
        case .internalServerError(let message):
            .internalServerError(message: message)
        case .provisionedThroughputExceeded(let message):
            .provisionedThroughputExceeded(message: message)
        case .requestLimitExceeded(let message):
            .requestLimitExceeded(message: message)
        case .resourceNotFound(let message):
            .resourceNotFound(partitionKey: partitionKey, sortKey: sortKey, message: message)
        case .throttling(let message):
            .throttling(message: message)
        case .transactionConflict(let message):
            .transactionConflict(message: message)
        case .transactionCanceled:
            .unexpectedError(cause: self)
        case .unknown(let message):
            .unknown(code: nil, partitionKey: partitionKey, sortKey: sortKey, message: message)
        }
    }
}
