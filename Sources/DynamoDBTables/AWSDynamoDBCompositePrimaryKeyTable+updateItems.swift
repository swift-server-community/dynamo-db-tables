//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/AWSDynamoDBCompositePrimaryKeyTable+updateItems.swift
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
//  AWSDynamoDBCompositePrimaryKeyTable+updateItems.swift
//  DynamoDBTables
//

import AWSDynamoDB
import CollectionConcurrencyKit
import Foundation
import Logging

private let millisecondsToNanoSeconds: UInt64 = 1_000_000

public enum AWSDynamoDBLimits {
    // BatchExecuteStatement has a maximum of 25 statements
    public static let maximumUpdatesPerExecuteStatement = 25
    public static let maximumUpdatesPerTransactionStatement = 100
    public static let maxStatementLength = 8192
}

private struct AWSDynamoDBPolymorphicWriteEntryTransform: PolymorphicWriteEntryTransform {
    typealias TableType = AWSDynamoDBCompositePrimaryKeyTable

    let statement: String

    init(_ entry: WriteEntry<some PrimaryKeyAttributes, some Codable>, table: TableType) throws {
        self.statement = try table.entryToStatement(entry)
    }
}

private struct AWSDynamoDBPolymorphicTransactionConstraintTransform: PolymorphicTransactionConstraintTransform {
    typealias TableType = AWSDynamoDBCompositePrimaryKeyTable

    let statement: String

    init(_ entry: TransactionConstraintEntry<some PrimaryKeyAttributes, some Codable>,
         table: TableType) throws
    {
        self.statement = try table.entryToStatement(entry)
    }
}

/// DynamoDBTable conformance updateItems function
public extension AWSDynamoDBCompositePrimaryKeyTable {
    func validateEntry(entry: WriteEntry<some Any, some Any>) throws {
        let statement: String = try entryToStatement(entry)

        if statement.count > AWSDynamoDBLimits.maxStatementLength {
            throw DynamoDBTableError.statementLengthExceeded(
                reason: "failed to satisfy constraint: Member must have length less than or equal "
                    + "to \(AWSDynamoDBLimits.maxStatementLength). Actual length \(statement.count)")
        }
    }

    internal func entryToStatement(
        _ entry: WriteEntry<some Any, some Any>) throws -> String
    {
        let statement: String
        switch entry {
        case let .update(new: new, existing: existing):
            statement = try getUpdateExpression(tableName: self.targetTableName,
                                                newItem: new,
                                                existingItem: existing)
        case let .insert(new: new):
            statement = try getInsertExpression(tableName: self.targetTableName,
                                                newItem: new)
        case let .deleteAtKey(key: key):
            statement = try getDeleteExpression(tableName: self.targetTableName,
                                                existingKey: key)
        case let .deleteItem(existing: existing):
            statement = try getDeleteExpression(tableName: self.targetTableName,
                                                existingItem: existing)
        }

        return statement
    }

    internal func entryToStatement(
        _ entry: TransactionConstraintEntry<some Any, some Any>) throws -> String
    {
        let statement: String
        switch entry {
        case let .required(existing: existing):
            statement = getExistsExpression(tableName: self.targetTableName,
                                            existingItem: existing)
        }

        return statement
    }

    private func entryToBatchStatementRequest(
        _ entry: WriteEntry<some Any, some Any>) throws -> DynamoDBClientTypes.BatchStatementRequest
    {
        let statement: String = try entryToStatement(entry)

        // doesn't require read consistency as no items are being read
        return DynamoDBClientTypes.BatchStatementRequest(consistentRead: false, statement: statement)
    }

    func throwOnBatchExecuteStatementErrors(response: AWSDynamoDB.BatchExecuteStatementOutput) throws {
        var errorMap: [String: Int] = [:]
        var errorCount = 0
        response.responses?.forEach { response in
            if let error = response.error {
                errorCount += 1

                var messageElements: [String] = []
                if let code = error.code {
                    messageElements.append(code.rawValue)
                }

                if let message = error.message {
                    messageElements.append(message)
                }

                if !messageElements.isEmpty {
                    let message = messageElements.joined(separator: ":")
                    var updatedErrorCount = errorMap[message] ?? 0
                    updatedErrorCount += 1
                    errorMap[message] = updatedErrorCount
                }
            }
        }

        guard errorCount > 0 else {
            // no errors
            return
        }

        throw DynamoDBTableError.batchErrorsReturned(errorCount: errorCount, messageMap: errorMap)
    }

    private func writeTransactionItems(
        _ entries: [some PolymorphicWriteEntry], constraints: [some PolymorphicTransactionConstraintEntry]) async throws
    {
        // if there are no items, there is nothing to update
        guard entries.count > 0 else {
            return
        }

        let context = StandardPolymorphicWriteEntryContext<AWSDynamoDBPolymorphicWriteEntryTransform,
            AWSDynamoDBPolymorphicTransactionConstraintTransform>(table: self)
        let entryStatements = try entries.map { entry -> DynamoDBClientTypes.ParameterizedStatement in
            let transform: AWSDynamoDBPolymorphicWriteEntryTransform = try entry.handle(context: context)
            let statement = transform.statement

            return DynamoDBClientTypes.ParameterizedStatement(statement: statement)
        }

        let requiredItemsStatements = try constraints.map { entry -> DynamoDBClientTypes.ParameterizedStatement in
            let transform: AWSDynamoDBPolymorphicTransactionConstraintTransform = try entry.handle(context: context)
            let statement = transform.statement

            return DynamoDBClientTypes.ParameterizedStatement(statement: statement)
        }

        let transactionInput = ExecuteTransactionInput(transactStatements: entryStatements + requiredItemsStatements)

        _ = try await dynamodb.executeTransaction(input: transactionInput)
    }

    func transactWrite(_ entries: [some PolymorphicWriteEntry]) async throws {
        let noConstraints: [EmptyPolymorphicTransactionConstraintEntry] = []
        return try await self.transactWrite(entries, constraints: noConstraints,
                                            retriesRemaining: self.retryConfiguration.numRetries)
    }

    func transactWrite(
        _ entries: [some PolymorphicWriteEntry], constraints: [some PolymorphicTransactionConstraintEntry]) async throws
    {
        try await self.transactWrite(entries, constraints: constraints,
                                     retriesRemaining: self.retryConfiguration.numRetries)
    }

    private func transactWrite(
        _ entries: [some PolymorphicWriteEntry], constraints: [some PolymorphicTransactionConstraintEntry],
        retriesRemaining: Int) async throws
    {
        let entryCount = entries.count + constraints.count

        if entryCount > AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement {
            throw DynamoDBTableError.transactionSizeExceeded(attemptedSize: entryCount,
                                                             maximumSize: AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement)
        }

        let result: Swift.Result<Void, DynamoDBTableError>
        do {
            try await self.writeTransactionItems(entries, constraints: constraints)

            result = .success(())
        } catch let exception as TransactionCanceledException {
            guard let cancellationReasons = exception.properties.cancellationReasons else {
                throw DynamoDBTableError.transactionCanceled(reasons: [])
            }

            let keys = entries.map(\.compositePrimaryKey) + constraints.map(\.compositePrimaryKey)

            var isTransactionConflict = false
            let reasons = try zip(cancellationReasons, keys).compactMap { cancellationReason, entryKey -> DynamoDBTableError? in
                let key: StandardCompositePrimaryKey?
                if let item = cancellationReason.item {
                    key = try DynamoDBDecoder().decode(.m(item))
                } else {
                    key = nil
                }

                let partitionKey = key?.partitionKey ?? entryKey?.partitionKey
                let sortKey = key?.sortKey ?? entryKey?.sortKey

                // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ExecuteTransaction.html
                switch cancellationReason.code {
                case "None":
                    return nil
                case "ConditionalCheckFailed":
                    return DynamoDBTableError.transactionConditionalCheckFailed(partitionKey: partitionKey,
                                                                                sortKey: sortKey,
                                                                                message: cancellationReason.message)
                case "DuplicateItem":
                    return DynamoDBTableError.duplicateItem(partitionKey: partitionKey, sortKey: sortKey,
                                                            message: cancellationReason.message)
                case "ItemCollectionSizeLimitExceeded":
                    return DynamoDBTableError.transactionSizeExceeded(attemptedSize: entryCount,
                                                                      maximumSize: AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement)
                case "TransactionConflict":
                    isTransactionConflict = true

                    return DynamoDBTableError.transactionConflict(message: cancellationReason.message)
                case "ProvisionedThroughputExceeded":
                    return DynamoDBTableError.transactionProvisionedThroughputExceeded(message: cancellationReason.message)
                case "ThrottlingError":
                    return DynamoDBTableError.transactionThrottling(message: cancellationReason.message)
                case "ValidationError":
                    return DynamoDBTableError.transactionValidation(partitionKey: partitionKey, sortKey: sortKey,
                                                                    message: cancellationReason.message)
                default:
                    return DynamoDBTableError.transactionUnknown(code: cancellationReason.code, partitionKey: partitionKey,
                                                                 sortKey: sortKey, message: cancellationReason.message)
                }
            }

            if isTransactionConflict, retriesRemaining > 0 {
                return try await retryTransactWrite(entries, constraints: constraints, retriesRemaining: retriesRemaining)
            }

            result = .failure(DynamoDBTableError.transactionCanceled(reasons: reasons))
        } catch let exception as TransactionConflictException {
            if retriesRemaining > 0 {
                return try await retryTransactWrite(entries, constraints: constraints, retriesRemaining: retriesRemaining)
            }

            let reason = DynamoDBTableError.transactionConflict(message: exception.message)

            result = .failure(DynamoDBTableError.transactionCanceled(reasons: [reason]))
        }

        let retryCount = self.retryConfiguration.numRetries - retriesRemaining
        self.tableMetrics.transactWriteRetryCountRecorder?.record(retryCount)

        switch result {
        case .success:
            return
        case let .failure(failure):
            throw failure
        }
    }

    private func retryTransactWrite(
        _ entries: [some PolymorphicWriteEntry], constraints: [some PolymorphicTransactionConstraintEntry],
        retriesRemaining: Int) async throws
    {
        // determine the required interval
        let retryInterval = Int(self.retryConfiguration.getRetryInterval(retriesRemaining: retriesRemaining))

        logger.warning(
            "Transaction retried due to conflict. Remaining retries: \(retriesRemaining). Retrying in \(retryInterval) ms.")
        try await Task.sleep(nanoseconds: UInt64(retryInterval) * millisecondsToNanoSeconds)

        logger.trace("Reattempting request due to remaining retries: \(retryInterval)")
        return try await self.transactWrite(entries, constraints: constraints, retriesRemaining: retriesRemaining - 1)
    }

    private func writeChunkedItems(_ entries: [some PolymorphicWriteEntry]) async throws {
        // if there are no items, there is nothing to update
        guard entries.count > 0 else {
            return
        }

        let context = StandardPolymorphicWriteEntryContext<AWSDynamoDBPolymorphicWriteEntryTransform,
            AWSDynamoDBPolymorphicTransactionConstraintTransform>(table: self)
        let statements = try entries.map { entry -> DynamoDBClientTypes.BatchStatementRequest in
            let transform: AWSDynamoDBPolymorphicWriteEntryTransform = try entry.handle(context: context)
            let statement = transform.statement

            return DynamoDBClientTypes.BatchStatementRequest(consistentRead: true, statement: statement)
        }

        let executeInput = BatchExecuteStatementInput(statements: statements)

        let response = try await dynamodb.batchExecuteStatement(input: executeInput)
        try self.throwOnBatchExecuteStatementErrors(response: response)
    }

    func bulkWrite(_ entries: [some PolymorphicWriteEntry]) async throws {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedEntries = entries.chunked(by: AWSDynamoDBLimits.maximumUpdatesPerExecuteStatement)
        try await chunkedEntries.concurrentForEach { chunk in
            try await self.writeChunkedItems(chunk)
        }
    }

    private func writeChunkedItems(_ entries: [WriteEntry<some Any, some Any>]) async throws {
        // if there are no items, there is nothing to update
        guard entries.count > 0 else {
            return
        }

        let statements = try entries.map { entry -> DynamoDBClientTypes.BatchStatementRequest in
            let statement: String
            switch entry {
            case let .update(new: new, existing: existing):
                statement = try getUpdateExpression(tableName: self.targetTableName,
                                                    newItem: new,
                                                    existingItem: existing)
            case let .insert(new: new):
                statement = try getInsertExpression(tableName: self.targetTableName,
                                                    newItem: new)
            case let .deleteAtKey(key: key):
                statement = try getDeleteExpression(tableName: self.targetTableName,
                                                    existingKey: key)
            case let .deleteItem(existing: existing):
                statement = try getDeleteExpression(tableName: self.targetTableName,
                                                    existingItem: existing)
            }

            return DynamoDBClientTypes.BatchStatementRequest(consistentRead: true, statement: statement)
        }

        let executeInput = BatchExecuteStatementInput(statements: statements)

        let response = try await dynamodb.batchExecuteStatement(input: executeInput)
        try self.throwOnBatchExecuteStatementErrors(response: response)
    }

    func monomorphicBulkWrite(_ entries: [WriteEntry<some Any, some Any>]) async throws {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedEntries = entries.chunked(by: AWSDynamoDBLimits.maximumUpdatesPerExecuteStatement)
        try await chunkedEntries.concurrentForEach { chunk in
            try await self.writeChunkedItems(chunk)
        }
    }

    func monomorphicBulkWriteWithFallback<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws {
        // fall back to singel operation if the write entry exceeds the statement length limitation
        var bulkWriteEntries: [WriteEntry<AttributesType, ItemType>] = []
        try await entries.concurrentForEach { entry in
            do {
                try self.validateEntry(entry: entry)
                bulkWriteEntries.append(entry)
            } catch DynamoDBTableError.statementLengthExceeded {
                switch entry {
                case let .update(new: new, existing: existing):
                    try await self.updateItem(newItem: new, existingItem: existing)
                case let .insert(new: new):
                    try await self.insertItem(new)
                case let .deleteAtKey(key: key):
                    try await self.deleteItem(forKey: key)
                case let .deleteItem(existing: existing):
                    try await self.deleteItem(existingItem: existing)
                }
            }
        }

        return try await self.monomorphicBulkWrite(bulkWriteEntries)
    }

    func writeChunkedItemsWithoutThrowing(_ entries: [WriteEntry<some Any, some Any>]) async throws
        -> Set<DynamoDBClientTypes.BatchStatementErrorCodeEnum>
    {
        // if there are no items, there is nothing to update

        guard entries.count > 0 else {
            self.logger.trace("\(entries) with count = 0")

            return []
        }

        let statements: [DynamoDBClientTypes.BatchStatementRequest] = try entries.map { try self.entryToBatchStatementRequest($0) }
        let executeInput = BatchExecuteStatementInput(statements: statements)
        let result = try await dynamodb.batchExecuteStatement(input: executeInput)

        var errorCodeSet: Set<DynamoDBClientTypes.BatchStatementErrorCodeEnum> = Set()
        // TODO: Remove errorCodeSet and return errorSet instead
        var errorSet: Set<DynamoDBClientTypes.BatchStatementError> = Set()
        result.responses?.forEach { response in
            if let error = response.error, let code = error.code {
                errorCodeSet.insert(code)
                errorSet.insert(error)
            }
        }

        // if there are errors
        if !errorSet.isEmpty {
            self.logger.error("Received BatchStatmentErrors from dynamodb are \(errorSet)")
        }
        return errorCodeSet
    }

    func monomorphicBulkWriteWithoutThrowing(_ entries: [WriteEntry<some Any, some Any>]) async throws
        -> Set<DynamoDBClientTypes.BatchStatementErrorCodeEnum>
    {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedEntries = entries.chunked(by: AWSDynamoDBLimits.maximumUpdatesPerExecuteStatement)

        let results = try await chunkedEntries.concurrentMap { chunk in
            try await self.writeChunkedItemsWithoutThrowing(chunk)
        }

        return results.reduce([]) { partialResult, currentResult in
            partialResult.union(currentResult)
        }
    }
}

extension DynamoDBClientTypes.BatchStatementError: Equatable {
    public static func == (lhs: DynamoDBClientTypes.BatchStatementError, rhs: DynamoDBClientTypes.BatchStatementError) -> Bool {
        guard lhs.code == rhs.code, lhs.item == rhs.item, lhs.message == rhs.message else {
            return false
        }

        return true
    }
}

extension DynamoDBClientTypes.BatchStatementError: Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(self.code)
        hasher.combine(self.message)
    }
}
