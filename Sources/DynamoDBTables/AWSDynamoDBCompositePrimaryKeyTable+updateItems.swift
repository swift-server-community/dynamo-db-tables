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
//  AWSDynamoDBCompositePrimaryKeyTable+updateItems.swift
//  DynamoDBTables
//

import Foundation
import SmokeAWSCore
import DynamoDBModel
import SmokeHTTPClient
import Logging
import CollectionConcurrencyKit

private let millisecondsToNanoSeconds: UInt64 = 1000000

public struct AWSDynamoDBLimits {
    // BatchExecuteStatement has a maximum of 25 statements
    public static let maximumUpdatesPerExecuteStatement = 25
    public static let maximumUpdatesPerTransactionStatement = 100
    public static let maxStatementLength = 8192
}

private struct AWSDynamoDBPolymorphicWriteEntryTransform<InvocationReportingType: HTTPClientCoreInvocationReporting>: PolymorphicWriteEntryTransform {
    typealias TableType = AWSDynamoDBCompositePrimaryKeyTable<InvocationReportingType>

    let statement: String

    init<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: WriteEntry<AttributesType, ItemType>, table: TableType) throws {
        self.statement = try table.entryToStatement(entry)
    }
}

private struct AWSDynamoDBPolymorphicTransactionConstraintTransform<
        InvocationReportingType: HTTPClientCoreInvocationReporting>: PolymorphicTransactionConstraintTransform {
    typealias TableType = AWSDynamoDBCompositePrimaryKeyTable<InvocationReportingType>

    let statement: String
    
    init<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: TransactionConstraintEntry<AttributesType, ItemType>,
                                                                  table: TableType) throws {
        self.statement = try table.entryToStatement(entry)
    }
}

/// DynamoDBTable conformance updateItems function
public extension AWSDynamoDBCompositePrimaryKeyTable {
    
    func validateEntry<AttributesType, ItemType>(entry: WriteEntry<AttributesType, ItemType>) throws {
        
        let statement: String = try entryToStatement(entry)
        
        if statement.count > AWSDynamoDBLimits.maxStatementLength {
            throw DynamoDBTableError.statementLengthExceeded(
                reason: "failed to satisfy constraint: Member must have length less than or equal "
                    + "to \(AWSDynamoDBLimits.maxStatementLength). Actual length \(statement.count)")
        }
    }
    
    internal func entryToStatement<AttributesType, ItemType>(
        _ entry: WriteEntry<AttributesType, ItemType>) throws -> String {
        
        let statement: String
        switch entry {
        case .update(new: let new, existing: let existing):
            statement = try getUpdateExpression(tableName: self.targetTableName,
                                                newItem: new,
                                                existingItem: existing)
        case .insert(new: let new):
            statement = try getInsertExpression(tableName: self.targetTableName,
                                                newItem: new)
        case .deleteAtKey(key: let key):
            statement = try getDeleteExpression(tableName: self.targetTableName,
                                                existingKey: key)
        case .deleteItem(existing: let existing):
            statement = try getDeleteExpression(tableName: self.targetTableName,
                                                existingItem: existing)
        }
        
        return statement
    }
    
    internal func entryToStatement<AttributesType, ItemType>(
        _ entry: TransactionConstraintEntry<AttributesType, ItemType>) throws -> String {
        
        let statement: String
        switch entry {
        case .required(existing: let existing):
            statement = getExistsExpression(tableName: self.targetTableName,
                                            existingItem: existing)
        }
        
        return statement
    }

    private func entryToBatchStatementRequest<AttributesType, ItemType>(
        _ entry: WriteEntry<AttributesType, ItemType>) throws -> BatchStatementRequest {
        
        let statement: String = try entryToStatement(entry)
        
        // doesn't require read consistency as no items are being read
        return BatchStatementRequest(consistentRead: false, statement: statement)
    }
    
    func throwOnBatchExecuteStatementErrors(response: DynamoDBModel.BatchExecuteStatementOutput) throws {
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
    
    private func writeTransactionItems<WriteEntryType: PolymorphicWriteEntry,
                                       TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
                                        _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType]) async throws
    {
        // if there are no items, there is nothing to update
        guard entries.count > 0 else {
            return
        }
        
        let context = StandardPolymorphicWriteEntryContext<AWSDynamoDBPolymorphicWriteEntryTransform,
                                                           AWSDynamoDBPolymorphicTransactionConstraintTransform>(table: self)
        let entryStatements: [ParameterizedStatement] = try entries.map { entry -> ParameterizedStatement in
            let transform: AWSDynamoDBPolymorphicWriteEntryTransform = try entry.handle(context: context)
            let statement = transform.statement
            
            return ParameterizedStatement(statement: statement)
        }
        
        let requiredItemsStatements: [ParameterizedStatement] = try constraints.map { entry -> ParameterizedStatement in
            let transform: AWSDynamoDBPolymorphicTransactionConstraintTransform = try entry.handle(context: context)
            let statement = transform.statement
            
            return ParameterizedStatement(statement: statement)
        }
        
        let transactionInput = ExecuteTransactionInput(transactStatements: entryStatements + requiredItemsStatements)
        
        _ = try await dynamodb.executeTransaction(input: transactionInput)
    }
    
    func transactWrite<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws {
        let noConstraints: [EmptyPolymorphicTransactionConstraintEntry] = []
        return try await transactWrite(entries, constraints: noConstraints,
                                       retriesRemaining: self.dynamodb.retryConfiguration.numRetries)
    }
    
    func transactWrite<WriteEntryType: PolymorphicWriteEntry,
                       TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
                        _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType]) async throws {
        return try await transactWrite(entries, constraints: constraints,
                                       retriesRemaining: self.dynamodb.retryConfiguration.numRetries)
    }
    
    private func transactWrite<WriteEntryType: PolymorphicWriteEntry,
                               TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
                        _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType],
                        retriesRemaining: Int) async throws {
        let entryCount = entries.count + constraints.count
            
        if entryCount > AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement {
            throw DynamoDBTableError.transactionSizeExceeded(attemptedSize: entryCount,
                                                             maximumSize: AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement)
        }
        
        let result: Swift.Result<Void, DynamoDBTableError>
        do {
            try await self.writeTransactionItems(entries, constraints: constraints)
            
            result = .success(())
        } catch DynamoDBError.transactionCanceled(let exception) {
            guard let cancellationReasons = exception.cancellationReasons else {
                throw DynamoDBTableError.transactionCanceled(reasons: [])
            }
            
            let keys = entries.map { $0.compositePrimaryKey } + constraints.map { $0.compositePrimaryKey }
            
            var isTransactionConflict = false
            let reasons = try zip(cancellationReasons, keys).compactMap { (cancellationReason, entryKey) -> DynamoDBTableError? in
                let key: StandardCompositePrimaryKey?
                if let item = cancellationReason.item {
                    key = try DynamoDBDecoder().decode(.init(M: item))
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
                                                                 sortKey: sortKey,message: cancellationReason.message)
                }
            }
            
            if isTransactionConflict && retriesRemaining > 0 {
                return try await retryTransactWrite(entries, constraints: constraints, retriesRemaining: retriesRemaining)
            }
            
            result = .failure(DynamoDBTableError.transactionCanceled(reasons: reasons))
        } catch DynamoDBError.transactionConflict(let exception) {
            if retriesRemaining > 0 {
                return try await retryTransactWrite(entries, constraints: constraints, retriesRemaining: retriesRemaining)
            }
            
            let reason = DynamoDBTableError.transactionConflict(message: exception.message)
            
            result = .failure(DynamoDBTableError.transactionCanceled(reasons: [reason]))
        }
                            
        let retryCount = self.dynamodb.retryConfiguration.numRetries - retriesRemaining
        self.tableMetrics.transactWriteRetryCountRecorder?.record(retryCount)
                            
        switch result {
        case .success:
            return
        case .failure(let failure):
            throw failure
        }
    }
    
    private func retryTransactWrite<WriteEntryType: PolymorphicWriteEntry,
                                    TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
                        _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType],
                        retriesRemaining: Int) async throws {
        // determine the required interval
        let retryInterval = Int(self.dynamodb.retryConfiguration.getRetryInterval(retriesRemaining: retriesRemaining))
                
        logger.warning(
            "Transaction retried due to conflict. Remaining retries: \(retriesRemaining). Retrying in \(retryInterval) ms.")
        try await Task.sleep(nanoseconds: UInt64(retryInterval) * millisecondsToNanoSeconds)
                
        logger.trace("Reattempting request due to remaining retries: \(retryInterval)")
        return try await transactWrite(entries, constraints: constraints, retriesRemaining: retriesRemaining - 1)
    }
    
    private func writeChunkedItems<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws
    {
        // if there are no items, there is nothing to update
        guard entries.count > 0 else {
            return
        }
        
        let context = StandardPolymorphicWriteEntryContext<AWSDynamoDBPolymorphicWriteEntryTransform,
                                                           AWSDynamoDBPolymorphicTransactionConstraintTransform>(table: self)
        let statements: [BatchStatementRequest] = try entries.map { entry -> BatchStatementRequest in
            let transform: AWSDynamoDBPolymorphicWriteEntryTransform = try entry.handle(context: context)
            let statement = transform.statement
            
            return BatchStatementRequest(consistentRead: true, statement: statement)
        }
        
        let executeInput = BatchExecuteStatementInput(statements: statements)
        
        let response = try await dynamodb.batchExecuteStatement(input: executeInput)
        try throwOnBatchExecuteStatementErrors(response: response)
    }
    
    func bulkWrite<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedEntries = entries.chunked(by: AWSDynamoDBLimits.maximumUpdatesPerExecuteStatement)
        try await chunkedEntries.concurrentForEach { chunk in
            try await self.writeChunkedItems(chunk)
        }
    }
    
    private func writeChunkedItems<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws {
        // if there are no items, there is nothing to update
        guard entries.count > 0 else {
            return
        }
        
        let statements: [BatchStatementRequest] = try entries.map { entry -> BatchStatementRequest in
            let statement: String
            switch entry {
            case .update(new: let new, existing: let existing):
                statement = try getUpdateExpression(tableName: self.targetTableName,
                                                    newItem: new,
                                                    existingItem: existing)
            case .insert(new: let new):
                statement = try getInsertExpression(tableName: self.targetTableName,
                                                    newItem: new)
            case .deleteAtKey(key: let key):
                statement = try getDeleteExpression(tableName: self.targetTableName,
                                                    existingKey: key)
            case .deleteItem(existing: let existing):
                statement = try getDeleteExpression(tableName: self.targetTableName,
                                                    existingItem: existing)
            }
            
            return BatchStatementRequest(consistentRead: true, statement: statement)
        }
        
        let executeInput = BatchExecuteStatementInput(statements: statements)
        
        let response = try await dynamodb.batchExecuteStatement(input: executeInput)
        try throwOnBatchExecuteStatementErrors(response: response)
    }
    
    func monomorphicBulkWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws {
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
                case .update(new: let new, existing: let existing):
                    try await self.updateItem(newItem: new, existingItem: existing)
                case .insert(new: let new):
                    try await self.insertItem(new)
                case .deleteAtKey(key: let key):
                    try await self.deleteItem(forKey: key)
                case .deleteItem(existing: let existing):
                    try await self.deleteItem(existingItem: existing)
                }
            }
        }

        return try await monomorphicBulkWrite(bulkWriteEntries)
    }
    
    func writeChunkedItemsWithoutThrowing<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws
    -> Set<BatchStatementErrorCodeEnum> {
        // if there are no items, there is nothing to update
        
        guard entries.count > 0 else {
            self.logger.trace("\(entries) with count = 0")
            
            return []
        }
        
        let statements: [BatchStatementRequest] = try entries.map { try entryToBatchStatementRequest( $0 ) }
        let executeInput = BatchExecuteStatementInput(statements: statements)
        let result = try await dynamodb.batchExecuteStatement(input: executeInput)
        
        var errorCodeSet: Set<BatchStatementErrorCodeEnum> = Set()
        // TODO: Remove errorCodeSet and return errorSet instead
        var errorSet: Set<BatchStatementError> = Set()
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
    
    func monomorphicBulkWriteWithoutThrowing<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws
    -> Set<BatchStatementErrorCodeEnum> {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedEntries = entries.chunked(by: AWSDynamoDBLimits.maximumUpdatesPerExecuteStatement)

        let results = try await chunkedEntries.concurrentMap { chunk in
            return try await self.writeChunkedItemsWithoutThrowing(chunk)
        }
        
        return results.reduce([]) { partialResult, currentResult in
            return partialResult.union(currentResult)
        }
    }
}

extension BatchStatementError: Hashable {

    public func hash(into hasher: inout Hasher) {
        hasher.combine(self.code)
        hasher.combine(self.message)
    }
}
