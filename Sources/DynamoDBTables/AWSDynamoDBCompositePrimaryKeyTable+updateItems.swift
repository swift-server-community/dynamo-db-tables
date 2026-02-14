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
import Logging

private let millisecondsToNanoSeconds: UInt64 = 1_000_000

public enum AWSDynamoDBLimits {
    // BatchExecuteStatement has a maximum of 25 statements
    public static let maximumUpdatesPerExecuteStatement = 25
    public static let maximumUpdatesPerTransactionStatement = 100
    public static let maxStatementLength = 8192
}

private struct AWSDynamoDBPolymorphicWriteEntryTransform<Client: DynamoDBClientProtocol & Sendable>:
    PolymorphicWriteEntryTransform
{
    typealias TableType = GenericAWSDynamoDBCompositePrimaryKeyTable<Client>

    let statement: String

    init(
        _ entry: WriteEntry<some PrimaryKeyAttributes, some Codable, some TimeToLiveAttributes>,
        table: TableType
    ) throws {
        self.statement = try table.entryToStatement(entry)
    }
}

private struct AWSDynamoDBPolymorphicTransactionConstraintTransform<Client: DynamoDBClientProtocol & Sendable>:
    PolymorphicTransactionConstraintTransform
{
    typealias TableType = GenericAWSDynamoDBCompositePrimaryKeyTable<Client>

    let statement: String

    init(
        _ entry: TransactionConstraintEntry<
            some PrimaryKeyAttributes, some Codable & Sendable, some TimeToLiveAttributes
        >,
        table: TableType
    ) throws {
        self.statement = try table.entryToStatement(entry)
    }
}

/// DynamoDBTable conformance updateItems function
extension GenericAWSDynamoDBCompositePrimaryKeyTable {
    public func validateEntry(entry: WriteEntry<some Any, some Any, some Any>) throws {
        let statement: String = try entryToStatement(entry)

        if statement.count > AWSDynamoDBLimits.maxStatementLength {
            throw DynamoDBTableError.statementLengthExceeded(
                reason: "failed to satisfy constraint: Member must have length less than or equal "
                    + "to \(AWSDynamoDBLimits.maxStatementLength). Actual length \(statement.count)"
            )
        }
    }

    internal func entryToStatement(
        _ entry: WriteEntry<some Any, some Any, some Any>
    ) throws -> String {
        let statement: String =
            switch entry {
            case let .update(new: new, existing: existing):
                try getUpdateExpression(
                    tableName: self.targetTableName,
                    newItem: new,
                    existingItem: existing
                )
            case let .insert(new: new):
                try getInsertExpression(
                    tableName: self.targetTableName,
                    newItem: new
                )
            case let .deleteAtKey(key: key):
                try getDeleteExpression(
                    tableName: self.targetTableName,
                    existingKey: key
                )
            case let .deleteItem(existing: existing):
                try getDeleteExpression(
                    tableName: self.targetTableName,
                    existingItem: existing
                )
            }

        return statement
    }

    internal func entryToStatement(
        _ entry: TransactionConstraintEntry<some Any, some Sendable, some Any>
    ) throws -> String {
        let statement: String =
            switch entry {
            case let .required(existing: existing):
                getExistsExpression(
                    tableName: self.targetTableName,
                    existingItem: existing
                )
            }

        return statement
    }

    private func writeTransactionItems<AttributesType, ItemType, TimeToLiveAttributesType>(
        _ entries: [WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>],
        constraints: [TransactionConstraintEntry<AttributesType, ItemType, TimeToLiveAttributesType>]
    ) async throws {
        // if there are no items, there is nothing to update
        guard entries.count > 0 else {
            return
        }

        let entryStatements = try entries.map { entry -> DynamoDBClientTypes.ParameterizedStatement in
            let statement = try self.entryToStatement(entry)

            return DynamoDBClientTypes.ParameterizedStatement(statement: statement)
        }

        let requiredItemsStatements = try constraints.map { entry -> DynamoDBClientTypes.ParameterizedStatement in
            let statement = try self.entryToStatement(entry)

            return DynamoDBClientTypes.ParameterizedStatement(statement: statement)
        }

        let transactionInput = ExecuteTransactionInput(transactStatements: entryStatements + requiredItemsStatements)

        _ = try await dynamodb.executeTransaction(input: transactionInput)
    }

    private func getExecuteTransactionInput(
        _ entries: [some PolymorphicWriteEntry],
        constraints: [some PolymorphicTransactionConstraintEntry]
    ) throws -> ExecuteTransactionInput? {
        // if there are no items, there is nothing to update
        guard entries.count > 0 else {
            return nil
        }

        let context = StandardPolymorphicWriteEntryContext<
            AWSDynamoDBPolymorphicWriteEntryTransform<Client>,
            AWSDynamoDBPolymorphicTransactionConstraintTransform<Client>
        >(table: self)
        let entryStatements = try entries.map { entry -> DynamoDBClientTypes.ParameterizedStatement in
            let transform: AWSDynamoDBPolymorphicWriteEntryTransform<Client> = try entry.handle(context: context)
            let statement = transform.statement

            return DynamoDBClientTypes.ParameterizedStatement(statement: statement)
        }

        let requiredItemsStatements = try constraints.map { entry -> DynamoDBClientTypes.ParameterizedStatement in
            let transform: AWSDynamoDBPolymorphicTransactionConstraintTransform<Client> = try entry.handle(
                context: context
            )
            let statement = transform.statement

            return DynamoDBClientTypes.ParameterizedStatement(statement: statement)
        }

        return ExecuteTransactionInput(transactStatements: entryStatements + requiredItemsStatements)
    }

    public func transactWrite(_ entries: [WriteEntry<some Any, some Any, some Any>]) async throws {
        try await self.transactWrite(
            entries,
            constraints: [],
            retriesRemaining: self.tableConfiguration.retry.numRetries
        )
    }

    public func transactWrite<AttributesType, ItemType, TimeToLiveAttributesType>(
        _ entries: [WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>],
        constraints: [TransactionConstraintEntry<AttributesType, ItemType, TimeToLiveAttributesType>]
    ) async throws {
        try await self.transactWrite(
            entries,
            constraints: constraints,
            retriesRemaining: self.tableConfiguration.retry.numRetries
        )
    }

    public func polymorphicTransactWrite<WriteEntryType: PolymorphicWriteEntry>(
        _ entries: [WriteEntryType]
    ) async throws {
        let noConstraints: [EmptyPolymorphicTransactionConstraintEntry<WriteEntryType.AttributesType>] = []

        guard let transactionInput = try getExecuteTransactionInput(entries, constraints: noConstraints) else {
            // nothing to do
            return
        }
        let inputKeys = entries.map(\.compositePrimaryKey)

        try await self.polymorphicTransactWrite(
            transactionInput,
            inputKeys: inputKeys,
            retriesRemaining: self.tableConfiguration.retry.numRetries
        )
    }

    public func polymorphicTransactWrite<
        WriteEntryType: PolymorphicWriteEntry,
        TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry
    >(
        _ entries: [WriteEntryType],
        constraints: [TransactionConstraintEntryType]
    ) async throws
    where WriteEntryType.AttributesType == TransactionConstraintEntryType.AttributesType {
        guard let transactionInput = try getExecuteTransactionInput(entries, constraints: constraints) else {
            // nothing to do
            return
        }
        let inputKeys = entries.map(\.compositePrimaryKey) + constraints.map(\.compositePrimaryKey)

        try await self.polymorphicTransactWrite(
            transactionInput,
            inputKeys: inputKeys,
            retriesRemaining: self.tableConfiguration.retry.numRetries
        )
    }

    private func getErrorReasons<AttributesType>(
        cancellationReasons: [DynamoDBClientTypes.CancellationReason],
        keys: [CompositePrimaryKey<AttributesType>],
        entryCount: Int
    ) throws -> (reasons: [DynamoDBTableError], isTransactionConflict: Bool) {
        var isTransactionConflict = false
        let reasons = try zip(cancellationReasons, keys).compactMap {
            cancellationReason,
            entryKey -> DynamoDBTableError? in
            let key: CompositePrimaryKey<AttributesType>? =
                if let item = cancellationReason.item {
                    try DynamoDBDecoder().decode(.m(item))
                } else {
                    nil
                }

            let partitionKey = key?.partitionKey ?? entryKey.partitionKey
            let sortKey = key?.sortKey ?? entryKey.sortKey

            // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ExecuteTransaction.html
            switch cancellationReason.code {
            case "None":
                return nil
            case "ConditionalCheckFailed":
                return DynamoDBTableError.conditionalCheckFailed(
                    partitionKey: partitionKey,
                    sortKey: sortKey,
                    message: cancellationReason.message
                )
            case "DuplicateItem":
                return DynamoDBTableError.duplicateItem(
                    partitionKey: partitionKey,
                    sortKey: sortKey,
                    message: cancellationReason.message
                )
            case "ItemCollectionSizeLimitExceeded":
                return DynamoDBTableError.itemCollectionSizeLimitExceeded(
                    attemptedSize: entryCount,
                    maximumSize: AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement
                )
            case "TransactionConflict":
                isTransactionConflict = true

                return DynamoDBTableError.transactionConflict(message: cancellationReason.message)
            case "ProvisionedThroughputExceeded":
                return DynamoDBTableError.provisionedThroughputExceeded(message: cancellationReason.message)
            case "ThrottlingError":
                return DynamoDBTableError.throttling(message: cancellationReason.message)
            case "ValidationError":
                return DynamoDBTableError.validation(
                    partitionKey: partitionKey,
                    sortKey: sortKey,
                    message: cancellationReason.message
                )
            default:
                return DynamoDBTableError.unknown(
                    code: cancellationReason.code,
                    partitionKey: partitionKey,
                    sortKey: sortKey,
                    message: cancellationReason.message
                )
            }
        }

        return (reasons, isTransactionConflict)
    }

    private func transactWrite<AttributesType, ItemType, TimeToLiveAttributesType>(
        _ entries: [WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>],
        constraints: [TransactionConstraintEntry<AttributesType, ItemType, TimeToLiveAttributesType>],
        retriesRemaining: Int
    ) async throws {
        let entryCount = entries.count + constraints.count

        if entryCount > AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement {
            throw DynamoDBTableError.itemCollectionSizeLimitExceeded(
                attemptedSize: entryCount,
                maximumSize: AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement
            )
        }

        let result: Swift.Result<Void, DynamoDBTableError>
        do {
            try await self.writeTransactionItems(entries, constraints: constraints)

            result = .success(())
        } catch let exception as TransactionCanceledException {
            guard let cancellationReasons = exception.properties.cancellationReasons else {
                throw DynamoDBTableError.transactionCanceled(reasons: [])
            }

            let keys: [CompositePrimaryKey<AttributesType>] =
                entries.map(\.compositePrimaryKey) + constraints.map(\.compositePrimaryKey)

            let (reasons, isTransactionConflict) = try getErrorReasons(
                cancellationReasons: cancellationReasons,
                keys: keys,
                entryCount: entryCount
            )

            if isTransactionConflict, retriesRemaining > 0 {
                return try await retryTransactWrite(
                    entries,
                    constraints: constraints,
                    retriesRemaining: retriesRemaining
                )
            }

            result = .failure(DynamoDBTableError.transactionCanceled(reasons: reasons))
        } catch let exception as TransactionConflictException {
            if retriesRemaining > 0 {
                return try await retryTransactWrite(
                    entries,
                    constraints: constraints,
                    retriesRemaining: retriesRemaining
                )
            }

            let reason = DynamoDBTableError.transactionConflict(message: exception.properties.message)

            result = .failure(DynamoDBTableError.transactionCanceled(reasons: [reason]))
        }

        let retryCount = self.tableConfiguration.retry.numRetries - retriesRemaining
        self.tableMetrics.transactWriteRetryCountRecorder?.record(retryCount)

        switch result {
        case .success:
            return
        case let .failure(failure):
            throw failure
        }
    }

    private func retryTransactWrite<AttributesType, ItemType, TimeToLiveAttributesType>(
        _ entries: [WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>],
        constraints: [TransactionConstraintEntry<AttributesType, ItemType, TimeToLiveAttributesType>],
        retriesRemaining: Int
    ) async throws {
        // determine the required interval
        let retryInterval = Int(self.tableConfiguration.retry.getRetryInterval(retriesRemaining: retriesRemaining))

        logger.warning(
            "Transaction retried due to conflict. Remaining retries: \(retriesRemaining). Retrying in \(retryInterval) ms."
        )
        try await Task.sleep(nanoseconds: UInt64(retryInterval) * millisecondsToNanoSeconds)

        logger.trace("Reattempting request due to remaining retries: \(retryInterval)")
        return try await self.transactWrite(entries, constraints: constraints, retriesRemaining: retriesRemaining - 1)
    }

    private func polymorphicTransactWrite<AttributesType: PrimaryKeyAttributes>(
        _ transactionInput: ExecuteTransactionInput,
        inputKeys: [CompositePrimaryKey<AttributesType>],
        retriesRemaining: Int
    ) async throws {
        if inputKeys.count > AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement {
            throw DynamoDBTableError.itemCollectionSizeLimitExceeded(
                attemptedSize: inputKeys.count,
                maximumSize: AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement
            )
        }

        let result: Swift.Result<Void, DynamoDBTableError>
        do {
            _ = try await dynamodb.executeTransaction(input: transactionInput)

            result = .success(())
        } catch let exception as TransactionCanceledException {
            guard let cancellationReasons = exception.properties.cancellationReasons else {
                throw DynamoDBTableError.transactionCanceled(reasons: [])
            }

            let (reasons, isTransactionConflict) = try getErrorReasons(
                cancellationReasons: cancellationReasons,
                keys: inputKeys,
                entryCount: inputKeys.count
            )

            if isTransactionConflict, retriesRemaining > 0 {
                return try await retryPolymorphicTransactWrite(
                    transactionInput,
                    inputKeys: inputKeys,
                    retriesRemaining: retriesRemaining
                )
            }

            result = .failure(DynamoDBTableError.transactionCanceled(reasons: reasons))
        } catch let exception as TransactionConflictException {
            if retriesRemaining > 0 {
                return try await retryPolymorphicTransactWrite(
                    transactionInput,
                    inputKeys: inputKeys,
                    retriesRemaining: retriesRemaining
                )
            }

            let reason = DynamoDBTableError.transactionConflict(message: exception.message)

            result = .failure(DynamoDBTableError.transactionCanceled(reasons: [reason]))
        }

        let retryCount = self.tableConfiguration.retry.numRetries - retriesRemaining
        self.tableMetrics.transactWriteRetryCountRecorder?.record(retryCount)

        switch result {
        case .success:
            return
        case let .failure(failure):
            throw failure
        }
    }

    private func retryPolymorphicTransactWrite(
        _ transactionInput: ExecuteTransactionInput,
        inputKeys: [CompositePrimaryKey<some PrimaryKeyAttributes>],
        retriesRemaining: Int
    ) async throws {
        // determine the required interval
        let retryInterval = Int(self.tableConfiguration.retry.getRetryInterval(retriesRemaining: retriesRemaining))

        logger.warning(
            "Transaction retried due to conflict. Remaining retries: \(retriesRemaining). Retrying in \(retryInterval) ms."
        )
        try await Task.sleep(nanoseconds: UInt64(retryInterval) * millisecondsToNanoSeconds)

        logger.trace("Reattempting request due to remaining retries: \(retryInterval)")
        return try await self.polymorphicTransactWrite(
            transactionInput,
            inputKeys: inputKeys,
            retriesRemaining: retriesRemaining - 1
        )
    }

    private func writeChunkedItems(
        _ entries: [some PolymorphicWriteEntry]
    ) async throws -> [DynamoDBClientTypes.BatchStatementResponse] {
        // if there are no items, there is nothing to update
        guard entries.count > 0 else {
            return []
        }

        let context = StandardPolymorphicWriteEntryContext<
            AWSDynamoDBPolymorphicWriteEntryTransform<Client>,
            AWSDynamoDBPolymorphicTransactionConstraintTransform<Client>
        >(table: self)
        let statements = try entries.map { entry -> DynamoDBClientTypes.BatchStatementRequest in
            let transform: AWSDynamoDBPolymorphicWriteEntryTransform<Client> = try entry.handle(context: context)
            let statement = transform.statement

            return DynamoDBClientTypes.BatchStatementRequest(
                consistentRead: self.tableConfiguration.consistentRead,
                statement: statement
            )
        }

        let executeInput = BatchExecuteStatementInput(statements: statements)

        let response = try await dynamodb.batchExecuteStatement(input: executeInput)
        return response.responses ?? []
    }

    public func polymorphicBulkWrite(_ entries: [some PolymorphicWriteEntry]) async throws {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedEntries = entries.chunked(by: AWSDynamoDBLimits.maximumUpdatesPerExecuteStatement)
        let zippedResponses = try await chunkedEntries.concurrentFlatMap { chunk in
            let responses = try await self.writeChunkedItems(chunk)

            return zip(responses, chunk)
        }

        let errors = zippedResponses.compactMap { response, item in
            response.error?.asDynamoDBTableError(
                partitionKey: item.compositePrimaryKey.partitionKey,
                sortKey: item.compositePrimaryKey.sortKey,
                entryCount: entries.count
            )
        }

        if !errors.isEmpty {
            throw DynamoDBTableError.batchFailures(errors: errors.removeDuplicates())
        }
    }

    private func writeChunkedItems(
        _ entries: [WriteEntry<some Any, some Any, some Any>]
    ) async throws -> [DynamoDBClientTypes.BatchStatementResponse] {
        // if there are no items, there is nothing to update
        guard entries.count > 0 else {
            return []
        }

        let statements = try entries.map { entry -> DynamoDBClientTypes.BatchStatementRequest in
            let statement: String =
                switch entry {
                case let .update(new: new, existing: existing):
                    try getUpdateExpression(
                        tableName: self.targetTableName,
                        newItem: new,
                        existingItem: existing
                    )
                case let .insert(new: new):
                    try getInsertExpression(
                        tableName: self.targetTableName,
                        newItem: new
                    )
                case let .deleteAtKey(key: key):
                    try getDeleteExpression(
                        tableName: self.targetTableName,
                        existingKey: key
                    )
                case let .deleteItem(existing: existing):
                    try getDeleteExpression(
                        tableName: self.targetTableName,
                        existingItem: existing
                    )
                }

            return DynamoDBClientTypes.BatchStatementRequest(
                consistentRead: self.tableConfiguration.consistentRead,
                statement: statement
            )
        }

        let executeInput = BatchExecuteStatementInput(statements: statements)

        do {
            let response = try await dynamodb.batchExecuteStatement(input: executeInput)
            return response.responses ?? []
        } catch {
            throw error.asUnrecognizedDynamoDBTableError()
        }
    }

    public func bulkWrite(_ entries: [WriteEntry<some Any, some Any, some Any>]) async throws {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedEntries = entries.chunked(by: AWSDynamoDBLimits.maximumUpdatesPerExecuteStatement)
        let zippedResponses = try await chunkedEntries.concurrentFlatMap { chunk in
            let responses = try await self.writeChunkedItems(chunk)

            return zip(responses, chunk)
        }

        let errors = zippedResponses.compactMap { response, item in
            response.error?.asDynamoDBTableError(
                partitionKey: item.compositePrimaryKey.partitionKey,
                sortKey: item.compositePrimaryKey.sortKey,
                entryCount: entries.count
            )
        }

        if !errors.isEmpty {
            throw DynamoDBTableError.batchFailures(errors: errors.removeDuplicates())
        }
    }

    private enum FallbackResult<
        AttributesType: PrimaryKeyAttributes,
        ItemType: Codable & Sendable,
        TimeToLiveAttributesType: TimeToLiveAttributes
    > {
        case unevaluated(WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>)
        case success
        case failure(DynamoDBTableError)
    }

    public func bulkWriteWithFallback<AttributesType, ItemType: Sendable, TimeToLiveAttributesType>(
        _ entries: [WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>]
    ) async throws {
        // fall back to single operation if the write entry exceeds the statement length limitation
        let results: [FallbackResult<AttributesType, ItemType, TimeToLiveAttributesType>] =
            try await entries.concurrentMap { entry in
                do {
                    try self.validateEntry(entry: entry)

                    return .unevaluated(entry)
                } catch DynamoDBTableError.statementLengthExceeded {
                    do {
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
                    } catch let error as DynamoDBTableError {
                        return .failure(error)
                    }
                }

                return .success
            }

        var bulkWriteEntries: [WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>] = []
        var errors: [DynamoDBTableError] = []
        for result in results {
            switch result {
            case let .unevaluated(entry):
                bulkWriteEntries.append(entry)
            case let .failure(error):
                errors.append(error)
            case .success:
                break
            }
        }

        let batchErrors: [DynamoDBTableError]
        do {
            try await self.bulkWrite(bulkWriteEntries)
            batchErrors = []
        } catch let DynamoDBTableError.batchFailures(bulkErrors) {
            batchErrors = bulkErrors
        }

        let combinedErrors = errors + batchErrors
        if !combinedErrors.isEmpty {
            // combine all errors and re-throw
            throw DynamoDBTableError.batchFailures(errors: combinedErrors.removeDuplicates())
        }
    }
}
