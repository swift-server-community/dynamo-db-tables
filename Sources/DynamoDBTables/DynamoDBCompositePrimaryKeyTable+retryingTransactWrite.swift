//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/DynamoDBCompositePrimaryKeyTable+conditionallyInTransaction.swift
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
//  DynamoDBCompositePrimaryKeyTable+retryingTransactWrite.swift
//  DynamoDBTables
//

import Foundation

public enum RetryingTransactWriteError<AttributesType: PrimaryKeyAttributes>: Error {
    case constraintFailure(reasons: [DynamoDBTableError])
    case concurrencyError(keys: [CompositePrimaryKey<AttributesType>], message: String?)
}

private struct TableKey: Hashable {
    let partitionKey: String
    let sortKey: String
}

private func hasConstraintFailure(
    reasons: [DynamoDBTableError],
    constraintKeys: Set<TableKey>
) -> Bool {
    guard !constraintKeys.isEmpty else { return false }

    return reasons.contains { reason in
        switch reason {
        case .conditionalCheckFailed(let pk, let sk, _):
            constraintKeys.contains(TableKey(partitionKey: pk, sortKey: sk))
        default:
            false
        }
    }
}

extension DynamoDBCompositePrimaryKeyTable {
    /**
     Method to perform a transaction for a set of keys. The `writeEntryProvider` will be called once for
     each key specified in the input, either with the current item corresponding to that key or nil if the
     item currently doesn't exist. The `writeEntryProvider` should return the `WriteEntry` for this key in
     the transaction or nil if the key should not be part of the transaction. The transaction may fail in
     which case the process repeats until the retry limit has been reached.
    
     - Parameters:
        - keys: the item keys to use in the transaction
        - withRetries: the number of times to attempt to retry the update before failing.
        - constraints: the contraints to include as part of the transaction
        - updatedPayloadProvider: the provider that will the `WriteEntry`s to use in the transaction.
     - Returns: the list of `WriteEntry` used in the successful transaction
     */
    @discardableResult
    public func retryingTransactWrite<AttributesType, ItemType, TimeToLiveAttributesType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>],
        withRetries retries: Int = 10,
        constraints: [TransactionConstraintEntry<AttributesType, ItemType, TimeToLiveAttributesType>] = [],
        writeEntryProvider:
            @Sendable @escaping (
                CompositePrimaryKey<AttributesType>,
                TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>?
            )
            async throws -> WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>?
    ) async throws
        -> [WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        let constraintKeys = Set(
            constraints.map {
                TableKey(partitionKey: $0.compositePrimaryKey.partitionKey, sortKey: $0.compositePrimaryKey.sortKey)
            }
        )

        return try await self.retryingTransactWriteInternal(
            forKeys: keys,
            withRetries: retries,
            constraintKeys: constraintKeys,
            constraints: constraints,
            writeEntryProvider: writeEntryProvider
        )
    }

    @discardableResult
    private func retryingTransactWriteInternal<AttributesType, ItemType, TimeToLiveAttributesType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>],
        withRetries retries: Int,
        constraintKeys: Set<TableKey>,
        constraints: [TransactionConstraintEntry<AttributesType, ItemType, TimeToLiveAttributesType>],
        writeEntryProvider:
            @Sendable @escaping (
                CompositePrimaryKey<AttributesType>,
                TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>?
            )
            async throws -> WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>?
    ) async throws
        -> [WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        guard retries > 0 else {
            throw RetryingTransactWriteError.concurrencyError(
                keys: keys,
                message: "Unable to complete conditional transact write in specified number of attempts"
            )
        }

        let existingItems:
            [CompositePrimaryKey<AttributesType>: TypedTTLDatabaseItem<
                AttributesType, ItemType, TimeToLiveAttributesType
            >] = try await getItems(forKeys: keys)

        let entries = try await keys.asyncCompactMap { key in
            try await writeEntryProvider(key, existingItems[key])
        }

        do {
            try await self.transactWrite(entries, constraints: constraints)

            return entries
        } catch DynamoDBTableError.transactionCanceled(let reasons) {
            if hasConstraintFailure(reasons: reasons, constraintKeys: constraintKeys) {
                throw RetryingTransactWriteError<AttributesType>.constraintFailure(reasons: reasons)
            }

            return try await self.retryingTransactWriteInternal(
                forKeys: keys,
                withRetries: retries - 1,
                constraintKeys: constraintKeys,
                constraints: constraints,
                writeEntryProvider: writeEntryProvider
            )
        }
    }

    /**
     Method to perform a transaction for a set of keys. The `writeEntryProvider` will be called once for
     each key specified in the input, either with the current item corresponding to that key or nil if the
     item currently doesn't exist. The `writeEntryProvider` should return the `WriteEntry` for this key in
     the transaction or nil if the key should not be part of the transaction. The transaction may fail in
     which case the process repeats until the retry limit has been reached.
    
     - Parameters:
        - keys: the item keys to use in the transaction
        - withRetries: the number of times to attempt to retry the update before failing.
        - updatedPayloadProvider: the provider that will the `WriteEntry`s to use in the transaction.
     - Returns: the list of `WriteEntry` used in the successful transaction
     */
    @discardableResult
    public func retryingPolymorphicTransactWrite<
        WriteEntryType: PolymorphicWriteEntry,
        ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType
    >(
        forKeys keys: [CompositePrimaryKey<WriteEntryType.AttributesType>],
        withRetries retries: Int = 10,
        writeEntryProvider:
            @Sendable @escaping (CompositePrimaryKey<WriteEntryType.AttributesType>, ReturnedType?)
            async throws -> WriteEntryType?
    ) async throws
        -> [WriteEntryType] where WriteEntryType.AttributesType == ReturnedType.AttributesType
    {
        let noConstraints: [EmptyPolymorphicTransactionConstraintEntry<WriteEntryType.AttributesType>] = []
        return try await self.retryingPolymorphicTransactWrite(
            forKeys: keys,
            withRetries: retries,
            constraints: noConstraints,
            writeEntryProvider: writeEntryProvider
        )
    }

    /**
     Method to perform a transaction for a set of keys. The `writeEntryProvider` will be called once for
     each key specified in the input, either with the current item corresponding to that key or nil if the
     item currently doesn't exist. The `writeEntryProvider` should return the `WriteEntry` for this key in
     the transaction or nil if the key should not be part of the transaction. The transaction may fail in
     which case the process repeats until the retry limit has been reached.
    
     - Parameters:
        - keys: the item keys to use in the transaction
        - withRetries: the number of times to attempt to retry the update before failing.
        - constraints: the contraints to include as part of the transaction
        - updatedPayloadProvider: the provider that will the `WriteEntry`s to use in the transaction.
     - Returns: the list of `WriteEntry` used in the successful transaction
     */
    @discardableResult
    public func retryingPolymorphicTransactWrite<
        WriteEntryType: PolymorphicWriteEntry,
        TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry,
        ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType
    >(
        forKeys keys: [CompositePrimaryKey<WriteEntryType.AttributesType>],
        withRetries retries: Int = 10,
        constraints: [TransactionConstraintEntryType],
        writeEntryProvider:
            @Sendable @escaping (CompositePrimaryKey<WriteEntryType.AttributesType>, ReturnedType?)
            async throws -> WriteEntryType?
    ) async throws
        -> [WriteEntryType]
    where
        WriteEntryType.AttributesType == ReturnedType.AttributesType,
        WriteEntryType.AttributesType == TransactionConstraintEntryType.AttributesType
    {
        let constraintKeys = Set(
            constraints.map {
                TableKey(partitionKey: $0.compositePrimaryKey.partitionKey, sortKey: $0.compositePrimaryKey.sortKey)
            }
        )

        return try await self.retryingPolymorphicTransactWriteInternal(
            forKeys: keys,
            withRetries: retries,
            constraintKeys: constraintKeys,
            constraints: constraints,
            writeEntryProvider: writeEntryProvider
        )
    }

    @discardableResult
    private func retryingPolymorphicTransactWriteInternal<
        WriteEntryType: PolymorphicWriteEntry,
        TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry,
        ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType
    >(
        forKeys keys: [CompositePrimaryKey<WriteEntryType.AttributesType>],
        withRetries retries: Int,
        constraintKeys: Set<TableKey>,
        constraints: [TransactionConstraintEntryType],
        writeEntryProvider:
            @Sendable @escaping (CompositePrimaryKey<WriteEntryType.AttributesType>, ReturnedType?)
            async throws -> WriteEntryType?
    ) async throws
        -> [WriteEntryType]
    where
        WriteEntryType.AttributesType == ReturnedType.AttributesType,
        WriteEntryType.AttributesType == TransactionConstraintEntryType.AttributesType
    {
        guard retries > 0 else {
            throw RetryingTransactWriteError.concurrencyError(
                keys: keys,
                message: "Unable to complete conditional transact write in specified number of attempts"
            )
        }

        let existingItems: [CompositePrimaryKey<WriteEntryType.AttributesType>: ReturnedType] =
            try await polymorphicGetItems(forKeys: keys)

        let entries = try await keys.asyncCompactMap { key in
            try await writeEntryProvider(key, existingItems[key])
        }

        do {
            try await self.polymorphicTransactWrite(entries, constraints: constraints)

            return entries
        } catch DynamoDBTableError.transactionCanceled(let reasons) {
            if hasConstraintFailure(reasons: reasons, constraintKeys: constraintKeys) {
                throw RetryingTransactWriteError<WriteEntryType.AttributesType>.constraintFailure(
                    reasons: reasons
                )
            }

            return try await self.retryingPolymorphicTransactWriteInternal(
                forKeys: keys,
                withRetries: retries - 1,
                constraintKeys: constraintKeys,
                constraints: constraints,
                writeEntryProvider: writeEntryProvider
            )
        }
    }
}
