//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
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
//  DynamoDBCompositePrimaryKeyTable+retryingTransactWriteWithHistoricalRows.swift
//  DynamoDBTables
//

import Foundation

extension DynamoDBCompositePrimaryKeyTable {
    /**
     Method to perform a transaction for a set of keys, where each write entry can optionally produce a
     historical row that is written atomically in the same transaction. The `writeEntryProvider` will be
     called once for each key specified in the input, either with the current item corresponding to that key
     or nil if the item currently doesn't exist. The provider should return a tuple of the primary `WriteEntry`
     and an optional historical `WriteEntry`, or nil if the key should not be part of the transaction. The
     transaction may fail in which case the process repeats until the retry limit has been reached.
    
     The total number of write entries, historical entries, and constraints must not exceed the DynamoDB
     transaction limit of 100 items. This method validates the total count before attempting the transaction
     and throws `itemCollectionSizeLimitExceeded` if exceeded.
    
     - Parameters:
        - keys: the item keys to use in the transaction.
        - withRetries: the number of times to attempt to retry the update before failing.
        - constraints: the constraints to include as part of the transaction.
        - writeEntryProvider: the provider that returns a write entry and an optional historical entry per key.
     - Returns: the list of primary `WriteEntry` values used in the successful transaction (excluding historical entries).
     */
    @discardableResult
    public func retryingTransactWriteWithHistoricalRows<AttributesType, ItemType, TimeToLiveAttributesType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>],
        withRetries retries: Int = 10,
        constraints: [TransactionConstraintEntry<AttributesType, ItemType, TimeToLiveAttributesType>] = [],
        writeEntryProvider:
            @Sendable @escaping (
                CompositePrimaryKey<AttributesType>,
                TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>?
            )
            async throws -> (
                entry: WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>,
                historicalEntry: WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>?
            )?
    ) async throws
        -> [WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        let constraintKeys = Set(
            constraints.map {
                TableKey(partitionKey: $0.compositePrimaryKey.partitionKey, sortKey: $0.compositePrimaryKey.sortKey)
            }
        )

        return try await self.retryingTransactWriteWithHistoricalRowsInternal(
            forKeys: keys,
            withRetries: retries,
            constraintKeys: constraintKeys,
            constraints: constraints,
            writeEntryProvider: writeEntryProvider
        )
    }

    @discardableResult
    private func retryingTransactWriteWithHistoricalRowsInternal<AttributesType, ItemType, TimeToLiveAttributesType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>],
        withRetries retries: Int,
        constraintKeys: Set<TableKey>,
        constraints: [TransactionConstraintEntry<AttributesType, ItemType, TimeToLiveAttributesType>],
        writeEntryProvider:
            @Sendable @escaping (
                CompositePrimaryKey<AttributesType>,
                TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>?
            )
            async throws -> (
                entry: WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>,
                historicalEntry: WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>?
            )?
    ) async throws
        -> [WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        guard retries > 0 else {
            let firstKey = keys.first
            let allKeysDescription = keys.map { "(\($0.partitionKey), \($0.sortKey))" }.joined(separator: ", ")
            throw DynamoDBTableError.concurrencyError(
                partitionKey: firstKey?.partitionKey ?? "",
                sortKey: firstKey?.sortKey ?? "",
                message:
                    "Unable to complete conditional transact write in specified number of attempts for keys: \(allKeysDescription)"
            )
        }

        let existingItems:
            [CompositePrimaryKey<AttributesType>: TypedTTLDatabaseItem<
                AttributesType, ItemType, TimeToLiveAttributesType
            >] = try await getItems(forKeys: keys)

        var entries: [WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>] = []
        var historicalEntries: [WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>] = []

        for key in keys {
            if let result = try await writeEntryProvider(key, existingItems[key]) {
                entries.append(result.entry)
                if let historicalEntry = result.historicalEntry {
                    historicalEntries.append(historicalEntry)
                }
            }
        }

        let totalEntryCount = entries.count + historicalEntries.count + constraints.count
        guard totalEntryCount <= AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement else {
            throw DynamoDBTableError.itemCollectionSizeLimitExceeded(
                attemptedSize: totalEntryCount,
                maximumSize: AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement
            )
        }

        let allEntries = entries + historicalEntries

        do {
            try await self.transactWrite(allEntries, constraints: constraints)

            return entries
        } catch DynamoDBTableError.transactionCanceled(let reasons) {
            if hasConstraintFailure(reasons: reasons, constraintKeys: constraintKeys) {
                throw DynamoDBTableError.constraintFailure(reasons: reasons)
            }

            return try await self.retryingTransactWriteWithHistoricalRowsInternal(
                forKeys: keys,
                withRetries: retries - 1,
                constraintKeys: constraintKeys,
                constraints: constraints,
                writeEntryProvider: writeEntryProvider
            )
        }
    }
}
