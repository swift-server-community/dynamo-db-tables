//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of DynamoDBTables authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

//
//  InMemoryDynamoDBCompositePrimaryKeyTable+transact.swift
//  DynamoDBTables
//

@preconcurrency import AWSDynamoDB
import Foundation

// MARK: - Transaction implementations

public extension InMemoryDynamoDBCompositePrimaryKeyTable {
    func transactWrite(
        _ entries: [WriteEntry<some Any, some Any, some Any>]) async throws
    {
        try await self.transactWrite(entries, constraints: [])
    }

    func transactWrite<AttributesType, ItemType, TimeToLiveAttributesType>(
        _ entries: [WriteEntry<AttributesType, ItemType, TimeToLiveAttributesType>],
        constraints: [TransactionConstraintEntry<AttributesType, ItemType, TimeToLiveAttributesType>]) async throws
    {
        // if there is a transaction delegate and it wants to inject errors
        let inputKeys = entries.map(\.compositePrimaryKey) + constraints.map(\.compositePrimaryKey)
        if let errors = try await transactionDelegate?.injectErrors(inputKeys: inputKeys, table: self), !errors.isEmpty {
            throw DynamoDBTableError.transactionCanceled(reasons: errors)
        }

        let inMemoryEntries = try entries.map { try $0.inMemoryForm() }
        let inMemoryConstraints = try constraints.map { try $0.inMemoryForm() }

        try await self.storeWrapper.execute { store in
            try self.bulkWrite(inMemoryEntries, constraints: inMemoryConstraints, store: &store, isTransaction: true)
        }
    }

    func polymorphicTransactWrite(_ entries: [some PolymorphicWriteEntry]) async throws {
        let noConstraints: [EmptyPolymorphicTransactionConstraintEntry] = []
        return try await self.polymorphicTransactWrite(entries, constraints: noConstraints)
    }

    func polymorphicTransactWrite(
        _ entries: [some PolymorphicWriteEntry], constraints: [some PolymorphicTransactionConstraintEntry]) async throws
    {
        // if there is a transaction delegate and it wants to inject errors
        let inputKeys = entries.map(\.compositePrimaryKey) + constraints.map(\.compositePrimaryKey)
        if let errors = try await transactionDelegate?.injectErrors(inputKeys: inputKeys, table: self), !errors.isEmpty {
            throw DynamoDBTableError.transactionCanceled(reasons: errors)
        }

        let context = StandardPolymorphicWriteEntryContext<InMemoryPolymorphicWriteEntryTransform,
            InMemoryPolymorphicTransactionConstraintTransform>(table: self)

        let entryTransformResults = entries.asInMemoryTransforms(context: context)
        let contraintTransformResults = constraints.asInMemoryTransforms(context: context)

        try await self.storeWrapper.execute { store in
            try self.polymorphicBulkWrite(entryTransformResults, constraintTransformResults: contraintTransformResults,
                                          store: &store, context: context, isTransaction: true)
        }
    }

    func polymorphicBulkWrite(_ entries: [some PolymorphicWriteEntry]) async throws {
        let context = StandardPolymorphicWriteEntryContext<InMemoryPolymorphicWriteEntryTransform,
            InMemoryPolymorphicTransactionConstraintTransform>(table: self)

        let entryTransformResults = entries.asInMemoryTransforms(context: context)

        try await self.storeWrapper.execute { store in
            try self.polymorphicBulkWrite(entryTransformResults, constraintTransformResults: [],
                                          store: &store, context: context, isTransaction: false)
        }
    }

    func bulkWrite(_ entries: [WriteEntry<some Any, some Any, some Any>]) async throws {
        let inMemoryEntries = try entries.map { try $0.inMemoryForm() }
        try await self.storeWrapper.execute { store in
            try self.bulkWrite(inMemoryEntries, constraints: [], store: &store, isTransaction: false)
        }
    }

    func bulkWriteWithFallback<AttributesType>(_ entries: [WriteEntry<AttributesType, some Any, some Any>]) async throws {
        // fall back to single operation if the write entry exceeds the statement length limitation
        var bulkWriteEntries: [InMemoryWriteEntry<AttributesType>] = []
        var nonBulkWriteEntries: [InMemoryWriteEntry<AttributesType>] = []

        for entry in entries {
            do {
                try self.validateEntry(entry: entry)
                try bulkWriteEntries.append(entry.inMemoryForm())
            } catch DynamoDBTableError.statementLengthExceeded {
                try nonBulkWriteEntries.append(entry.inMemoryForm())
            }
        }

        try await self.storeWrapper.execute { store in
            try self.bulkWrite(bulkWriteEntries, constraints: [], store: &store, isTransaction: false)

            try nonBulkWriteEntries.forEach { nonBulkWriteEntry in
                switch nonBulkWriteEntry {
                case let .update(new: new, existing: existing):
                    try self.updateItem(newItem: new.inMemoryDatabaseItem,
                                        existingItemMetadata: existing.asMetadataWithKey(),
                                        store: &store)
                case let .insert(new: new):
                    try self.insertItem(new, store: &store)
                case let .deleteAtKey(key: key):
                    try self.deleteItem(forKey: key, store: &store)
                case let .deleteItem(existing: existing):
                    try self.deleteItem(itemMetadata: existing.asMetadataWithKey(), store: &store)
                }
            }
        }
    }

    func bulkWriteWithoutThrowing(_ entries: [WriteEntry<some Any, some Any, some Any>]) async throws
        -> Set<DynamoDBClientTypes.BatchStatementErrorCodeEnum>
    {
        let inMemoryEntries = try entries.map { try $0.inMemoryForm() }

        let results = await self.storeWrapper.execute { store in
            inMemoryEntries.map { entry -> Bool in
                switch entry {
                case let .update(new: new, existing: existing):
                    do {
                        try self.updateItem(newItem: new.inMemoryDatabaseItem,
                                            existingItemMetadata: existing.asMetadataWithKey(),
                                            store: &store)

                        return false
                    } catch {
                        return true
                    }
                case let .insert(new: new):
                    do {
                        try self.insertItem(new, store: &store)

                        return false
                    } catch {
                        return true
                    }
                case let .deleteAtKey(key: key):
                    do {
                        try self.deleteItem(forKey: key, store: &store)

                        return false
                    } catch {
                        return true
                    }
                case let .deleteItem(existing: existing):
                    do {
                        try self.deleteItem(itemMetadata: existing.asMetadataWithKey(), store: &store)

                        return false
                    } catch {
                        return true
                    }
                }
            }
        }

        var errors: Set<DynamoDBClientTypes.BatchStatementErrorCodeEnum> = Set()
        for result in results {
            if result {
                errors.insert(.duplicateitem)
            }
        }

        return errors
    }
}
