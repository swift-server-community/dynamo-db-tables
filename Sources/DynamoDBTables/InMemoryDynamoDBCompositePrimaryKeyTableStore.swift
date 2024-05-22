// swiftlint:disable cyclomatic_complexity
//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/main/Sources/SmokeDynamoDB/InMemoryDynamoDBCompositePrimaryKeyTableStore.swift
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
//  InMemoryDynamoDBCompositePrimaryKeyTableStore.swift
//  DynamoDBTables
//

import AWSDynamoDB
import Foundation

private let itemAlreadyExistsMessage = "Row already exists."

// MARK: - Transforms

struct InMemoryPolymorphicWriteEntryTransform: PolymorphicWriteEntryTransform {
    typealias TableType = InMemoryDynamoDBCompositePrimaryKeyTableStore

    let operation: (inout TableType.StoreType) throws -> Void

    init(_ entry: WriteEntry<some PrimaryKeyAttributes, some Codable>, table _: TableType) throws {
        switch entry {
        case let .update(new: new, existing: existing):
            self.operation = { store in
                try updateItem(newItem: new, existingItem: existing, store: &store)
            }
        case let .insert(new: new):
            self.operation = { store in
                try insertItem(new, store: &store)
            }
        case let .deleteAtKey(key: key):
            self.operation = { store in
                try deleteItem(forKey: key, store: &store)
            }
        case let .deleteItem(existing: existing):
            self.operation = { store in
                try deleteItem(existingItem: existing, store: &store)
            }
        }
    }
}

struct InMemoryPolymorphicTransactionConstraintTransform: PolymorphicTransactionConstraintTransform {
    typealias TableType = InMemoryDynamoDBCompositePrimaryKeyTableStore

    let partitionKey: String
    let sortKey: String
    let rowVersion: Int

    init(_ entry: TransactionConstraintEntry<some PrimaryKeyAttributes, some Codable>,
         table _: TableType) throws
    {
        switch entry {
        case let .required(existing: existing):
            self.partitionKey = existing.compositePrimaryKey.partitionKey
            self.sortKey = existing.compositePrimaryKey.sortKey
            self.rowVersion = existing.rowStatus.rowVersion
        }
    }
}

// MARK: - Shared implementations

// Can be used directly by `InMemoryPolymorphicTransactionConstraintTransform` or through the `InMemoryPolymorphicWriteEntryTransform`

private func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                  existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                  store: inout [String: [String: PolymorphicOperationReturnTypeConvertable]]) throws
{
    let partition = store[newItem.compositePrimaryKey.partitionKey]

    // if there is already a partition
    var updatedPartition: [String: PolymorphicOperationReturnTypeConvertable]
    if let partition {
        updatedPartition = partition

        // if the row already exists
        if let actuallyExistingItem = partition[newItem.compositePrimaryKey.sortKey] {
            if existingItem.rowStatus.rowVersion != actuallyExistingItem.rowStatus.rowVersion ||
                existingItem.createDate.iso8601 != actuallyExistingItem.createDate.iso8601
            {
                throw DynamoDBTableError.conditionalCheckFailed(partitionKey: newItem.compositePrimaryKey.partitionKey,
                                                                sortKey: newItem.compositePrimaryKey.sortKey,
                                                                message: "Trying to overwrite incorrect version.")
            }
        } else {
            throw DynamoDBTableError.conditionalCheckFailed(partitionKey: newItem.compositePrimaryKey.partitionKey,
                                                            sortKey: newItem.compositePrimaryKey.sortKey,
                                                            message: "Existing item does not exist.")
        }

        updatedPartition[newItem.compositePrimaryKey.sortKey] = newItem
    } else {
        throw DynamoDBTableError.conditionalCheckFailed(partitionKey: newItem.compositePrimaryKey.partitionKey,
                                                        sortKey: newItem.compositePrimaryKey.sortKey,
                                                        message: "Existing item does not exist.")
    }

    store[newItem.compositePrimaryKey.partitionKey] = updatedPartition
}

private func insertItem(_ item: TypedDatabaseItem<some Any, some Any>,
                        store: inout [String: [String: PolymorphicOperationReturnTypeConvertable]]) throws
{
    let partition = store[item.compositePrimaryKey.partitionKey]

    // if there is already a partition
    var updatedPartition: [String: PolymorphicOperationReturnTypeConvertable]
    if let partition {
        updatedPartition = partition

        // if the row already exists
        if partition[item.compositePrimaryKey.sortKey] != nil {
            throw DynamoDBTableError.conditionalCheckFailed(partitionKey: item.compositePrimaryKey.partitionKey,
                                                            sortKey: item.compositePrimaryKey.sortKey,
                                                            message: "Row already exists.")
        }

        updatedPartition[item.compositePrimaryKey.sortKey] = item
    } else {
        updatedPartition = [item.compositePrimaryKey.sortKey: item]
    }

    store[item.compositePrimaryKey.partitionKey] = updatedPartition
}

private func deleteItem(forKey key: CompositePrimaryKey<some Any>,
                        store: inout [String: [String: PolymorphicOperationReturnTypeConvertable]]) throws
{
    store[key.partitionKey]?[key.sortKey] = nil
}

private func deleteItem(existingItem: some DatabaseItem,
                        store: inout [String: [String: PolymorphicOperationReturnTypeConvertable]]) throws
{
    let partition = store[existingItem.compositePrimaryKey.partitionKey]

    // if there is already a partition
    var updatedPartition: [String: PolymorphicOperationReturnTypeConvertable]
    if let partition {
        updatedPartition = partition

        // if the row already exists
        if let actuallyExistingItem = partition[existingItem.compositePrimaryKey.sortKey] {
            if existingItem.rowStatus.rowVersion != actuallyExistingItem.rowStatus.rowVersion ||
                existingItem.createDate.iso8601 != actuallyExistingItem.createDate.iso8601
            {
                throw DynamoDBTableError.conditionalCheckFailed(partitionKey: existingItem.compositePrimaryKey.partitionKey,
                                                                sortKey: existingItem.compositePrimaryKey.sortKey,
                                                                message: "Trying to delete incorrect version.")
            }
        } else {
            throw DynamoDBTableError.conditionalCheckFailed(partitionKey: existingItem.compositePrimaryKey.partitionKey,
                                                            sortKey: existingItem.compositePrimaryKey.sortKey,
                                                            message: "Existing item does not exist.")
        }

        updatedPartition[existingItem.compositePrimaryKey.sortKey] = nil
    } else {
        throw DynamoDBTableError.conditionalCheckFailed(partitionKey: existingItem.compositePrimaryKey.partitionKey,
                                                        sortKey: existingItem.compositePrimaryKey.sortKey,
                                                        message: "Existing item does not exist.")
    }

    store[existingItem.compositePrimaryKey.partitionKey] = updatedPartition
}

// MARK: - Store implementation

actor InMemoryDynamoDBCompositePrimaryKeyTableStore {
    typealias StoreType = [String: [String: PolymorphicOperationReturnTypeConvertable]]

    var store: StoreType = [:]
    let executeItemFilter: ExecuteItemFilterType?

    init(executeItemFilter: ExecuteItemFilterType? = nil) {
        self.executeItemFilter = executeItemFilter
    }

    nonisolated func validateEntry(entry: WriteEntry<some Any, some Any>) throws {
        let entryString = "\(entry)"
        if entryString.count > AWSDynamoDBLimits.maxStatementLength {
            throw DynamoDBTableError.statementLengthExceeded(
                reason: "failed to satisfy constraint: Member must have length less than or equal to "
                    + "\(AWSDynamoDBLimits.maxStatementLength). Actual length \(entryString.count)")
        }
    }

    func insertItem(_ item: TypedDatabaseItem<some Any, some Any>) throws {
        try DynamoDBTables.insertItem(item, store: &self.store)
    }

    func clobberItem(_ item: TypedDatabaseItem<some Any, some Any>) throws {
        let partition = self.store[item.compositePrimaryKey.partitionKey]

        // if there is already a partition
        var updatedPartition: [String: PolymorphicOperationReturnTypeConvertable]
        if let partition {
            updatedPartition = partition

            updatedPartition[item.compositePrimaryKey.sortKey] = item
        } else {
            updatedPartition = [item.compositePrimaryKey.sortKey: item]
        }

        self.store[item.compositePrimaryKey.partitionKey] = updatedPartition
    }

    func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                              existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws
    {
        try DynamoDBTables.updateItem(newItem: newItem, existingItem: existingItem, store: &self.store)
    }

    func bulkWrite(
        _ entries: [some PolymorphicWriteEntry], constraints: [some PolymorphicTransactionConstraintEntry],
        isTransaction: Bool) throws
    {
        let entryCount = entries.count + constraints.count
        let context = StandardPolymorphicWriteEntryContext<InMemoryPolymorphicWriteEntryTransform,
            InMemoryPolymorphicTransactionConstraintTransform>(table: self)

        if entryCount > AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement {
            throw DynamoDBTableError.transactionSizeExceeded(attemptedSize: entryCount,
                                                             maximumSize: AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement)
        }

        let store = self.store

        if let error = self.handleConstraints(constraints: constraints, isTransaction: isTransaction, context: context) {
            throw error
        }

        if let error = self.handleEntries(entries: entries, isTransaction: isTransaction, context: context) {
            if isTransaction {
                // restore the state prior to the transaction
                self.store = store
            }

            throw error
        }
    }

    func monomorphicBulkWrite(_ entries: [WriteEntry<some Any, some Any>]) throws {
        try entries.forEach { entry in
            switch entry {
            case let .update(new: new, existing: existing):
                return try self.updateItem(newItem: new, existingItem: existing)
            case let .insert(new: new):
                return try self.insertItem(new)
            case let .deleteAtKey(key: key):
                return try self.deleteItem(forKey: key)
            case let .deleteItem(existing: existing):
                return try self.deleteItem(existingItem: existing)
            }
        }
    }

    func monomorphicBulkWriteWithFallback<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) throws {
        // fall back to single operation if the write entry exceeds the statement length limitation
        var nonBulkWriteEntries: [WriteEntry<AttributesType, ItemType>] = []

        let bulkWriteEntries = try entries.compactMap { entry in
            do {
                try self.validateEntry(entry: entry)
                return entry
            } catch DynamoDBTableError.statementLengthExceeded {
                nonBulkWriteEntries.append(entry)
                return nil
            }
        }

        try self.monomorphicBulkWrite(bulkWriteEntries)

        try nonBulkWriteEntries.forEach { nonBulkWriteEntry in
            switch nonBulkWriteEntry {
            case let .update(new: new, existing: existing):
                try self.updateItem(newItem: new, existingItem: existing)
            case let .insert(new: new):
                try self.insertItem(new)
            case let .deleteAtKey(key: key):
                try self.deleteItem(forKey: key)
            case let .deleteItem(existing: existing):
                try self.deleteItem(existingItem: existing)
            }
        }
    }

    func monomorphicBulkWriteWithoutThrowing(
        _ entries: [WriteEntry<some Any, some Any>]) throws
        -> Set<DynamoDBClientTypes.BatchStatementErrorCodeEnum>
    {
        let results = entries.map { entry -> DynamoDBClientTypes.BatchStatementErrorCodeEnum? in
            switch entry {
            case let .update(new: new, existing: existing):
                do {
                    try self.updateItem(newItem: new, existingItem: existing)

                    return nil
                } catch {
                    return .duplicateitem
                }
            case let .insert(new: new):
                do {
                    try self.insertItem(new)

                    return nil
                } catch {
                    return .duplicateitem
                }
            case let .deleteAtKey(key: key):
                do {
                    try self.deleteItem(forKey: key)

                    return nil
                } catch {
                    return .duplicateitem
                }
            case let .deleteItem(existing: existing):
                do {
                    try self.deleteItem(existingItem: existing)

                    return nil
                } catch {
                    return .duplicateitem
                }
            }
        }

        var errors: Set<DynamoDBClientTypes.BatchStatementErrorCodeEnum> = Set()
        for result in results {
            if let result {
                errors.insert(result)
            }
        }
        return errors
    }

    func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) throws
        -> TypedDatabaseItem<AttributesType, ItemType>?
    {
        if let partition = self.store[key.partitionKey] {
            guard let value = partition[key.sortKey] else {
                return nil
            }

            guard let item = value as? TypedDatabaseItem<AttributesType, ItemType> else {
                let foundType = type(of: value)
                let description = "Expected to decode \(TypedDatabaseItem<AttributesType, ItemType>.self). Instead found \(foundType)."
                let context = DecodingError.Context(codingPath: [], debugDescription: description)

                throw DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
            }

            return item
        }

        return nil
    }

    func getItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>]) throws
        -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]
    {
        var map: [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType] = [:]

        try keys.forEach { key in
            if let partition = self.store[key.partitionKey] {
                guard let value = partition[key.sortKey] else {
                    return
                }

                let itemAsReturnedType: ReturnedType = try self.convertToQueryableType(input: value)

                map[key] = itemAsReturnedType
            }
        }

        return map
    }

    func deleteItem(forKey key: CompositePrimaryKey<some Any>) throws {
        try DynamoDBTables.deleteItem(forKey: key, store: &self.store)
    }

    func deleteItem(existingItem: some DatabaseItem) throws {
        try DynamoDBTables.deleteItem(existingItem: existingItem, store: &self.store)
    }

    func deleteItems(forKeys keys: [CompositePrimaryKey<some Any>]) throws {
        try keys.forEach { key in
            try self.deleteItem(forKey: key)
        }
    }

    func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType]) throws {
        try existingItems.forEach { (existingItem: ItemType) in
            try self.deleteItem(existingItem: existingItem)
        }
    }

    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             consistentRead _: Bool) throws
        -> [ReturnedType]
    {
        var items: [ReturnedType] = []

        if let partition = self.store[partitionKey] {
            let sortedPartition = partition.sorted(by: { left, right -> Bool in
                left.key < right.key
            })

            sortKeyIteration: for (sortKey, value) in sortedPartition {
                if let currentSortKeyCondition = sortKeyCondition {
                    switch currentSortKeyCondition {
                    case let .equals(value):
                        if !(value == sortKey) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case let .lessThan(value):
                        if !(sortKey < value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case let .lessThanOrEqual(value):
                        if !(sortKey <= value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case let .greaterThan(value):
                        if !(sortKey > value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case let .greaterThanOrEqual(value):
                        if !(sortKey >= value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case let .between(value1, value2):
                        if !(sortKey > value1 && sortKey < value2) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case let .beginsWith(value):
                        if !(sortKey.hasPrefix(value)) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    }
                }

                try items.append(self.convertToQueryableType(input: value))
            }
        }

        return items
    }

    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             limit: Int?,
                                                             scanIndexForward: Bool,
                                                             exclusiveStartKey: String?,
                                                             consistentRead: Bool) throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        // get all the results
        let rawItems: [ReturnedType] = try query(forPartitionKey: partitionKey,
                                                 sortKeyCondition: sortKeyCondition,
                                                 consistentRead: consistentRead)

        let items: [ReturnedType]
        if !scanIndexForward {
            items = rawItems.reversed()
        } else {
            items = rawItems
        }

        let startIndex: Int
        // if there is an exclusiveStartKey
        if let exclusiveStartKey {
            guard let storedStartIndex = Int(exclusiveStartKey) else {
                fatalError("Unexpectedly encoded exclusiveStartKey '\(exclusiveStartKey)'")
            }

            startIndex = storedStartIndex
        } else {
            startIndex = 0
        }

        let endIndex: Int
        let lastEvaluatedKey: String?
        if let limit, startIndex + limit < items.count {
            endIndex = startIndex + limit
            lastEvaluatedKey = String(endIndex)
        } else {
            endIndex = items.count
            lastEvaluatedKey = nil
        }

        return (Array(items[startIndex ..< endIndex]), lastEvaluatedKey)
    }
}

// MARK: - Internal helper functions

extension InMemoryDynamoDBCompositePrimaryKeyTableStore {
    func handleConstraints(
        constraints: [some PolymorphicTransactionConstraintEntry], isTransaction: Bool,
        context: StandardPolymorphicWriteEntryContext<InMemoryPolymorphicWriteEntryTransform,
            InMemoryPolymorphicTransactionConstraintTransform>)
        -> DynamoDBTableError?
    {
        let errors = constraints.compactMap { entry -> DynamoDBTableError? in
            let transform: InMemoryPolymorphicTransactionConstraintTransform
            do {
                transform = try entry.handle(context: context)
            } catch {
                return DynamoDBTableError.unexpectedError(cause: error)
            }

            guard let partition = store[transform.partitionKey],
                  let item = partition[transform.sortKey],
                  item.rowStatus.rowVersion == transform.rowVersion
            else {
                if isTransaction {
                    return DynamoDBTableError.transactionConditionalCheckFailed(partitionKey: transform.partitionKey,
                                                                                sortKey: transform.sortKey,
                                                                                message: "Item doesn't exist or doesn't have correct version")
                } else {
                    return DynamoDBTableError.conditionalCheckFailed(partitionKey: transform.partitionKey,
                                                                     sortKey: transform.sortKey,
                                                                     message: "Item doesn't exist or doesn't have correct version")
                }
            }

            return nil
        }

        if !errors.isEmpty {
            return DynamoDBTableError.transactionCanceled(reasons: errors)
        }

        return nil
    }

    func handleEntries(
        entries: [some PolymorphicWriteEntry], isTransaction: Bool,
        context: StandardPolymorphicWriteEntryContext<InMemoryPolymorphicWriteEntryTransform,
            InMemoryPolymorphicTransactionConstraintTransform>)
        -> DynamoDBTableError?
    {
        let writeErrors = entries.compactMap { entry -> DynamoDBTableError? in
            let transform: InMemoryPolymorphicWriteEntryTransform
            do {
                transform = try entry.handle(context: context)
            } catch {
                return DynamoDBTableError.unexpectedError(cause: error)
            }

            do {
                try transform.operation(&self.store)
            } catch {
                if let typedError = error as? DynamoDBTableError {
                    if case let .conditionalCheckFailed(partitionKey, sortKey, message) = typedError, isTransaction {
                        if message == itemAlreadyExistsMessage {
                            return .duplicateItem(partitionKey: partitionKey, sortKey: sortKey, message: message)
                        } else {
                            return .transactionConditionalCheckFailed(partitionKey: partitionKey,
                                                                      sortKey: sortKey, message: message)
                        }
                    }
                    return typedError
                }

                // return unexpected error
                return DynamoDBTableError.unexpectedError(cause: error)
            }

            return nil
        }

        if writeErrors.count > 0 {
            if isTransaction {
                return DynamoDBTableError.transactionCanceled(reasons: writeErrors)
            } else {
                return DynamoDBTableError.batchErrorsReturned(errorCount: writeErrors.count, messageMap: [:])
            }
        }

        return nil
    }

    func convertToQueryableType<ReturnedType: PolymorphicOperationReturnType>(input: PolymorphicOperationReturnTypeConvertable) throws -> ReturnedType {
        let storedRowTypeName = input.rowTypeIdentifier

        var queryableTypeProviders: [String: PolymorphicOperationReturnOption<ReturnedType.AttributesType, ReturnedType>] = [:]
        for (type, provider) in ReturnedType.types {
            queryableTypeProviders[getTypeRowIdentifier(type: type)] = provider
        }

        if let provider = queryableTypeProviders[storedRowTypeName] {
            return try provider.getReturnType(input: input)
        } else {
            // throw an exception, we don't know what this type is
            throw DynamoDBTableError.unexpectedType(provided: storedRowTypeName)
        }
    }

    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             limit: Int?,
                                                             exclusiveStartKey: String?,
                                                             consistentRead: Bool) throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        try self.query(forPartitionKey: partitionKey,
                       sortKeyCondition: sortKeyCondition,
                       limit: limit,
                       scanIndexForward: true,
                       exclusiveStartKey: exclusiveStartKey,
                       consistentRead: consistentRead)
    }
}
