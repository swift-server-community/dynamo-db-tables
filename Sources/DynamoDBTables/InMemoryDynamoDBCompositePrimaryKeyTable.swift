// swiftlint:disable cyclomatic_complexity
//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/main/Sources/SmokeDynamoDB/InMemoryDynamoDBCompositePrimaryKeyTable.swift
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
//  InMemoryDynamoDBCompositePrimaryKeyTable.swift
//  DynamoDBTables
//

@preconcurrency import AWSDynamoDB
import Foundation

public extension TypedTTLDatabaseItem {
    var rowTypeIdentifier: String {
        getTypeRowIdentifier(type: RowType.self)
    }
}

public typealias ExecuteItemFilterType = @Sendable (String, String, String, InMemoryDatabaseItem)
    -> Bool

public protocol InMemoryTransactionDelegate: Sendable {
    /**
      Inject errors into a `transactWrite` or `polymorphicTransactWrite` call.
     */
    func injectErrors<AttributesType>(
        inputKeys: [CompositePrimaryKey<AttributesType>?], table: InMemoryDynamoDBCompositePrimaryKeyTable) async throws -> [DynamoDBTableError]
}

public struct InMemoryDynamoDBCompositePrimaryKeyTable: DynamoDBCompositePrimaryKeyTable, Sendable {
    public let escapeSingleQuoteInPartiQL: Bool
    public let transactionDelegate: InMemoryTransactionDelegate?
    public let executeItemFilter: ExecuteItemFilterType?
    let storeWrapper: InMemoryDynamoDBCompositePrimaryKeyTableStore

    public init(executeItemFilter: ExecuteItemFilterType? = nil,
                escapeSingleQuoteInPartiQL: Bool = false,
                transactionDelegate: InMemoryTransactionDelegate? = nil)
    {
        self.storeWrapper = InMemoryDynamoDBCompositePrimaryKeyTableStore()
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.transactionDelegate = transactionDelegate
        self.executeItemFilter = executeItemFilter
    }

    public var store: [String: [String: InMemoryDatabaseItem]] {
        get async {
            await self.storeWrapper.store
        }
    }

    public func validateEntry(entry: WriteEntry<some Any, some Any, some Any>) throws {
        let entryString = "\(entry)"
        if entryString.count > AWSDynamoDBLimits.maxStatementLength {
            throw DynamoDBTableError.statementLengthExceeded(
                reason: "failed to satisfy constraint: Member must have length less than or equal to "
                    + "\(AWSDynamoDBLimits.maxStatementLength). Actual length \(entryString.count)")
        }
    }

    public func insertItem(_ item: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
        let inMemoryDatabaseItem = try item.inMemoryFormWithKey()
        try await self.storeWrapper.execute { store in
            try self.insertItem(inMemoryDatabaseItem, store: &store)
        }
    }

    public func clobberItem(_ item: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
        let inMemoryDatabaseItem = try item.inMemoryForm()
        let compositePrimaryKey = item.compositePrimaryKey

        await self.storeWrapper.execute { store in
            let partition = store[compositePrimaryKey.partitionKey]

            // if there is already a partition
            var updatedPartition: [String: InMemoryDatabaseItem]
            if let partition {
                updatedPartition = partition

                updatedPartition[compositePrimaryKey.sortKey] = inMemoryDatabaseItem
            } else {
                updatedPartition = [compositePrimaryKey.sortKey: inMemoryDatabaseItem]
            }

            store[compositePrimaryKey.partitionKey] = updatedPartition
        }
    }

    public func updateItem<AttributesType, ItemType, TimeToLiveAttributesType>(
        newItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        existingItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>) async throws
    {
        let inMemoryDatabaseItem = try newItem.inMemoryForm()
        let existingItemMetadata = existingItem.asMetadataWithKey()
        try await self.storeWrapper.execute { store in
            try self.updateItem(newItem: inMemoryDatabaseItem, existingItemMetadata: existingItemMetadata, store: &store)
        }
    }

    public func transactWrite(
        _ entries: [WriteEntry<some Any, some Any, some Any>]) async throws
    {
        try await self.transactWrite(entries, constraints: [])
    }

    public func transactWrite<AttributesType, ItemType, TimeToLiveAttributesType>(
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

    public func polymorphicTransactWrite(_ entries: [some PolymorphicWriteEntry]) async throws {
        let noConstraints: [EmptyPolymorphicTransactionConstraintEntry] = []
        return try await self.polymorphicTransactWrite(entries, constraints: noConstraints)
    }

    public func polymorphicTransactWrite(
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

    public func polymorphicBulkWrite(_ entries: [some PolymorphicWriteEntry]) async throws {
        let context = StandardPolymorphicWriteEntryContext<InMemoryPolymorphicWriteEntryTransform,
            InMemoryPolymorphicTransactionConstraintTransform>(table: self)

        let entryTransformResults = entries.asInMemoryTransforms(context: context)

        try await self.storeWrapper.execute { store in
            try self.polymorphicBulkWrite(entryTransformResults, constraintTransformResults: [],
                                          store: &store, context: context, isTransaction: false)
        }
    }

    public func bulkWrite(_ entries: [WriteEntry<some Any, some Any, some Any>]) async throws {
        let inMemoryEntries = try entries.map { try $0.inMemoryForm() }
        try await self.storeWrapper.execute { store in
            try self.bulkWrite(inMemoryEntries, constraints: [], store: &store, isTransaction: false)
        }
    }

    public func bulkWriteWithFallback<AttributesType>(_ entries: [WriteEntry<AttributesType, some Any, some Any>]) async throws {
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

    public func bulkWriteWithoutThrowing(_ entries: [WriteEntry<some Any, some Any, some Any>]) async throws
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

    public func getItem<AttributesType, ItemType, TimeToLiveAttributesType>(forKey key: CompositePrimaryKey<AttributesType>) async throws
        -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>?
    {
        if let partition = await self.store[key.partitionKey] {
            guard let value = partition[key.sortKey] else {
                return nil
            }

            return try value.getItem()
        }

        return nil
    }

    public func polymorphicGetItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>]) async throws
        -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]
    {
        let store = await self.store

        var resultMap: [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType] = [:]
        for key in keys {
            if let value = store[key.partitionKey]?[key.sortKey] {
                let itemAsReturnedType: ReturnedType = try self.convertToQueryableType(input: value)

                resultMap[key] = itemAsReturnedType
            }
        }
        return resultMap
    }

    public func deleteItem(forKey key: CompositePrimaryKey<some Any>) async throws {
        try await self.storeWrapper.execute { store in
            try self.deleteItem(forKey: key, store: &store)
        }
    }

    public func deleteItems(forKeys keys: [CompositePrimaryKey<some Any>]) async throws {
        try await self.storeWrapper.execute { store in
            try keys.forEach { key in
                try self.deleteItem(forKey: key, store: &store)
            }
        }
    }

    public func deleteItems(existingItems: [TypedTTLDatabaseItem<some Any, some Any, some Any>]) async throws {
        let itemMetadataList = existingItems.map { $0.asMetadataWithKey() }
        try await self.storeWrapper.execute { store in
            try itemMetadataList.forEach { itemMetadata in
                try self.deleteItem(itemMetadata: itemMetadata, store: &store)
            }
        }
    }

    public func deleteItem(existingItem: TypedTTLDatabaseItem<some Any, some Any, some Any>) async throws {
        let itemMetadata = existingItem.asMetadataWithKey()
        try await self.storeWrapper.execute { store in
            try self.deleteItem(itemMetadata: itemMetadata, store: &store)
        }
    }

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                               sortKeyCondition: AttributeCondition?,
                                                                               consistentRead _: Bool) async throws
        -> [ReturnedType]
    {
        var items: [ReturnedType] = []

        if let partition = await self.store[partitionKey] {
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

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                               sortKeyCondition: AttributeCondition?,
                                                                               limit: Int?,
                                                                               exclusiveStartKey: String?,
                                                                               consistentRead: Bool) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        try await self.polymorphicQuery(forPartitionKey: partitionKey,
                                        sortKeyCondition: sortKeyCondition,
                                        limit: limit,
                                        scanIndexForward: true,
                                        exclusiveStartKey: exclusiveStartKey,
                                        consistentRead: consistentRead)
    }

    public func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                               sortKeyCondition: AttributeCondition?,
                                                                               limit: Int?,
                                                                               scanIndexForward: Bool,
                                                                               exclusiveStartKey: String?,
                                                                               consistentRead: Bool) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        // get all the results
        let rawItems: [ReturnedType] = try await polymorphicQuery(forPartitionKey: partitionKey,
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

    public func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter _: [String]?,
        additionalWhereClause: String?) async throws -> [ReturnedType]
    {
        let items = await self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)

        let returnedItems: [ReturnedType] = try items.map { item in
            try self.convertToQueryableType(input: item)
        }

        return returnedItems
    }

    public func polymorphicExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter _: [String]?,
        additionalWhereClause: String?, nextToken _: String?) async throws
        -> (items: [ReturnedType], lastEvaluatedKey: String?)
    {
        let items = await self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)

        let returnedItems: [ReturnedType] = try items.map { item in
            try self.convertToQueryableType(input: item)
        }

        return (returnedItems, nil)
    }

    public func execute<AttributesType, ItemType, TimeToLiveAttributesType>(
        partitionKeys: [String],
        attributesFilter _: [String]?,
        additionalWhereClause: String?) async throws -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        let items = await self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)

        let returnedItems: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] = try items.map { item in
            guard let typedItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType> = try item.getItem() else {
                let foundType = type(of: item)
                let description = "Expected to decode \(TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>.self). Instead found \(foundType)."
                let context = DecodingError.Context(codingPath: [], debugDescription: description)
                let error = DecodingError.typeMismatch(TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>.self, context)

                throw error
            }

            return typedItem
        }

        return returnedItems
    }

    public func execute<AttributesType, ItemType, TimeToLiveAttributesType>(
        partitionKeys: [String],
        attributesFilter _: [String]?,
        additionalWhereClause: String?, nextToken _: String?) async throws
        -> (items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>], lastEvaluatedKey: String?)
    {
        let items = await self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)

        let returnedItems: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] = try items.map { item in
            guard let typedItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType> = try item.getItem() else {
                let foundType = type(of: item)
                let description = "Expected to decode \(TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>.self). Instead found \(foundType)."
                let context = DecodingError.Context(codingPath: [], debugDescription: description)
                let error = DecodingError.typeMismatch(TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>.self, context)

                throw error
            }

            return typedItem
        }

        return (returnedItems, nil)
    }

    private func getInMemoryDatabaseItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) async
        -> [CompositePrimaryKey<AttributesType>: InMemoryDatabaseItem]
    {
        var map: [CompositePrimaryKey<AttributesType>: InMemoryDatabaseItem] = [:]

        let store = await self.storeWrapper.store

        for key in keys {
            if let partition = store[key.partitionKey] {
                guard let value = partition[key.sortKey] else {
                    continue
                }

                map[key] = value
            }
        }

        return map
    }

    public func getItems<AttributesType, ItemType, TimeToLiveAttributesType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws
        -> [CompositePrimaryKey<AttributesType>: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        let items = await self.getInMemoryDatabaseItems(forKeys: keys)

        var map: [CompositePrimaryKey<AttributesType>: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] = [:]

        try items.forEach { key, value in
            map[key] = try DynamoDBDecoder().decode(DynamoDBClientTypes.AttributeValue.m(value.item))
        }

        return map
    }

    public func query<AttributesType, ItemType, TimeToLiveAttributesType>(forPartitionKey partitionKey: String,
                                                                          sortKeyCondition: AttributeCondition?,
                                                                          consistentRead _: Bool) async throws
        -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        var items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] = []

        if let partition = await self.store[partitionKey] {
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

                if let typedValue: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType> = try value.getItem() {
                    items.append(typedValue)
                } else {
                    let description = "Expected type \(TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>.self), "
                        + " was \(type(of: value))."
                    let context = DecodingError.Context(codingPath: [], debugDescription: description)

                    throw DecodingError.typeMismatch(TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>.self, context)
                }
            }
        }

        return items
    }

    public func query<AttributesType, ItemType, TimeToLiveAttributesType>(forPartitionKey partitionKey: String,
                                                                          sortKeyCondition: AttributeCondition?,
                                                                          limit: Int?,
                                                                          scanIndexForward: Bool,
                                                                          exclusiveStartKey: String?,
                                                                          consistentRead: Bool) async throws
        -> (items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>], lastEvaluatedKey: String?)
    {
        // get all the results
        let rawItems: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>] = try await query(forPartitionKey: partitionKey,
                                                                                                                   sortKeyCondition: sortKeyCondition,
                                                                                                                   consistentRead: consistentRead)

        let items: [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
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

    func convertToQueryableType<ReturnedType: PolymorphicOperationReturnType>(input: InMemoryDatabaseItem) throws -> ReturnedType {
        let attributeValue = DynamoDBClientTypes.AttributeValue.m(input.item)

        let decodedItem: ReturnTypeDecodable<ReturnedType> = try DynamoDBDecoder().decode(attributeValue)

        return decodedItem.decodedValue
    }

    func getExecuteItems(partitionKeys: [String],
                         additionalWhereClause: String?) async -> [InMemoryDatabaseItem]
    {
        let store = await self.store

        var items: [InMemoryDatabaseItem] = []
        for partitionKey in partitionKeys {
            guard let partition = store[partitionKey] else {
                // no such partition, continue
                continue
            }

            for (sortKey, databaseItem) in partition {
                // if there is an additional where clause
                if let additionalWhereClause {
                    // there must be an executeItemFilter
                    if let executeItemFilter = self.executeItemFilter {
                        if executeItemFilter(partitionKey, sortKey, additionalWhereClause, databaseItem) {
                            // add if the filter says yes
                            items.append(databaseItem)
                        }
                    } else {
                        fatalError("An executeItemFilter must be provided when an excute call includes an additionalWhereClause")
                    }
                } else {
                    // otherwise just add the item
                    items.append(databaseItem)
                }
            }
        }

        return items
    }
}
