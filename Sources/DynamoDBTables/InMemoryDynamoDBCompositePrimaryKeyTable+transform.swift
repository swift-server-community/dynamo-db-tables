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
//  InMemoryDynamoDBCompositePrimaryKeyTable+transform.swift
//  DynamoDBTables
//

@preconcurrency import AWSDynamoDB

// MARK: - Transforms

struct InMemoryPolymorphicWriteEntryTransform: PolymorphicWriteEntryTransform, Sendable {
    typealias TableType = InMemoryDynamoDBCompositePrimaryKeyTable

    let operation: @Sendable (inout InMemoryDynamoDBCompositePrimaryKeyTableStore.StoreType) throws -> Void

    init(_ entry: WriteEntry<some PrimaryKeyAttributes, some Any, some Any>, table: TableType) throws {
        switch entry {
        case let .update(new: new, existing: existing):
            let inMemoryNewItem = try new.inMemoryForm()
            let existingItemMetadata = existing.asMetadataWithKey()
            self.operation = { store in
                try table.updateItem(
                    newItem: inMemoryNewItem,
                    existingItemMetadata: existingItemMetadata,
                    store: &store
                )
            }
        case let .insert(new: new):
            let inMemoryNewItem = try new.inMemoryFormWithKey()
            self.operation = { store in
                try table.insertItem(inMemoryNewItem, store: &store)
            }
        case let .deleteAtKey(key: key):
            self.operation = { store in
                try table.deleteItem(forKey: key, store: &store)
            }
        case let .deleteItem(existing: existing):
            let existingItemMetadata = existing.asMetadataWithKey()
            self.operation = { store in
                try table.deleteItem(itemMetadata: existingItemMetadata, store: &store)
            }
        }
    }
}

extension Array where Element: PolymorphicWriteEntry {
    func asInMemoryTransforms(
        context: StandardPolymorphicWriteEntryContext<
            InMemoryPolymorphicWriteEntryTransform,
            InMemoryPolymorphicTransactionConstraintTransform
        >
    )
        -> [Swift.Result<InMemoryPolymorphicWriteEntryTransform, DynamoDBTableError>]
    {
        self.map { entry in
            let transform: InMemoryPolymorphicWriteEntryTransform
            do {
                transform = try entry.handle(context: context)
            } catch {
                return .failure(DynamoDBTableError.unexpectedError(cause: error))
            }

            return .success(transform)
        }
    }
}

struct InMemoryPolymorphicTransactionConstraintTransform: PolymorphicTransactionConstraintTransform, Sendable {
    typealias TableType = InMemoryDynamoDBCompositePrimaryKeyTable

    let partitionKey: String
    let sortKey: String
    let rowVersion: Int

    init(
        _ entry: TransactionConstraintEntry<
            some PrimaryKeyAttributes, some Codable & Sendable, some TimeToLiveAttributes
        >,
        table _: TableType
    ) throws {
        switch entry {
        case let .required(existing: existing):
            self.partitionKey = existing.compositePrimaryKey.partitionKey
            self.sortKey = existing.compositePrimaryKey.sortKey
            self.rowVersion = existing.rowStatus.rowVersion
        }
    }
}

extension Array where Element: PolymorphicTransactionConstraintEntry {
    func asInMemoryTransforms(
        context: StandardPolymorphicWriteEntryContext<
            InMemoryPolymorphicWriteEntryTransform,
            InMemoryPolymorphicTransactionConstraintTransform
        >
    )
        -> [Swift.Result<InMemoryPolymorphicTransactionConstraintTransform, DynamoDBTableError>]
    {
        self.map { entry in
            let transform: InMemoryPolymorphicTransactionConstraintTransform
            do {
                transform = try entry.handle(context: context)
            } catch {
                return .failure(DynamoDBTableError.unexpectedError(cause: error))
            }

            return .success(transform)
        }
    }
}

// MARK: - Shared implementations

private let itemAlreadyExistsMessage = "Row already exists."

extension InMemoryDynamoDBCompositePrimaryKeyTable {
    // Can be used directly by `InMemoryPolymorphicTransactionConstraintTransform` or through the `InMemoryPolymorphicWriteEntryTransform`

    func insertItem(
        _ item: InMemoryDatabaseItemWithKey<some Any>,
        store: inout [String: [String: InMemoryDatabaseItem]]
    ) throws {
        let key = item.compositePrimaryKey
        let partition = store[key.partitionKey]

        // if there is already a partition
        var updatedPartition: [String: InMemoryDatabaseItem]
        if let partition {
            updatedPartition = partition

            // if the row already exists
            if partition[item.compositePrimaryKey.sortKey] != nil {
                throw DynamoDBTableError.conditionalCheckFailed(
                    partitionKey: key.partitionKey,
                    sortKey: key.sortKey,
                    message: "Row already exists."
                )
            }

            updatedPartition[key.sortKey] = item.inMemoryDatabaseItem
        } else {
            updatedPartition = [key.sortKey: item.inMemoryDatabaseItem]
        }

        store[key.partitionKey] = updatedPartition
    }

    func updateItem(
        newItem: InMemoryDatabaseItem,
        existingItemMetadata: DatabaseItemMetadataWithKey<some Any>,
        store: inout [String: [String: InMemoryDatabaseItem]]
    ) throws {
        let key = existingItemMetadata.compositePrimaryKey
        let partition = store[key.partitionKey]

        // if there is already a partition
        var updatedPartition: [String: InMemoryDatabaseItem]
        if let partition {
            updatedPartition = partition

            // if the row already exists
            if let actuallyExistingItem = partition[key.sortKey] {
                if existingItemMetadata.rowStatus.rowVersion != actuallyExistingItem.rowStatus.rowVersion
                    || existingItemMetadata.createDate.iso8601 != actuallyExistingItem.createDate.iso8601
                {
                    throw DynamoDBTableError.conditionalCheckFailed(
                        partitionKey: key.partitionKey,
                        sortKey: key.sortKey,
                        message: "Trying to overwrite incorrect version."
                    )
                }
            } else {
                throw DynamoDBTableError.conditionalCheckFailed(
                    partitionKey: key.partitionKey,
                    sortKey: key.sortKey,
                    message: "Existing item does not exist."
                )
            }

            updatedPartition[key.sortKey] = newItem
        } else {
            throw DynamoDBTableError.conditionalCheckFailed(
                partitionKey: key.partitionKey,
                sortKey: key.sortKey,
                message: "Existing item does not exist."
            )
        }

        store[key.partitionKey] = updatedPartition
    }

    func deleteItem(
        forKey key: CompositePrimaryKey<some Any>,
        store: inout [String: [String: InMemoryDatabaseItem]]
    ) throws {
        store[key.partitionKey]?[key.sortKey] = nil
    }

    func deleteItem(
        itemMetadata: DatabaseItemMetadataWithKey<some Any>,
        store: inout [String: [String: InMemoryDatabaseItem]]
    ) throws {
        let partition = store[itemMetadata.compositePrimaryKey.partitionKey]

        // if there is already a partition
        var updatedPartition: [String: InMemoryDatabaseItem]
        if let partition {
            updatedPartition = partition

            // if the row already exists
            if let actuallyExistingItem = partition[itemMetadata.compositePrimaryKey.sortKey] {
                if itemMetadata.rowStatus.rowVersion != actuallyExistingItem.rowStatus.rowVersion
                    || itemMetadata.createDate.iso8601 != actuallyExistingItem.createDate.iso8601
                {
                    throw DynamoDBTableError.conditionalCheckFailed(
                        partitionKey: itemMetadata.compositePrimaryKey.partitionKey,
                        sortKey: itemMetadata.compositePrimaryKey.sortKey,
                        message: "Trying to delete incorrect version."
                    )
                }
            } else {
                throw DynamoDBTableError.conditionalCheckFailed(
                    partitionKey: itemMetadata.compositePrimaryKey.partitionKey,
                    sortKey: itemMetadata.compositePrimaryKey.sortKey,
                    message: "Existing item does not exist."
                )
            }

            updatedPartition[itemMetadata.compositePrimaryKey.sortKey] = nil
        } else {
            throw DynamoDBTableError.conditionalCheckFailed(
                partitionKey: itemMetadata.compositePrimaryKey.partitionKey,
                sortKey: itemMetadata.compositePrimaryKey.sortKey,
                message: "Existing item does not exist."
            )
        }

        store[itemMetadata.compositePrimaryKey.partitionKey] = updatedPartition
    }

    func bulkWrite<AttributesType>(
        _ entries: [InMemoryWriteEntry<AttributesType>],
        constraints: [InMemoryTransactionConstraintEntry<AttributesType>],
        store: inout [String: [String: InMemoryDatabaseItem]],
        isTransaction: Bool
    ) throws {
        let entryCount = entries.count + constraints.count
        if isTransaction, entryCount > AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement {
            throw DynamoDBTableError.itemCollectionSizeLimitExceeded(
                attemptedSize: entryCount,
                maximumSize: AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement
            )
        }

        let savedStore = store

        if let error = self.handleConstraints(constraints: constraints, store: &store, isTransaction: isTransaction) {
            throw error
        }

        if let error = self.handleEntries(entries: entries, store: &store, isTransaction: isTransaction) {
            if isTransaction {
                // restore the state prior to the transaction
                store = savedStore
            }

            throw error
        }
    }

    func polymorphicBulkWrite(
        _ entryTransformResults: [Swift.Result<InMemoryPolymorphicWriteEntryTransform, DynamoDBTableError>],
        constraintTransformResults: [Swift.Result<
            InMemoryPolymorphicTransactionConstraintTransform, DynamoDBTableError
        >],
        store: inout [String: [String: InMemoryDatabaseItem]],
        context: StandardPolymorphicWriteEntryContext<
            InMemoryPolymorphicWriteEntryTransform,
            InMemoryPolymorphicTransactionConstraintTransform
        >,
        isTransaction: Bool
    ) throws {
        let entryCount = entryTransformResults.count + constraintTransformResults.count

        if isTransaction, entryCount > AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement {
            throw DynamoDBTableError.itemCollectionSizeLimitExceeded(
                attemptedSize: entryCount,
                maximumSize: AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement
            )
        }

        let savedStore = store

        if let error = self.handleConstraints(
            transformResults: constraintTransformResults,
            isTransaction: isTransaction,
            store: &store,
            context: context
        ) {
            throw error
        }

        if let error = self.handlePolymorphicEntries(
            entryTransformResults: entryTransformResults,
            isTransaction: isTransaction,
            store: &store,
            context: context
        ) {
            if isTransaction {
                // restore the state prior to the transaction
                store = savedStore
            }

            throw error
        }
    }

    func handleConstraints<AttributesType>(
        constraints: [InMemoryTransactionConstraintEntry<AttributesType>],
        store: inout [String: [String: InMemoryDatabaseItem]],
        isTransaction _: Bool
    )
        -> DynamoDBTableError?
    {
        let errors = constraints.compactMap { entry -> DynamoDBTableError? in
            let existingItem: InMemoryDatabaseItemWithKey<AttributesType> =
                switch entry {
                case let .required(existing: existing):
                    existing
                }

            let compositePrimaryKey = existingItem.compositePrimaryKey

            guard let partition = store[compositePrimaryKey.partitionKey],
                let item = partition[compositePrimaryKey.sortKey],
                item.rowStatus.rowVersion == existingItem.rowStatus.rowVersion
            else {
                return DynamoDBTableError.conditionalCheckFailed(
                    partitionKey: compositePrimaryKey.partitionKey,
                    sortKey: compositePrimaryKey.sortKey,
                    message: "Item doesn't exist or doesn't have correct version"
                )
            }

            return nil
        }

        if !errors.isEmpty {
            return DynamoDBTableError.transactionCanceled(reasons: errors)
        }

        return nil
    }

    func handleConstraints(
        transformResults: [Swift.Result<InMemoryPolymorphicTransactionConstraintTransform, DynamoDBTableError>],
        isTransaction _: Bool,
        store: inout [String: [String: InMemoryDatabaseItem]],
        context _: StandardPolymorphicWriteEntryContext<
            InMemoryPolymorphicWriteEntryTransform,
            InMemoryPolymorphicTransactionConstraintTransform
        >
    )
        -> DynamoDBTableError?
    {
        let errors = transformResults.compactMap { transformResult -> DynamoDBTableError? in
            let transform: InMemoryPolymorphicTransactionConstraintTransform
            switch transformResult {
            case let .success(theTransform):
                transform = theTransform
            case let .failure(error):
                return error
            }

            guard let partition = store[transform.partitionKey],
                let item = partition[transform.sortKey],
                item.rowStatus.rowVersion == transform.rowVersion
            else {
                return DynamoDBTableError.conditionalCheckFailed(
                    partitionKey: transform.partitionKey,
                    sortKey: transform.sortKey,
                    message: "Item doesn't exist or doesn't have correct version"
                )
            }

            return nil
        }

        if !errors.isEmpty {
            return DynamoDBTableError.transactionCanceled(reasons: errors)
        }

        return nil
    }

    func handleEntries(
        entries: [InMemoryWriteEntry<some Any>],
        store: inout [String: [String: InMemoryDatabaseItem]],
        isTransaction: Bool
    )
        -> DynamoDBTableError?
    {
        let writeErrors = entries.compactMap { entry -> DynamoDBTableError? in
            do {
                switch entry {
                case let .update(new: new, existing: existing):
                    try self.updateItem(
                        newItem: new.inMemoryDatabaseItem,
                        existingItemMetadata: existing.asMetadataWithKey(),
                        store: &store
                    )
                case let .insert(new: new):
                    try self.insertItem(new, store: &store)
                case let .deleteAtKey(key: key):
                    try self.deleteItem(forKey: key, store: &store)
                case let .deleteItem(existing: existing):
                    try self.deleteItem(itemMetadata: existing.asMetadataWithKey(), store: &store)
                }
            } catch {
                if let typedError = error as? DynamoDBTableError {
                    if case let .conditionalCheckFailed(partitionKey, sortKey, message) = typedError, isTransaction {
                        if message == itemAlreadyExistsMessage {
                            return .duplicateItem(partitionKey: partitionKey, sortKey: sortKey, message: message)
                        } else {
                            return .conditionalCheckFailed(
                                partitionKey: partitionKey,
                                sortKey: sortKey,
                                message: message
                            )
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
                return DynamoDBTableError.batchFailures(errors: writeErrors)
            }
        }

        return nil
    }

    func handlePolymorphicEntries(
        entryTransformResults: [Swift.Result<InMemoryPolymorphicWriteEntryTransform, DynamoDBTableError>],
        isTransaction: Bool,
        store: inout [String: [String: InMemoryDatabaseItem]],
        context _: StandardPolymorphicWriteEntryContext<
            InMemoryPolymorphicWriteEntryTransform,
            InMemoryPolymorphicTransactionConstraintTransform
        >
    )
        -> DynamoDBTableError?
    {
        let writeErrors = entryTransformResults.compactMap { entryTransformResult -> DynamoDBTableError? in
            let transform: InMemoryPolymorphicWriteEntryTransform
            switch entryTransformResult {
            case let .success(theTransform):
                transform = theTransform
            case let .failure(error):
                return error
            }

            do {
                try transform.operation(&store)
            } catch {
                if let typedError = error as? DynamoDBTableError {
                    if case let .conditionalCheckFailed(partitionKey, sortKey, message) = typedError, isTransaction {
                        if message == itemAlreadyExistsMessage {
                            return .duplicateItem(partitionKey: partitionKey, sortKey: sortKey, message: message)
                        } else {
                            return .conditionalCheckFailed(
                                partitionKey: partitionKey,
                                sortKey: sortKey,
                                message: message
                            )
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
                return DynamoDBTableError.batchFailures(errors: writeErrors)
            }
        }

        return nil
    }

    func convertToQueryableType<ReturnedType: PolymorphicOperationReturnType>(
        input: InMemoryDatabaseItem
    ) throws -> ReturnedType {
        let attributeValue = DynamoDBClientTypes.AttributeValue.m(input.item)

        let decodedItem: ReturnTypeDecodable<ReturnedType> = try DynamoDBDecoder().decode(attributeValue)

        return decodedItem.decodedValue
    }
}
