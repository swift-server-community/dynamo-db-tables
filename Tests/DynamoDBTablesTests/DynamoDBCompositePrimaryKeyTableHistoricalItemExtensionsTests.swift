//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/main/Tests/SmokeDynamoDBTests/DynamoDBCompositePrimaryKeyTableHistoricalItemExtensionsTests.swift
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
//  DynamoDBCompositePrimaryKeyTableHistoricalItemExtensionsTests.swift
//      DynamoDB Historical Client Extension Tests
//  DynamoDBTablesTests
//

import Foundation
import Testing

@testable import DynamoDBTables

private typealias DatabaseRowType =
    TypedTTLDatabaseItem<StandardPrimaryKeyAttributes, RowWithItemVersion<TestTypeA>, StandardTimeToLiveAttributes>

/// For these tests, a primary item Provider should always return a default value for nil arguments. The Provider Provider requires a non-nil default in order to initialize a Provider.
private func primaryItemProviderProvider(_ defaultItem: DatabaseRowType) -> (DatabaseRowType?) -> DatabaseRowType {
    func primaryItemProvider(_ item: DatabaseRowType?) -> DatabaseRowType {
        guard let item else {
            return defaultItem
        }

        let newItemRowValue = item.rowValue.createUpdatedItem(
            withVersion: item.rowValue.itemVersion + 1,
            withValue: defaultItem.rowValue.rowValue
        )
        return item.createUpdatedItem(withValue: newItemRowValue)
    }

    return primaryItemProvider
}

struct CompositePrimaryKeyDynamoDBHistoricalClientTests {
    static let dKey = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
    static let dPayload = TestTypeA(firstly: "firstly", secondly: "secondly")

    private let testPrimaryItemProvider = primaryItemProviderProvider(
        StandardTypedDatabaseItem.newItem(
            withKey: dKey,
            andValue: RowWithItemVersion.newItem(withValue: dPayload)
        )
    )

    private func testHistoricalItemProvider(_ item: DatabaseRowType) -> DatabaseRowType {
        DatabaseRowType.newItem(
            withKey: StandardCompositePrimaryKey(
                partitionKey: "historical.\(item.compositePrimaryKey.partitionKey)",
                sortKey: "v0000\(item.rowValue.itemVersion).\(item.compositePrimaryKey.sortKey)"
            ),
            andValue: item.rowValue
        )
    }

    @Test
    func insertItemSuccess() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = self.testHistoricalItemProvider(databaseItem)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        try await table.insertItemWithHistoricalRow(primaryItem: databaseItem, historicalItem: historicalItem)
        let inserted: DatabaseRowType = try await table.getItem(forKey: databaseItem.compositePrimaryKey)!
        #expect(inserted.compositePrimaryKey.partitionKey == databaseItem.compositePrimaryKey.partitionKey)
        #expect(inserted.compositePrimaryKey.sortKey == databaseItem.compositePrimaryKey.sortKey)
    }

    @Test
    func insertItemFailure() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = self.testHistoricalItemProvider(databaseItem)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        try await table.insertItemWithHistoricalRow(primaryItem: databaseItem, historicalItem: historicalItem)

        do {
            // Second insert will fail.
            try await table.insertItemWithHistoricalRow(primaryItem: databaseItem, historicalItem: historicalItem)
        } catch DynamoDBTableError.conditionalCheckFailed {
            // Success
        } catch {
            Issue.record("Unexpected exception")
        }
    }

    @Test
    func updateItemSuccess() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = self.testHistoricalItemProvider(databaseItem)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        try await table.insertItemWithHistoricalRow(primaryItem: databaseItem, historicalItem: historicalItem)

        let updatedPayload = versionedPayload.createUpdatedItem(withValue: versionedPayload.rowValue)
        let updatedItem = databaseItem.createUpdatedItem(withValue: updatedPayload)
        try await table.updateItemWithHistoricalRow(
            primaryItem: updatedItem,
            existingItem: databaseItem,
            historicalItem: self.testHistoricalItemProvider(updatedItem)
        )

        let inserted: DatabaseRowType = try await table.getItem(forKey: key)!
        #expect(inserted.compositePrimaryKey.partitionKey == databaseItem.compositePrimaryKey.partitionKey)
        #expect(inserted.compositePrimaryKey.sortKey == databaseItem.compositePrimaryKey.sortKey)
    }

    @Test
    func updateItemFailure() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = self.testHistoricalItemProvider(databaseItem)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        try await table.insertItemWithHistoricalRow(primaryItem: databaseItem, historicalItem: historicalItem)

        let updatedPayload = versionedPayload.createUpdatedItem(withValue: versionedPayload.rowValue)
        let updatedItem = databaseItem.createUpdatedItem(withValue: updatedPayload)
        try await table.updateItemWithHistoricalRow(
            primaryItem: updatedItem,
            existingItem: databaseItem,
            historicalItem: self.testHistoricalItemProvider(updatedItem)
        )

        do {
            // Second update will fail.
            try await table.updateItemWithHistoricalRow(
                primaryItem: databaseItem.createUpdatedItem(withValue: versionedPayload),
                existingItem: databaseItem,
                historicalItem: historicalItem
            )
        } catch DynamoDBTableError.conditionalCheckFailed {
            // Success
        } catch {
            Issue.record("Unexpected exception")
        }
    }

    @Test
    func clobberItemSuccess() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let databaseItem = self.testPrimaryItemProvider(nil)

        try await table.clobberItemWithHistoricalRow(
            primaryItemProvider: self.testPrimaryItemProvider,
            historicalItemProvider: self.testHistoricalItemProvider
        )
        let inserted: DatabaseRowType = try await (table.getItem(forKey: databaseItem.compositePrimaryKey))!
        #expect(inserted.compositePrimaryKey.partitionKey == databaseItem.compositePrimaryKey.partitionKey)
        #expect(inserted.compositePrimaryKey.sortKey == databaseItem.compositePrimaryKey.sortKey)
    }

    @Test
    func clobberItemSuccessAfterRetry() async throws {
        let databaseItem = self.testPrimaryItemProvider(nil)

        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 5
        )

        try await table.clobberItemWithHistoricalRow(
            primaryItemProvider: self.testPrimaryItemProvider,
            historicalItemProvider: self.testHistoricalItemProvider
        )
        let inserted: DatabaseRowType = try await table.getItem(forKey: databaseItem.compositePrimaryKey)!
        #expect(inserted.rowStatus.rowVersion > databaseItem.rowStatus.rowVersion)
    }

    @Test
    func clobberItemFailure() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 12
        )

        do {
            try await table.clobberItemWithHistoricalRow(
                primaryItemProvider: self.testPrimaryItemProvider,
                historicalItemProvider: self.testHistoricalItemProvider,
                withRetries: 9
            )

            Issue.record("Expected error not thrown.")
        } catch DynamoDBTableError.concurrencyError {
            // Success
        } catch {
            Issue.record("Unexpected exception: \(error)")
        }
    }

    private func conditionalUpdatePrimaryItemProvider(existingItem: DatabaseRowType) throws -> DatabaseRowType {
        let rowVersion = existingItem.rowStatus.rowVersion
        let dPayload = TestTypeA(firstly: "firstly_\(rowVersion)", secondly: "secondly_\(rowVersion)")

        return try existingItem.createUpdatedRowWithItemVersion(
            withValue: dPayload,
            conditionalStatusVersion: nil
        )
    }

    private func getConditionalUpdatePrimaryItemProviderAsync() -> ((DatabaseRowType) async throws -> DatabaseRowType) {
        func provider(existingItem: DatabaseRowType) async throws -> DatabaseRowType {
            let rowVersion = existingItem.rowStatus.rowVersion
            let dPayload = TestTypeA(firstly: "firstly_\(rowVersion)", secondly: "secondly_\(rowVersion)")

            return try existingItem.createUpdatedRowWithItemVersion(
                withValue: dPayload,
                conditionalStatusVersion: nil
            )
        }

        return provider
    }

    private let historicalCompositePrimaryKey = StandardCompositePrimaryKey(
        partitionKey: "historicalPartitionKey",
        sortKey: "historicalSortKey"
    )
    private func conditionalUpdateHistoricalItemProvider(updatedItem: DatabaseRowType) -> DatabaseRowType {
        // create an item for the history partition
        TypedTTLDatabaseItem.newItem(
            withKey: self.historicalCompositePrimaryKey,
            andValue: updatedItem.rowValue
        )
    }

    @Test
    func conditionallyUpdateItemWithHistoricalRow() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let databaseItem = self.testPrimaryItemProvider(nil)
        try await table.insertItem(databaseItem)

        let updated = try await table.conditionallyUpdateItemWithHistoricalRow(
            compositePrimaryKey: Self.dKey,
            primaryItemProvider: self.conditionalUpdatePrimaryItemProvider,
            historicalItemProvider: self.conditionalUpdateHistoricalItemProvider
        )

        let inserted: DatabaseRowType = try await (table.getItem(forKey: databaseItem.compositePrimaryKey))!
        #expect(inserted.rowValue.rowValue.firstly == "firstly_1")
        #expect(inserted.rowValue.rowValue.secondly == "secondly_1")
        #expect(inserted.rowStatus.rowVersion == 2)
        #expect(inserted.rowValue.itemVersion == 2)

        #expect(updated.rowValue.rowValue.firstly == inserted.rowValue.rowValue.firstly)
        #expect(updated.rowValue.rowValue.secondly == inserted.rowValue.rowValue.secondly)
        #expect(updated.rowStatus.rowVersion == inserted.rowStatus.rowVersion)
        #expect(updated.rowValue.itemVersion == inserted.rowValue.itemVersion)

        let historicalInserted: DatabaseRowType = try await (table.getItem(forKey: self.historicalCompositePrimaryKey))!
        #expect(historicalInserted.rowValue.rowValue.firstly == "firstly_1")
        #expect(historicalInserted.rowValue.rowValue.secondly == "secondly_1")
        #expect(historicalInserted.rowStatus.rowVersion == 1)
        #expect(historicalInserted.rowValue.itemVersion == 2)
    }

    @Test
    func conditionallyUpdateItemWithHistoricalRowWithAsyncProvider() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let databaseItem = self.testPrimaryItemProvider(nil)
        try await table.insertItem(databaseItem)

        let asyncConditionalUpdatePrimaryItemProvider = self.getConditionalUpdatePrimaryItemProviderAsync()
        let updated = try await table.conditionallyUpdateItemWithHistoricalRow(
            compositePrimaryKey: Self.dKey,
            primaryItemProvider: asyncConditionalUpdatePrimaryItemProvider,
            historicalItemProvider: self.conditionalUpdateHistoricalItemProvider
        )

        let inserted: DatabaseRowType = try await (table.getItem(forKey: databaseItem.compositePrimaryKey))!
        #expect(inserted.rowValue.rowValue.firstly == "firstly_1")
        #expect(inserted.rowValue.rowValue.secondly == "secondly_1")
        #expect(inserted.rowStatus.rowVersion == 2)
        #expect(inserted.rowValue.itemVersion == 2)

        #expect(updated.rowValue.rowValue.firstly == inserted.rowValue.rowValue.firstly)
        #expect(updated.rowValue.rowValue.secondly == inserted.rowValue.rowValue.secondly)
        #expect(updated.rowStatus.rowVersion == inserted.rowStatus.rowVersion)
        #expect(updated.rowValue.itemVersion == inserted.rowValue.itemVersion)

        let historicalInserted: DatabaseRowType = try await (table.getItem(forKey: self.historicalCompositePrimaryKey))!
        #expect(historicalInserted.rowValue.rowValue.firstly == "firstly_1")
        #expect(historicalInserted.rowValue.rowValue.secondly == "secondly_1")
        #expect(historicalInserted.rowStatus.rowVersion == 1)
        #expect(historicalInserted.rowValue.itemVersion == 2)
    }

    @Test
    func conditionallyUpdateItemWithHistoricalRowAcceptableConcurrency() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 5,
            simulateOnInsertItem: false
        )

        let databaseItem = self.testPrimaryItemProvider(nil)
        try await table.insertItem(databaseItem)

        let updated = try await table.conditionallyUpdateItemWithHistoricalRow(
            compositePrimaryKey: Self.dKey,
            primaryItemProvider: self.conditionalUpdatePrimaryItemProvider,
            historicalItemProvider: self.conditionalUpdateHistoricalItemProvider
        )

        let inserted: DatabaseRowType = try await (table.getItem(forKey: databaseItem.compositePrimaryKey))!
        #expect(inserted.rowValue.rowValue.firstly == "firstly_6")
        #expect(inserted.rowValue.rowValue.secondly == "secondly_6")
        // the row version has been updated by the SimulateConcurrencyDynamoDBCompositePrimaryKeyTable an
        // additional 5 fives, item updated by conditionallyUpdateItemWithHistoricalRow
        // (which increments itemVersion) only once
        #expect(inserted.rowStatus.rowVersion == 7)
        #expect(inserted.rowValue.itemVersion == 2)

        #expect(updated.rowValue.rowValue.firstly == inserted.rowValue.rowValue.firstly)
        #expect(updated.rowValue.rowValue.secondly == inserted.rowValue.rowValue.secondly)
        #expect(updated.rowStatus.rowVersion == inserted.rowStatus.rowVersion)
        #expect(updated.rowValue.itemVersion == inserted.rowValue.itemVersion)
    }

    @Test
    func conditionallyUpdateItemWithHistoricalRowAcceptableConcurrencyWithAsyncProvider() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 5,
            simulateOnInsertItem: false
        )

        let databaseItem = self.testPrimaryItemProvider(nil)
        try await table.insertItem(databaseItem)

        let asyncConditionalUpdatePrimaryItemProvider = self.getConditionalUpdatePrimaryItemProviderAsync()
        let updated = try await table.conditionallyUpdateItemWithHistoricalRow(
            compositePrimaryKey: Self.dKey,
            primaryItemProvider: asyncConditionalUpdatePrimaryItemProvider,
            historicalItemProvider: self.conditionalUpdateHistoricalItemProvider
        )

        let inserted: DatabaseRowType = try await (table.getItem(forKey: databaseItem.compositePrimaryKey))!
        #expect(inserted.rowValue.rowValue.firstly == "firstly_6")
        #expect(inserted.rowValue.rowValue.secondly == "secondly_6")
        // the row version has been updated by the SimulateConcurrencyDynamoDBCompositePrimaryKeyTable an
        // additional 5 fives, item updated by conditionallyUpdateItemWithHistoricalRow
        // (which increments itemVersion) only once
        #expect(inserted.rowStatus.rowVersion == 7)
        #expect(inserted.rowValue.itemVersion == 2)

        #expect(updated.rowValue.rowValue.firstly == inserted.rowValue.rowValue.firstly)
        #expect(updated.rowValue.rowValue.secondly == inserted.rowValue.rowValue.secondly)
        #expect(updated.rowStatus.rowVersion == inserted.rowStatus.rowVersion)
        #expect(updated.rowValue.itemVersion == inserted.rowValue.itemVersion)
    }

    @Test
    func conditionallyUpdateItemWithHistoricalRowUnacceptableConcurrency() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 50,
            simulateOnInsertItem: false
        )

        let databaseItem = self.testPrimaryItemProvider(nil)
        try await table.insertItem(databaseItem)

        do {
            _ = try await table.conditionallyUpdateItemWithHistoricalRow(
                compositePrimaryKey: Self.dKey,
                primaryItemProvider: self.conditionalUpdatePrimaryItemProvider,
                historicalItemProvider: self.conditionalUpdateHistoricalItemProvider
            )

            Issue.record("Expected error not thrown.")
        } catch DynamoDBTableError.concurrencyError {
            // Success
        } catch {
            Issue.record("Unexpected exception: \(error)")
        }

        let inserted: DatabaseRowType = try await (table.getItem(forKey: databaseItem.compositePrimaryKey))!
        // confirm row has not been updated by conditionallyUpdateItemWithHistoricalRow
        #expect(inserted.rowValue.rowValue.firstly == "firstly")
        #expect(inserted.rowValue.rowValue.secondly == "secondly")
        #expect(inserted.rowStatus.rowVersion == 11)
        #expect(inserted.rowValue.itemVersion == 1)
    }

    @Test
    func conditionallyUpdateItemWithHistoricalRowUnacceptableConcurrencyWithAsyncProvider() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 50,
            simulateOnInsertItem: false
        )

        let databaseItem = self.testPrimaryItemProvider(nil)
        try await table.insertItem(databaseItem)

        let asyncConditionalUpdatePrimaryItemProvider = self.getConditionalUpdatePrimaryItemProviderAsync()
        do {
            _ = try await table.conditionallyUpdateItemWithHistoricalRow(
                compositePrimaryKey: Self.dKey,
                primaryItemProvider: asyncConditionalUpdatePrimaryItemProvider,
                historicalItemProvider: self.conditionalUpdateHistoricalItemProvider
            )

            Issue.record("Expected error not thrown.")
        } catch DynamoDBTableError.concurrencyError {
            // Success
        } catch {
            Issue.record("Unexpected exception: \(error)")
        }

        let inserted: DatabaseRowType = try await (table.getItem(forKey: databaseItem.compositePrimaryKey))!
        // confirm row has not been updated by conditionallyUpdateItemWithHistoricalRow
        #expect(inserted.rowValue.rowValue.firstly == "firstly")
        #expect(inserted.rowValue.rowValue.secondly == "secondly")
        #expect(inserted.rowStatus.rowVersion == 11)
        #expect(inserted.rowValue.itemVersion == 1)
    }

    enum TestError: Error {
        case everythingIsWrong
    }

    @Test
    func conditionallyUpdateItemWithHistoricalRowPrimaryItemProviderError() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 5,
            simulateOnInsertItem: false
        )

        let databaseItem = self.testPrimaryItemProvider(nil)
        try await table.insertItem(databaseItem)

        var providerCount = 0
        let primaryItemProvider: (DatabaseRowType) throws -> DatabaseRowType = { existingItem in
            guard providerCount < 5 else {
                throw TestError.everythingIsWrong
            }
            providerCount += 1

            let rowVersion = existingItem.rowStatus.rowVersion
            let dPayload = TestTypeA(firstly: "firstly_\(rowVersion)", secondly: "secondly_\(rowVersion)")

            return try existingItem.createUpdatedRowWithItemVersion(
                withValue: dPayload,
                conditionalStatusVersion: nil
            )
        }

        do {
            _ = try await table.conditionallyUpdateItemWithHistoricalRow(
                compositePrimaryKey: Self.dKey,
                primaryItemProvider: primaryItemProvider,
                historicalItemProvider: self.conditionalUpdateHistoricalItemProvider
            )

            Issue.record("Expected error not thrown.")
        } catch TestError.everythingIsWrong {
            // Success
        } catch {
            Issue.record("Unexpected exception: \(error)")
        }

        let inserted: DatabaseRowType = try await (table.getItem(forKey: databaseItem.compositePrimaryKey))!
        // confirm row has not been updated by conditionallyUpdateItemWithHistoricalRow
        #expect(inserted.rowValue.rowValue.firstly == "firstly")
        #expect(inserted.rowValue.rowValue.secondly == "secondly")
        #expect(inserted.rowStatus.rowVersion == 6)
        #expect(inserted.rowValue.itemVersion == 1)
    }

    @Test
    func conditionallyUpdateItemWithHistoricalRowPrimaryItemProviderErrorWithAsyncProvider() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 5,
            simulateOnInsertItem: false
        )

        let databaseItem = self.testPrimaryItemProvider(nil)
        try await table.insertItem(databaseItem)

        var providerCount = 0
        func primaryItemProvider(existingItem: DatabaseRowType) async throws -> DatabaseRowType {
            guard providerCount < 5 else {
                throw TestError.everythingIsWrong
            }
            providerCount += 1

            let rowVersion = existingItem.rowStatus.rowVersion
            let dPayload = TestTypeA(firstly: "firstly_\(rowVersion)", secondly: "secondly_\(rowVersion)")

            return try existingItem.createUpdatedRowWithItemVersion(
                withValue: dPayload,
                conditionalStatusVersion: nil
            )
        }

        do {
            _ = try await table.conditionallyUpdateItemWithHistoricalRow(
                compositePrimaryKey: Self.dKey,
                primaryItemProvider: primaryItemProvider,
                historicalItemProvider: self.conditionalUpdateHistoricalItemProvider
            )

            Issue.record("Expected error not thrown.")
        } catch TestError.everythingIsWrong {
            // Success
        } catch {
            Issue.record("Unexpected exception: \(error)")
        }

        let inserted: DatabaseRowType = try await (table.getItem(forKey: databaseItem.compositePrimaryKey))!
        // confirm row has not been updated by conditionallyUpdateItemWithHistoricalRow
        #expect(inserted.rowValue.rowValue.firstly == "firstly")
        #expect(inserted.rowValue.rowValue.secondly == "secondly")
        #expect(inserted.rowStatus.rowVersion == 6)
        #expect(inserted.rowValue.itemVersion == 1)
    }
}
