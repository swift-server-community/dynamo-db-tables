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
//  DynamoDBCompositePrimaryKeyTable+retryingUpdateItemWithHistoricalRowTests.swift
//  DynamoDBTablesTests
//

import Foundation
import Testing

@testable import DynamoDBTables

private typealias DatabaseRowType =
    TypedTTLDatabaseItem<StandardPrimaryKeyAttributes, RowWithItemVersion<TestTypeA>, StandardTimeToLiveAttributes>

struct RetryingUpdateItemWithHistoricalRowTests {
    static let dKey = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
    static let dPayload = TestTypeA(firstly: "firstly", secondly: "secondly")

    private func testNewItemProvider() -> DatabaseRowType {
        StandardTypedDatabaseItem.newItem(
            withKey: Self.dKey,
            andValue: RowWithItemVersion.newItem(withValue: Self.dPayload)
        )
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

    enum TestError: Error {
        case everythingIsWrong
    }

    @Test
    func retryingUpdateItemWithHistoricalRow() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let databaseItem = self.testNewItemProvider()
        try await table.insertItem(databaseItem)

        let updated = try await table.retryingUpdateItemWithHistoricalRow(
            forKey: Self.dKey,
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
    func retryingUpdateItemWithHistoricalRowWithAsyncProvider() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let databaseItem = self.testNewItemProvider()
        try await table.insertItem(databaseItem)

        let asyncConditionalUpdatePrimaryItemProvider = self.getConditionalUpdatePrimaryItemProviderAsync()
        let updated = try await table.retryingUpdateItemWithHistoricalRow(
            forKey: Self.dKey,
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
    func retryingUpdateItemWithHistoricalRowAcceptableConcurrency() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 5,
            simulateOnInsertItem: false
        )

        let databaseItem = self.testNewItemProvider()
        try await table.insertItem(databaseItem)

        let updated = try await table.retryingUpdateItemWithHistoricalRow(
            forKey: Self.dKey,
            primaryItemProvider: self.conditionalUpdatePrimaryItemProvider,
            historicalItemProvider: self.conditionalUpdateHistoricalItemProvider
        )

        let inserted: DatabaseRowType = try await (table.getItem(forKey: databaseItem.compositePrimaryKey))!
        #expect(inserted.rowValue.rowValue.firstly == "firstly_6")
        #expect(inserted.rowValue.rowValue.secondly == "secondly_6")
        // the row version has been updated by the SimulateConcurrencyDynamoDBCompositePrimaryKeyTable an
        // additional 5 fives, item updated by retryingUpdateItemWithHistoricalRow
        // (which increments itemVersion) only once
        #expect(inserted.rowStatus.rowVersion == 7)
        #expect(inserted.rowValue.itemVersion == 2)

        #expect(updated.rowValue.rowValue.firstly == inserted.rowValue.rowValue.firstly)
        #expect(updated.rowValue.rowValue.secondly == inserted.rowValue.rowValue.secondly)
        #expect(updated.rowStatus.rowVersion == inserted.rowStatus.rowVersion)
        #expect(updated.rowValue.itemVersion == inserted.rowValue.itemVersion)
    }

    @Test
    func retryingUpdateItemWithHistoricalRowAcceptableConcurrencyWithAsyncProvider() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 5,
            simulateOnInsertItem: false
        )

        let databaseItem = self.testNewItemProvider()
        try await table.insertItem(databaseItem)

        let asyncConditionalUpdatePrimaryItemProvider = self.getConditionalUpdatePrimaryItemProviderAsync()
        let updated = try await table.retryingUpdateItemWithHistoricalRow(
            forKey: Self.dKey,
            primaryItemProvider: asyncConditionalUpdatePrimaryItemProvider,
            historicalItemProvider: self.conditionalUpdateHistoricalItemProvider
        )

        let inserted: DatabaseRowType = try await (table.getItem(forKey: databaseItem.compositePrimaryKey))!
        #expect(inserted.rowValue.rowValue.firstly == "firstly_6")
        #expect(inserted.rowValue.rowValue.secondly == "secondly_6")
        // the row version has been updated by the SimulateConcurrencyDynamoDBCompositePrimaryKeyTable an
        // additional 5 fives, item updated by retryingUpdateItemWithHistoricalRow
        // (which increments itemVersion) only once
        #expect(inserted.rowStatus.rowVersion == 7)
        #expect(inserted.rowValue.itemVersion == 2)

        #expect(updated.rowValue.rowValue.firstly == inserted.rowValue.rowValue.firstly)
        #expect(updated.rowValue.rowValue.secondly == inserted.rowValue.rowValue.secondly)
        #expect(updated.rowStatus.rowVersion == inserted.rowStatus.rowVersion)
        #expect(updated.rowValue.itemVersion == inserted.rowValue.itemVersion)
    }

    @Test
    func retryingUpdateItemWithHistoricalRowUnacceptableConcurrency() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 50,
            simulateOnInsertItem: false
        )

        let databaseItem = self.testNewItemProvider()
        try await table.insertItem(databaseItem)

        do {
            _ = try await table.retryingUpdateItemWithHistoricalRow(
                forKey: Self.dKey,
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
        // confirm row has not been updated by retryingUpdateItemWithHistoricalRow
        #expect(inserted.rowValue.rowValue.firstly == "firstly")
        #expect(inserted.rowValue.rowValue.secondly == "secondly")
        #expect(inserted.rowStatus.rowVersion == 11)
        #expect(inserted.rowValue.itemVersion == 1)
    }

    @Test
    func retryingUpdateItemWithHistoricalRowUnacceptableConcurrencyWithAsyncProvider() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 50,
            simulateOnInsertItem: false
        )

        let databaseItem = self.testNewItemProvider()
        try await table.insertItem(databaseItem)

        let asyncConditionalUpdatePrimaryItemProvider = self.getConditionalUpdatePrimaryItemProviderAsync()
        do {
            _ = try await table.retryingUpdateItemWithHistoricalRow(
                forKey: Self.dKey,
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
        // confirm row has not been updated by retryingUpdateItemWithHistoricalRow
        #expect(inserted.rowValue.rowValue.firstly == "firstly")
        #expect(inserted.rowValue.rowValue.secondly == "secondly")
        #expect(inserted.rowStatus.rowVersion == 11)
        #expect(inserted.rowValue.itemVersion == 1)
    }

    @Test
    func retryingUpdateItemWithHistoricalRowPrimaryItemProviderError() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 5,
            simulateOnInsertItem: false
        )

        let databaseItem = self.testNewItemProvider()
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
            _ = try await table.retryingUpdateItemWithHistoricalRow(
                forKey: Self.dKey,
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
        // confirm row has not been updated by retryingUpdateItemWithHistoricalRow
        #expect(inserted.rowValue.rowValue.firstly == "firstly")
        #expect(inserted.rowValue.rowValue.secondly == "secondly")
        #expect(inserted.rowStatus.rowVersion == 6)
        #expect(inserted.rowValue.itemVersion == 1)
    }

    @Test
    func retryingUpdateItemWithHistoricalRowPrimaryItemProviderErrorWithAsyncProvider() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 5,
            simulateOnInsertItem: false
        )

        let databaseItem = self.testNewItemProvider()
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
            _ = try await table.retryingUpdateItemWithHistoricalRow(
                forKey: Self.dKey,
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
        // confirm row has not been updated by retryingUpdateItemWithHistoricalRow
        #expect(inserted.rowValue.rowValue.firstly == "firstly")
        #expect(inserted.rowValue.rowValue.secondly == "secondly")
        #expect(inserted.rowStatus.rowVersion == 6)
        #expect(inserted.rowValue.itemVersion == 1)
    }
}
