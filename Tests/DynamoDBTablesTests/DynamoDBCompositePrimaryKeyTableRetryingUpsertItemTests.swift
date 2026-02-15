//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// Copyright (c) 2026 the DynamoDBTables authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of DynamoDBTables authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

//
//  DynamoDBCompositePrimaryKeyTableRetryingUpsertItemTests.swift
//  DynamoDBTablesTests
//

import Testing

@testable import DynamoDBTables

struct DynamoDBCompositePrimaryKeyTableRetryingUpsertItemTests {
    static let dKey = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")

    typealias TestTypeADatabaseItem = StandardTypedDatabaseItem<TestTypeA>

    private func testNewItemProvider() -> TestTypeADatabaseItem {
        TestTypeADatabaseItem.newItem(
            withKey: Self.dKey,
            andValue: TestTypeA(firstly: "firstly", secondly: "secondly")
        )
    }

    private func testUpdatedItemProvider(existingItem: TestTypeADatabaseItem) -> TestTypeADatabaseItem {
        existingItem.createUpdatedItem(
            withValue: TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
        )
    }

    @Test
    func upsertItemInsertsWhenNotPresent() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let result = try await table.retryingUpsertItem(
            forKey: Self.dKey,
            newItemProvider: self.testNewItemProvider,
            updatedItemProvider: self.testUpdatedItemProvider
        )

        let inserted: TestTypeADatabaseItem = try await table.getItem(forKey: Self.dKey)!
        #expect(inserted.rowValue.firstly == "firstly")
        #expect(inserted.rowValue.secondly == "secondly")
        #expect(inserted.rowStatus.rowVersion == 1)
        #expect(result.rowValue.firstly == inserted.rowValue.firstly)
        #expect(result.rowValue.secondly == inserted.rowValue.secondly)
    }

    @Test
    func upsertItemUpdatesWhenPresent() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        // Pre-insert an item
        let existing = self.testNewItemProvider()
        try await table.insertItem(existing)

        let result = try await table.retryingUpsertItem(
            forKey: Self.dKey,
            newItemProvider: self.testNewItemProvider,
            updatedItemProvider: self.testUpdatedItemProvider
        )

        let updated: TestTypeADatabaseItem = try await table.getItem(forKey: Self.dKey)!
        #expect(updated.rowValue.firstly == "firstlyX2")
        #expect(updated.rowValue.secondly == "secondlyX2")
        #expect(updated.rowStatus.rowVersion == 2)
        #expect(result.rowValue.firstly == updated.rowValue.firstly)
        #expect(result.rowValue.secondly == updated.rowValue.secondly)
    }

    @Test
    func upsertItemSucceedsAfterRetry() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 5
        )

        let result = try await table.retryingUpsertItem(
            forKey: Self.dKey,
            newItemProvider: self.testNewItemProvider,
            updatedItemProvider: self.testUpdatedItemProvider
        )

        let inserted: TestTypeADatabaseItem = try await table.getItem(forKey: Self.dKey)!
        let newItem = self.testNewItemProvider()
        #expect(inserted.rowStatus.rowVersion > newItem.rowStatus.rowVersion)
        #expect(result.rowStatus.rowVersion == inserted.rowStatus.rowVersion)
    }

    @Test
    func upsertItemFailsAfterTooManyConcurrencyErrors() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 12
        )

        do {
            try await table.retryingUpsertItem(
                forKey: Self.dKey,
                withRetries: 9,
                newItemProvider: self.testNewItemProvider,
                updatedItemProvider: self.testUpdatedItemProvider
            )

            Issue.record("Expected error not thrown.")
        } catch DynamoDBTableError.concurrencyError {
            // Success
        } catch {
            Issue.record("Unexpected exception: \(error)")
        }
    }
}
