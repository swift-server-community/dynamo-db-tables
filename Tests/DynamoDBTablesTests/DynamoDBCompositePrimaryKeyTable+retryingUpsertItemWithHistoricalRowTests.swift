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
//  DynamoDBCompositePrimaryKeyTable+retryingUpsertItemWithHistoricalRowTests.swift
//  DynamoDBTablesTests
//

import Foundation
import Testing

@testable import DynamoDBTables

private typealias DatabaseRowType =
    TypedTTLDatabaseItem<StandardPrimaryKeyAttributes, RowWithItemVersion<TestTypeA>, StandardTimeToLiveAttributes>

struct RetryingUpsertItemWithHistoricalRowTests {
    static let dKey = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
    static let dPayload = TestTypeA(firstly: "firstly", secondly: "secondly")

    private func testNewItemProvider() -> DatabaseRowType {
        StandardTypedDatabaseItem.newItem(
            withKey: Self.dKey,
            andValue: RowWithItemVersion.newItem(withValue: Self.dPayload)
        )
    }

    private func testUpdatedItemProvider(existingItem: DatabaseRowType) -> DatabaseRowType {
        let newItemRowValue = existingItem.rowValue.createUpdatedItem(
            withVersion: existingItem.rowValue.itemVersion + 1,
            withValue: Self.dPayload
        )
        return existingItem.createUpdatedItem(withValue: newItemRowValue)
    }

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
    func upsertItemSuccess() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        try await table.retryingUpsertItemWithHistoricalRow(
            forKey: Self.dKey,
            newItemProvider: self.testNewItemProvider,
            updatedItemProvider: self.testUpdatedItemProvider,
            historicalItemProvider: self.testHistoricalItemProvider
        )
        let inserted: DatabaseRowType = try await (table.getItem(forKey: Self.dKey))!
        #expect(inserted.compositePrimaryKey.partitionKey == Self.dKey.partitionKey)
        #expect(inserted.compositePrimaryKey.sortKey == Self.dKey.sortKey)
    }

    @Test
    func upsertItemSuccessAfterRetry() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 5
        )

        try await table.retryingUpsertItemWithHistoricalRow(
            forKey: Self.dKey,
            newItemProvider: self.testNewItemProvider,
            updatedItemProvider: self.testUpdatedItemProvider,
            historicalItemProvider: self.testHistoricalItemProvider
        )
        let inserted: DatabaseRowType = try await table.getItem(forKey: Self.dKey)!
        let newItem = self.testNewItemProvider()
        #expect(inserted.rowStatus.rowVersion > newItem.rowStatus.rowVersion)
    }

    @Test
    func upsertItemFailure() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 12
        )

        do {
            try await table.retryingUpsertItemWithHistoricalRow(
                forKey: Self.dKey,
                withRetries: 9,
                newItemProvider: self.testNewItemProvider,
                updatedItemProvider: self.testUpdatedItemProvider,
                historicalItemProvider: self.testHistoricalItemProvider
            )

            Issue.record("Expected error not thrown.")
        } catch DynamoDBTableError.concurrencyError {
            // Success
        } catch {
            Issue.record("Unexpected exception: \(error)")
        }
    }
}
