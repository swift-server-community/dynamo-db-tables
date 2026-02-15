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
//  DynamoDBCompositePrimaryKeyTable+retryingTransactWriteWithHistoricalRowsTests.swift
//  DynamoDBTablesTests
//

import Foundation
import Testing

@testable import DynamoDBTables

private typealias DatabaseRowType =
    TypedTTLDatabaseItem<StandardPrimaryKeyAttributes, RowWithItemVersion<TestTypeA>, StandardTimeToLiveAttributes>

struct RetryingTransactWriteWithHistoricalRowsTests {
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
    func retryingTransactWriteWithHistoricalRowsSuccess() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1", sortKey: "sortId1")
        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2", sortKey: "sortId2")

        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
        let payload2 = TestTypeA(firstly: "firstly2", secondly: "secondly2")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(
            withKey: key1,
            andValue: RowWithItemVersion.newItem(withValue: payload1)
        )
        let databaseItem2 = StandardTypedDatabaseItem.newItem(
            withKey: key2,
            andValue: RowWithItemVersion.newItem(withValue: payload2)
        )

        let entries = try await table.retryingTransactWriteWithHistoricalRows(
            forKeys: [key1, key2]
        ) {
            key,
            _ -> (
                entry: StandardWriteEntry<RowWithItemVersion<TestTypeA>>,
                historicalEntry: StandardWriteEntry<RowWithItemVersion<TestTypeA>>?
            )? in
            let item: DatabaseRowType
            if key == key1 {
                item = databaseItem1
            } else {
                item = databaseItem2
            }

            let historicalItem = self.testHistoricalItemProvider(item)
            return (entry: .insert(new: item), historicalEntry: .insert(new: historicalItem))
        }

        #expect(entries.count == 2)

        // Verify primary items were written
        let retrieved1: DatabaseRowType = try await table.getItem(forKey: key1)!
        #expect(retrieved1.rowValue.rowValue.firstly == "firstly1")

        let retrieved2: DatabaseRowType = try await table.getItem(forKey: key2)!
        #expect(retrieved2.rowValue.rowValue.firstly == "firstly2")

        // Verify historical items were written
        let historicalKey1 = StandardCompositePrimaryKey(
            partitionKey: "historical.partitionId1",
            sortKey: "v00001.sortId1"
        )
        let historicalItem1: DatabaseRowType = try await table.getItem(forKey: historicalKey1)!
        #expect(historicalItem1.rowValue.rowValue.firstly == "firstly1")

        let historicalKey2 = StandardCompositePrimaryKey(
            partitionKey: "historical.partitionId2",
            sortKey: "v00001.sortId2"
        )
        let historicalItem2: DatabaseRowType = try await table.getItem(forKey: historicalKey2)!
        #expect(historicalItem2.rowValue.rowValue.firstly == "firstly2")
    }

    @Test
    func retryingTransactWriteWithHistoricalRowsWithoutHistoricalEntries() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1", sortKey: "sortId1")

        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(
            withKey: key1,
            andValue: RowWithItemVersion.newItem(withValue: payload1)
        )

        let entries = try await table.retryingTransactWriteWithHistoricalRows(
            forKeys: [key1]
        ) {
            key,
            _ -> (
                entry: StandardWriteEntry<RowWithItemVersion<TestTypeA>>,
                historicalEntry: StandardWriteEntry<RowWithItemVersion<TestTypeA>>?
            )? in
            (entry: .insert(new: databaseItem1), historicalEntry: nil)
        }

        #expect(entries.count == 1)

        let retrieved: DatabaseRowType = try await table.getItem(forKey: key1)!
        #expect(retrieved.rowValue.rowValue.firstly == "firstly1")
    }

    @Test
    func retryingTransactWriteWithHistoricalRowsTransactionSizeLimitExceeded() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        // Create 51 keys — each with a historical entry = 102 items, exceeds 100
        let keys = (0..<51).map { i in
            StandardCompositePrimaryKey(partitionKey: "pk\(i)", sortKey: "sk\(i)")
        }

        do {
            try await table.retryingTransactWriteWithHistoricalRows(
                forKeys: keys
            ) {
                key,
                _ -> (
                    entry: StandardWriteEntry<RowWithItemVersion<TestTypeA>>,
                    historicalEntry: StandardWriteEntry<RowWithItemVersion<TestTypeA>>?
                )? in
                let payload = TestTypeA(firstly: "f", secondly: "s")
                let item = StandardTypedDatabaseItem.newItem(
                    withKey: key,
                    andValue: RowWithItemVersion.newItem(withValue: payload)
                )
                let historicalItem = self.testHistoricalItemProvider(item)
                return (entry: .insert(new: item), historicalEntry: .insert(new: historicalItem))
            }

            Issue.record("Expected error not thrown.")
        } catch DynamoDBTableError.itemCollectionSizeLimitExceeded(let attemptedSize, let maximumSize) {
            #expect(attemptedSize == 102)
            #expect(maximumSize == 100)
        } catch {
            Issue.record("Unexpected exception: \(error)")
        }
    }

    @Test
    func retryingTransactWriteWithHistoricalRowsAcceptableConcurrency() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1", sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(
            withKey: key1,
            andValue: RowWithItemVersion.newItem(withValue: payload1)
        )
        try await wrappedTable.insertItem(databaseItem1)

        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 5,
            simulateOnInsertItem: false
        )

        let entries = try await table.retryingTransactWriteWithHistoricalRows(
            forKeys: [key1]
        ) {
            key,
            existingItem -> (
                entry: StandardWriteEntry<RowWithItemVersion<TestTypeA>>,
                historicalEntry: StandardWriteEntry<RowWithItemVersion<TestTypeA>>?
            )? in
            guard let existing = existingItem else { return nil }
            let updatedPayload = TestTypeA(
                firstly: "updated_\(existing.rowStatus.rowVersion)",
                secondly: "updated"
            )
            let updatedValue = existing.rowValue.createUpdatedItem(withValue: updatedPayload)
            let updatedItem = existing.createUpdatedItem(withValue: updatedValue)
            let historicalItem = self.testHistoricalItemProvider(updatedItem)
            return (
                entry: .update(new: updatedItem, existing: existing),
                historicalEntry: .insert(new: historicalItem)
            )
        }

        #expect(entries.count == 1)

        let retrieved: DatabaseRowType = try await table.getItem(forKey: key1)!
        // row version is bumped by 5 simulated modifications + 1 real update
        #expect(retrieved.rowValue.rowValue.firstly == "updated_6")
    }

    @Test
    func retryingTransactWriteWithHistoricalRowsUnacceptableConcurrency() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1", sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(
            withKey: key1,
            andValue: RowWithItemVersion.newItem(withValue: payload1)
        )
        try await wrappedTable.insertItem(databaseItem1)

        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(
            wrappedDynamoDBTable: wrappedTable,
            simulateConcurrencyModifications: 50,
            simulateOnInsertItem: false
        )

        do {
            try await table.retryingTransactWriteWithHistoricalRows(
                forKeys: [key1],
                withRetries: 3
            ) {
                key,
                existingItem -> (
                    entry: StandardWriteEntry<RowWithItemVersion<TestTypeA>>,
                    historicalEntry: StandardWriteEntry<RowWithItemVersion<TestTypeA>>?
                )? in
                guard let existing = existingItem else { return nil }
                let updatedPayload = TestTypeA(firstly: "updated", secondly: "updated")
                let updatedValue = existing.rowValue.createUpdatedItem(withValue: updatedPayload)
                let updatedItem = existing.createUpdatedItem(withValue: updatedValue)
                let historicalItem = self.testHistoricalItemProvider(updatedItem)
                return (
                    entry: .update(new: updatedItem, existing: existing),
                    historicalEntry: .insert(new: historicalItem)
                )
            }

            Issue.record("Expected error not thrown.")
        } catch DynamoDBTableError.concurrencyError {
            // Success
        } catch {
            Issue.record("Unexpected exception: \(error)")
        }
    }
}
