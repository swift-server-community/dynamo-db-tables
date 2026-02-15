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
//  DynamoDBCompositePrimaryKeyTableHistoricalItemExtensionsTests.swift
//      DynamoDB Historical Client Extension Tests
//  DynamoDBTablesTests
//

import Foundation
import Testing

@testable import DynamoDBTables

private typealias DatabaseRowType =
    TypedTTLDatabaseItem<StandardPrimaryKeyAttributes, RowWithItemVersion<TestTypeA>, StandardTimeToLiveAttributes>

struct CompositePrimaryKeyDynamoDBHistoricalClientTests {
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
        } catch DynamoDBTableError.transactionCanceled {
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
        } catch DynamoDBTableError.transactionCanceled {
            // Success
        } catch {
            Issue.record("Unexpected exception")
        }
    }
}
