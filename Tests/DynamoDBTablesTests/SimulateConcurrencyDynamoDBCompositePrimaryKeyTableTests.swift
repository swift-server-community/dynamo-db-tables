//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Tests/SmokeDynamoDBTests/SimulateConcurrencyDynamoDBCompositePrimaryKeyTableTests.swift
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
//  SimulateConcurrencyDynamoDBCompositePrimaryKeyTableTests.swift
//  DynamoDBTablesTests
//

import AWSDynamoDB
@testable import DynamoDBTables
import Testing

private typealias DatabaseRowType = StandardTypedDatabaseItem<TestTypeA>

typealias CustomTypedDatabaseItem = StandardTypedDatabaseItem

@PolymorphicOperationReturnType(databaseItemType: "CustomTypedDatabaseItem")
enum ExpectedQueryableTypes {
    case testTypeA(CustomTypedDatabaseItem<TestTypeA>)
}

struct SimulateConcurrencyDynamoDBCompositePrimaryKeyTableTests {
    @Test
    func simulateConcurrencyOnInsert() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: InMemoryDynamoDBCompositePrimaryKeyTable(),
                                                                        simulateConcurrencyModifications: 5)

        do {
            try await table.insertItem(databaseItem)
            Issue.record()
        } catch {
            // expected error thrown
        }
    }

    private func verifyWithUpdate(table: SimulateConcurrencyDynamoDBCompositePrimaryKeyTable,
                                  databaseItem: StandardTypedDatabaseItem<TestTypeA>,
                                  key: StandardCompositePrimaryKey,
                                  expectedFailureCount: Int) async throws
    {
        try await table.insertItem(databaseItem)
        var errorCount = 0

        for _ in 0 ..< 10 {
            guard let item: DatabaseRowType = try await table.getItem(forKey: key) else {
                Issue.record("Expected to retrieve item and there was none")
                return
            }

            do {
                try await table.updateItem(newItem: item.createUpdatedItem(withValue: item.rowValue), existingItem: item)
            } catch {
                errorCount += 1
            }
        }

        // should fail the expected number of times
        #expect(expectedFailureCount == errorCount)

        try await table.deleteItem(forKey: key)

        let nowDeletedItem: DatabaseRowType? = try await table.getItem(forKey: key)
        #expect(nowDeletedItem == nil)
    }

    @Test
    func simulateConcurrencyWithUpdate() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: InMemoryDynamoDBCompositePrimaryKeyTable(),
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false)

        try await verifyWithUpdate(table: table, databaseItem: databaseItem, key: key, expectedFailureCount: 5)
    }

    @Test
    func simulateWithNoConcurrency() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: InMemoryDynamoDBCompositePrimaryKeyTable(),
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false,
                                                                        simulateOnUpdateItem: false)

        try await verifyWithUpdate(table: table, databaseItem: databaseItem, key: key, expectedFailureCount: 0)
    }

    @Test
    func simulateConcurrencyWithPolymorphicQuery() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: InMemoryDynamoDBCompositePrimaryKeyTable(),
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false)

        try await table.insertItem(databaseItem)
        var errorCount = 0

        for _ in 0 ..< 10 {
            let query: [ExpectedQueryableTypes] = try await table.polymorphicQuery(forPartitionKey: "partitionId",
                                                                                   sortKeyCondition: .equals("sortId"))

            guard query.count == 1, case let .testTypeA(firstDatabaseItem) = query[0] else {
                Issue.record("Expected to retrieve item and there wasn't the correct number or type.")
                return
            }

            let firstValue = firstDatabaseItem.rowValue

            let existingItem = DatabaseRowType(compositePrimaryKey: firstDatabaseItem.compositePrimaryKey,
                                               createDate: firstDatabaseItem.createDate,
                                               rowStatus: firstDatabaseItem.rowStatus,
                                               rowValue: firstValue)
            let item = firstDatabaseItem.createUpdatedItem(withValue: firstValue)

            do {
                try await table.updateItem(newItem: item, existingItem: existingItem)
            } catch {
                errorCount += 1
            }
        }

        // should only fail five times
        #expect(errorCount == 5)

        try await table.deleteItem(forKey: key)

        let nowDeletedItem: DatabaseRowType? = try await table.getItem(forKey: key)
        #expect(nowDeletedItem == nil)
    }

    @Test
    func simulateConcurrencyWithMonomorphicQuery() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: InMemoryDynamoDBCompositePrimaryKeyTable(),
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false)

        try await table.insertItem(databaseItem)
        var errorCount = 0

        for _ in 0 ..< 10 {
            let query: [DatabaseRowType] = try await table.query(forPartitionKey: "partitionId",
                                                                 sortKeyCondition: .equals("sortId"))

            guard query.count == 1, let firstQuery = query.first else {
                Issue.record("Expected to retrieve item and there wasn't the correct number or type.")
                return
            }

            let existingItem = DatabaseRowType(compositePrimaryKey: firstQuery.compositePrimaryKey,
                                               createDate: firstQuery.createDate,
                                               rowStatus: firstQuery.rowStatus,
                                               rowValue: firstQuery.rowValue)
            let item = firstQuery.createUpdatedItem(withValue: firstQuery.rowValue)

            do {
                try await table.updateItem(newItem: item, existingItem: existingItem)
            } catch {
                errorCount += 1
            }
        }

        // should only fail five times
        #expect(errorCount == 5)

        try await table.deleteItem(forKey: key)

        let nowDeletedItem: DatabaseRowType? = try await table.getItem(forKey: key)
        #expect(nowDeletedItem == nil)
    }

    @Test
    func simulateClobberConcurrencyWithGet() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                                        simulateConcurrencyModifications: 5)

        try await wrappedTable.insertItem(databaseItem)

        for _ in 0 ..< 10 {
            guard let item: DatabaseRowType = try await table.getItem(forKey: key) else {
                Issue.record("Expected to retrieve item and there was none")
                return
            }

            #expect(databaseItem.rowStatus.rowVersion == item.rowStatus.rowVersion)

            try await wrappedTable.updateItem(newItem: item.createUpdatedItem(withValue: item.rowValue), existingItem: item)
            try await table.clobberItem(item)
        }

        try await table.deleteItem(forKey: key)

        let nowDeletedItem: DatabaseRowType? = try await table.getItem(forKey: key)
        #expect(nowDeletedItem == nil)
    }
}
