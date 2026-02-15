//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/main/Tests/SmokeDynamoDBTests/InMemoryDynamoDBCompositePrimaryKeyTableTests.swift
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
//  InMemoryDynamoDBCompositePrimaryKeyTable+bulkWriteTests.swift
//  DynamoDBTablesTests
//

import Testing

@testable import DynamoDBTables

struct InMemoryDynamoDBCompositePrimaryKeyTableBulkWriteTests {
    @Test
    func bulkWrite() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId1"
        )
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId2"
        )
        let payload2 = TestTypeA(firstly: "firstly2", secondly: "secondly2")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let entryList: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: databaseItem1),
            .insert(new: databaseItem2),
        ]

        try await table.bulkWrite(entryList)

        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try await table.getItem(forKey: key1)!

        #expect(databaseItem1.compositePrimaryKey.sortKey == retrievedItem.compositePrimaryKey.sortKey)
        #expect(databaseItem1.rowValue.firstly == retrievedItem.rowValue.firstly)
        #expect(databaseItem1.rowValue.secondly == retrievedItem.rowValue.secondly)

        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try await table.getItem(forKey: key2)!

        #expect(databaseItem2.compositePrimaryKey.sortKey == secondRetrievedItem.compositePrimaryKey.sortKey)
        #expect(databaseItem2.rowValue.firstly == secondRetrievedItem.rowValue.firstly)
        #expect(databaseItem2.rowValue.secondly == secondRetrievedItem.rowValue.secondly)
    }

    @Test
    func bulkWriteWithExistingItem() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId1"
        )
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId2"
        )
        let payload2 = TestTypeA(firstly: "firstly2", secondly: "secondly2")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let entryList: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: databaseItem1),
            .insert(new: databaseItem2),
        ]

        try await table.insertItem(databaseItem1)

        do {
            try await table.bulkWrite(entryList)

            Issue.record()
        } catch let DynamoDBTableError.batchFailures(errors) {
            // one required item exists, one already exists
            #expect(errors.count == 1)

            guard case .conditionalCheckFailed = errors[0] else {
                Issue.record("Expected error to be conditionalCheckFailed")
                return
            }
        } catch {
            Issue.record()
        }
    }

    @Test
    func polymorphicBulkWrite() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId1"
        )
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId2"
        )
        let payload2 = TestTypeB(thirdly: "thirdly", fourthly: "fourthly")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let entryList: [TestPolymorphicWriteEntry] = [
            .testTypeA(.insert(new: databaseItem1)),
            .testTypeB(.insert(new: databaseItem2)),
        ]

        try await table.polymorphicBulkWrite(entryList)

        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try await table.getItem(forKey: key1)!

        #expect(databaseItem1.compositePrimaryKey.sortKey == retrievedItem.compositePrimaryKey.sortKey)
        #expect(databaseItem1.rowValue.firstly == retrievedItem.rowValue.firstly)
        #expect(databaseItem1.rowValue.secondly == retrievedItem.rowValue.secondly)

        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeB> = try await table.getItem(forKey: key2)!

        #expect(databaseItem2.compositePrimaryKey.sortKey == secondRetrievedItem.compositePrimaryKey.sortKey)
        #expect(databaseItem2.rowValue.thirdly == secondRetrievedItem.rowValue.thirdly)
        #expect(databaseItem2.rowValue.fourthly == secondRetrievedItem.rowValue.fourthly)
    }

    @Test
    func polymorphicBulkWriteWithExistingItem() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId1"
        )
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId2"
        )
        let payload2 = TestTypeB(thirdly: "thirdly", fourthly: "fourthly")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let entryList: [TestPolymorphicWriteEntry] = [
            .testTypeA(.insert(new: databaseItem1)),
            .testTypeB(.insert(new: databaseItem2)),
        ]

        try await table.insertItem(databaseItem1)

        do {
            try await table.polymorphicBulkWrite(entryList)

            Issue.record()
        } catch let DynamoDBTableError.batchFailures(errors) {
            // one required item exists, one already exists
            #expect(errors.count == 1)

            guard case .conditionalCheckFailed = errors[0] else {
                Issue.record("Expected error to be conditionalCheckFailed")
                return
            }
        } catch {
            Issue.record()
        }
    }
}
