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
//  InMemoryDynamoDBCompositePrimaryKeyTable+retryingTransactWriteTests.swift
//  DynamoDBTablesTests
//

import Testing

@testable import DynamoDBTables

struct InMemoryDynamoDBCompositePrimaryKeyTableRetryingTransactWriteTests {
    @Test
    func transactWriteForKeys() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId1"
        )
        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
        let payload3 = TestTypeA(firstly: "firstly3", secondly: "secondly3")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId2"
        )
        let payload2 = TestTypeA(firstly: "firstly2", secondly: "secondly2")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        try await table.insertItem(databaseItem1)

        @Sendable
        func writeEntryProvider(
            key: StandardCompositePrimaryKey,
            existingItem: StandardTypedDatabaseItem<TestTypeA>?
        ) throws
            -> StandardWriteEntry<TestTypeA>?
        {
            if key == key1 {
                let existingItem = try #require(existingItem)

                let updatedItem = existingItem.createUpdatedItem(withValue: payload3)
                return .update(new: updatedItem, existing: existingItem)
            } else if key == key2 {
                #expect(existingItem == nil)

                return .insert(new: databaseItem2)
            } else {
                return nil
            }
        }

        try await table.retryingTransactWrite(forKeys: [key1, key2], writeEntryProvider: writeEntryProvider)

        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try await table.getItem(forKey: key1)!

        #expect(databaseItem1.compositePrimaryKey.sortKey == retrievedItem.compositePrimaryKey.sortKey)
        #expect(payload3.firstly == retrievedItem.rowValue.firstly)
        #expect(payload3.secondly == retrievedItem.rowValue.secondly)

        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try await table.getItem(forKey: key2)!

        #expect(databaseItem2.compositePrimaryKey.sortKey == secondRetrievedItem.compositePrimaryKey.sortKey)
        #expect(databaseItem2.rowValue.firstly == secondRetrievedItem.rowValue.firstly)
        #expect(databaseItem2.rowValue.secondly == secondRetrievedItem.rowValue.secondly)
    }

    @Test
    func polymorphicTransactWriteForKeys() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId1"
        )
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payload3 = TestTypeA(firstly: "firstly3", secondly: "secondly3")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId2"
        )
        let payload2 = TestTypeB(thirdly: "thirdly", fourthly: "fourthly")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        try await table.insertItem(databaseItem1)

        @Sendable
        func writeEntryProvider(
            key: StandardCompositePrimaryKey,
            existingItem: TestQueryableTypes?
        ) throws
            -> TestPolymorphicWriteEntry?
        {
            if key == key1 {
                let testQueryableTypes = try #require(existingItem)

                guard case let .testTypeA(existingItem) = testQueryableTypes else {
                    fatalError()
                }

                let updatedItem = existingItem.createUpdatedItem(withValue: payload3)
                return .testTypeA(.update(new: updatedItem, existing: existingItem))
            } else if key == key2 {
                #expect(existingItem == nil)

                return .testTypeB(.insert(new: databaseItem2))
            } else {
                return nil
            }
        }

        try await table.retryingPolymorphicTransactWrite(forKeys: [key1, key2], writeEntryProvider: writeEntryProvider)

        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try await table.getItem(forKey: key1)!

        #expect(databaseItem1.compositePrimaryKey.sortKey == retrievedItem.compositePrimaryKey.sortKey)
        #expect(payload3.firstly == retrievedItem.rowValue.firstly)
        #expect(payload3.secondly == retrievedItem.rowValue.secondly)

        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeB> = try await table.getItem(forKey: key2)!

        #expect(databaseItem2.compositePrimaryKey.sortKey == secondRetrievedItem.compositePrimaryKey.sortKey)
        #expect(databaseItem2.rowValue.thirdly == secondRetrievedItem.rowValue.thirdly)
        #expect(databaseItem2.rowValue.fourthly == secondRetrievedItem.rowValue.fourthly)
    }

    @Test
    func retryingTransactWriteExceedsTransactionSizeLimit() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let keys = (0..<101).map { i in
            StandardCompositePrimaryKey(partitionKey: "pk\(i)", sortKey: "sk\(i)")
        }

        do {
            try await table.retryingTransactWrite(
                forKeys: keys
            ) { key, _ -> StandardWriteEntry<TestTypeA>? in
                let payload = TestTypeA(firstly: "f", secondly: "s")
                return .insert(new: StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload))
            }

            Issue.record("Expected error not thrown.")
        } catch DynamoDBTableError.itemCollectionSizeLimitExceeded(let attemptedSize, let maximumSize) {
            #expect(attemptedSize == 101)
            #expect(maximumSize == 100)
        } catch {
            Issue.record("Unexpected exception: \(error)")
        }
    }

    @Test
    func retryingTransactWriteWithConstraintsExceedsTransactionSizeLimit() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let keys = (0..<50).map { i in
            StandardCompositePrimaryKey(partitionKey: "pk\(i)", sortKey: "sk\(i)")
        }

        let constraints: [StandardTransactionConstraintEntry<TestTypeA>] = (0..<51).map { i in
            let key = StandardCompositePrimaryKey(partitionKey: "cpk\(i)", sortKey: "csk\(i)")
            let item = StandardTypedDatabaseItem.newItem(
                withKey: key,
                andValue: TestTypeA(firstly: "f", secondly: "s")
            )
            return .required(existing: item)
        }

        do {
            try await table.retryingTransactWrite(
                forKeys: keys,
                constraints: constraints
            ) { key, _ -> StandardWriteEntry<TestTypeA>? in
                let payload = TestTypeA(firstly: "f", secondly: "s")
                return .insert(new: StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload))
            }

            Issue.record("Expected error not thrown.")
        } catch DynamoDBTableError.itemCollectionSizeLimitExceeded(let attemptedSize, let maximumSize) {
            #expect(attemptedSize == 101)
            #expect(maximumSize == 100)
        } catch {
            Issue.record("Unexpected exception: \(error)")
        }
    }
}
