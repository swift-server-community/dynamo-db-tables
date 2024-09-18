//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/main/Tests/SmokeDynamoDBTests/DynamoDBCompositePrimaryKeyTableUpdateItemConditionallyAtKeyTests.swift
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
//  DynamoDBCompositePrimaryKeyTableUpdateItemConditionallyAtKeyTests.swift
//  DynamoDBTablesTests
//

@testable import DynamoDBTables
import Testing

struct DynamoDBCompositePrimaryKeyTableUpdateItemConditionallyAtKeyTests {
    func updatedPayloadProvider(item _: TestTypeA) -> TestTypeA {
        TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
    }

    typealias TestTypeADatabaseItem = StandardTypedDatabaseItem<TestTypeA>
    func updatedItemProvider(item _: TestTypeADatabaseItem) -> TestTypeADatabaseItem {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        return TestTypeADatabaseItem.newItem(
            withKey: key,
            andValue: TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2"))
    }

    @Test
    func updateItemConditionallyAtKey() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(databaseItem)

        let retrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect(databaseItem.compositePrimaryKey.sortKey == retrievedItem.compositePrimaryKey.sortKey)
        #expect(databaseItem.rowValue.firstly == retrievedItem.rowValue.firstly)
        #expect(databaseItem.rowValue.secondly == retrievedItem.rowValue.secondly)

        _ = try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: self.updatedPayloadProvider)

        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect("sortId" == secondRetrievedItem.compositePrimaryKey.sortKey)
        #expect("firstlyX2" == secondRetrievedItem.rowValue.firstly)
        #expect("secondlyX2" == secondRetrievedItem.rowValue.secondly)
    }

    @Test
    func updateItemConditionallyAtKeyWithItemProvider() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(databaseItem)

        let retrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect(databaseItem.compositePrimaryKey.sortKey == retrievedItem.compositePrimaryKey.sortKey)
        #expect(databaseItem.rowValue.firstly == retrievedItem.rowValue.firstly)
        #expect(databaseItem.rowValue.secondly == retrievedItem.rowValue.secondly)

        _ = try await table.conditionallyUpdateItem(forKey: key, updatedItemProvider: self.updatedItemProvider)

        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect("sortId" == secondRetrievedItem.compositePrimaryKey.sortKey)
        #expect("firstlyX2" == secondRetrievedItem.rowValue.firstly)
        #expect("secondlyX2" == secondRetrievedItem.rowValue.secondly)
    }

    @Test
    func updateItemConditionallyAtKeyWithAcceptableConcurrency() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false)

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(databaseItem)

        let retrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect(databaseItem.compositePrimaryKey.sortKey == retrievedItem.compositePrimaryKey.sortKey)
        #expect(databaseItem.rowValue.firstly == retrievedItem.rowValue.firstly)
        #expect(databaseItem.rowValue.secondly == retrievedItem.rowValue.secondly)

        _ = try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: self.updatedPayloadProvider)

        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect("sortId" == secondRetrievedItem.compositePrimaryKey.sortKey)
        #expect("firstlyX2" == secondRetrievedItem.rowValue.firstly)
        #expect("secondlyX2" == secondRetrievedItem.rowValue.secondly)
    }

    @Test
    func updateItemConditionallyAtKeyWithAcceptableConcurrencyWithItemProvider() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false)

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(databaseItem)

        let retrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect(databaseItem.compositePrimaryKey.sortKey == retrievedItem.compositePrimaryKey.sortKey)
        #expect(databaseItem.rowValue.firstly == retrievedItem.rowValue.firstly)
        #expect(databaseItem.rowValue.secondly == retrievedItem.rowValue.secondly)

        _ = try await table.conditionallyUpdateItem(forKey: key, updatedItemProvider: self.updatedItemProvider)

        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect("sortId" == secondRetrievedItem.compositePrimaryKey.sortKey)
        #expect("firstlyX2" == secondRetrievedItem.rowValue.firstly)
        #expect("secondlyX2" == secondRetrievedItem.rowValue.secondly)
    }

    @Test
    func updateItemConditionallyAtKeyWithUnacceptableConcurrency() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                                        simulateConcurrencyModifications: 100,
                                                                        simulateOnInsertItem: false)

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(databaseItem)

        let retrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect(databaseItem.compositePrimaryKey.sortKey == retrievedItem.compositePrimaryKey.sortKey)
        #expect(databaseItem.rowValue.firstly == retrievedItem.rowValue.firstly)
        #expect(databaseItem.rowValue.secondly == retrievedItem.rowValue.secondly)

        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: self.updatedPayloadProvider)

            Issue.record("Expected concurrency error not thrown.")
        } catch DynamoDBTableError.concurrencyError {
            // expected error thrown
        } catch {
            Issue.record("Unexpected error thrown: \(error).")
        }

        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect("sortId" == secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        #expect("firstly" == secondRetrievedItem.rowValue.firstly)
        #expect("secondly" == secondRetrievedItem.rowValue.secondly)
    }

    @Test
    func updateItemConditionallyAtKeyWithUnacceptableConcurrencyWithItemProvider() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                                        simulateConcurrencyModifications: 100,
                                                                        simulateOnInsertItem: false)

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(databaseItem)

        let retrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect(databaseItem.compositePrimaryKey.sortKey == retrievedItem.compositePrimaryKey.sortKey)
        #expect(databaseItem.rowValue.firstly == retrievedItem.rowValue.firstly)
        #expect(databaseItem.rowValue.secondly == retrievedItem.rowValue.secondly)

        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedItemProvider: self.updatedItemProvider)

            Issue.record("Expected concurrency error not thrown.")
        } catch DynamoDBTableError.concurrencyError {
            // expected error thrown
        } catch {
            Issue.record("Unexpected error thrown: \(error).")
        }

        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect("sortId" == secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        #expect("firstly" == secondRetrievedItem.rowValue.firstly)
        #expect("secondly" == secondRetrievedItem.rowValue.secondly)
    }

    @Test
    func updateItemConditionallyAtKeyWithUnacceptableConcurrencyWithPayloadProvider() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                                        simulateConcurrencyModifications: 100,
                                                                        simulateOnInsertItem: false)

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(databaseItem)

        let retrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect(databaseItem.compositePrimaryKey.sortKey == retrievedItem.compositePrimaryKey.sortKey)
        #expect(databaseItem.rowValue.firstly == retrievedItem.rowValue.firstly)
        #expect(databaseItem.rowValue.secondly == retrievedItem.rowValue.secondly)

        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: self.updatedPayloadProvider)

            Issue.record("Expected concurrency error not thrown.")
        } catch DynamoDBTableError.concurrencyError {
            // expected error thrown
        } catch {
            Issue.record("Unexpected error thrown: \(error).")
        }

        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect("sortId" == secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        #expect("firstly" == secondRetrievedItem.rowValue.firstly)
        #expect("secondly" == secondRetrievedItem.rowValue.secondly)
    }

    enum TestError: Error {
        case everythingIsWrong
    }

    @Test
    func updateItemConditionallyAtKeyWithFailingUpdate() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                                        simulateConcurrencyModifications: 100,
                                                                        simulateOnInsertItem: false)

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(databaseItem)

        let retrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect(databaseItem.compositePrimaryKey.sortKey == retrievedItem.compositePrimaryKey.sortKey)
        #expect(databaseItem.rowValue.firstly == retrievedItem.rowValue.firstly)
        #expect(databaseItem.rowValue.secondly == retrievedItem.rowValue.secondly)

        var passCount = 0

        func failingUpdatedPayloadProvider(item _: TestTypeA) throws -> TestTypeA {
            if passCount < 5 {
                passCount += 1
                return TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
            } else {
                // fail before the retry limit with a custom error
                throw TestError.everythingIsWrong
            }
        }

        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: failingUpdatedPayloadProvider)

            Issue.record("Expected everythingIsWrong error not thrown.")
        } catch TestError.everythingIsWrong {
            // expected error thrown
        } catch {
            Issue.record("Unexpected error thrown: \(error).")
        }

        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect("sortId" == secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        #expect("firstly" == secondRetrievedItem.rowValue.firstly)
        #expect("secondly" == secondRetrievedItem.rowValue.secondly)
    }

    @Test
    func updateItemConditionallyAtKeyWithFailingUpdateWithItemProvider() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                                        simulateConcurrencyModifications: 100,
                                                                        simulateOnInsertItem: false)

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(databaseItem)

        let retrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect(databaseItem.compositePrimaryKey.sortKey == retrievedItem.compositePrimaryKey.sortKey)
        #expect(databaseItem.rowValue.firstly == retrievedItem.rowValue.firstly)
        #expect(databaseItem.rowValue.secondly == retrievedItem.rowValue.secondly)

        var passCount = 0

        func failingUpdatedItemProvider(item _: TestTypeADatabaseItem) throws -> TestTypeADatabaseItem {
            if passCount < 5 {
                passCount += 1
                let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                      sortKey: "sortId")
                return TestTypeADatabaseItem.newItem(
                    withKey: key,
                    andValue: TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2"))
            } else {
                // fail before the retry limit with a custom error
                throw TestError.everythingIsWrong
            }
        }

        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedItemProvider: failingUpdatedItemProvider)

            Issue.record("Expected everythingIsWrong error not thrown.")
        } catch TestError.everythingIsWrong {
            // expected error thrown
        } catch {
            Issue.record("Unexpected error thrown: \(error).")
        }

        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!

        #expect("sortId" == secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        #expect("firstly" == secondRetrievedItem.rowValue.firstly)
        #expect("secondly" == secondRetrievedItem.rowValue.secondly)
    }

    @Test
    func updateItemConditionallyAtKeyWithUnknownItem() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")

        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: self.updatedPayloadProvider)

            Issue.record("Expected concurrency error not thrown.")
        } catch DynamoDBTableError.conditionalCheckFailed {
            // expected error thrown
        } catch {
            Issue.record("Unexpected error thrown: \(error).")
        }

        let secondRetrievedItem: TestTypeADatabaseItem? = try await table.getItem(forKey: key)

        #expect(secondRetrievedItem == nil)
    }

    @Test
    func updateItemConditionallyAtKeyWithUnknownItemWithItemProvider() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")

        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedItemProvider: self.updatedItemProvider)

            Issue.record("Expected concurrency error not thrown.")
        } catch DynamoDBTableError.conditionalCheckFailed {
            // expected error thrown
        } catch {
            Issue.record("Unexpected error thrown: \(error).")
        }

        let secondRetrievedItem: TestTypeADatabaseItem? = try await table.getItem(forKey: key)

        #expect(secondRetrievedItem == nil)
    }
}
