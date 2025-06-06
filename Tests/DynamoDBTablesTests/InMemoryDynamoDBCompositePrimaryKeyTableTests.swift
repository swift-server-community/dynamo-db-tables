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
//  InMemoryDynamoDBCompositePrimaryKeyTableTests.swift
//  DynamoDBTablesTests
//

import AWSDynamoDB
@testable import DynamoDBTables
import Testing

@PolymorphicOperationReturnType
enum TestPolymorphicOperationReturnType {
    case testTypeA(StandardTypedDatabaseItem<TestTypeA>)
}

struct InMemoryDynamoDBCompositePrimaryKeyTableTests {
    @Test
    func insertAndUpdate() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(databaseItem)

        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try await table.getItem(forKey: key)!

        #expect(databaseItem.compositePrimaryKey.sortKey == retrievedItem.compositePrimaryKey.sortKey)
        #expect(databaseItem.rowValue.firstly == retrievedItem.rowValue.firstly)
        #expect(databaseItem.rowValue.secondly == retrievedItem.rowValue.secondly)

        let updatedPayload = TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
        let updatedDatabaseItem = retrievedItem.createUpdatedItem(withValue: updatedPayload)
        try await table.updateItem(newItem: updatedDatabaseItem, existingItem: retrievedItem)

        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try await table.getItem(forKey: key)!

        #expect(updatedDatabaseItem.compositePrimaryKey.sortKey == secondRetrievedItem.compositePrimaryKey.sortKey)
        #expect(updatedDatabaseItem.rowValue.firstly == secondRetrievedItem.rowValue.firstly)
        #expect(updatedDatabaseItem.rowValue.secondly == secondRetrievedItem.rowValue.secondly)
    }

    @Test
    func doubleInsert() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(databaseItem)

        do {
            try await table.insertItem(databaseItem)
            Issue.record()
        } catch DynamoDBTableError.conditionalCheckFailed {
            // expected error
        } catch {
            Issue.record("Unexpected error \(error)")
        }
    }

    @Test
    func updateWithoutInsert() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let updatedPayload = TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        do {
            try await table.updateItem(newItem: databaseItem.createUpdatedItem(withValue: updatedPayload),
                                       existingItem: databaseItem)
            Issue.record()
        } catch DynamoDBTableError.conditionalCheckFailed {
            // expected error
        } catch {
            Issue.record("Unexpected error \(error)")
        }
    }

    @Test
    func paginatedPolymorphicQuery() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        var items: [StandardTypedDatabaseItem<TestTypeA>] = []

        // add to the database a lot of items - a number that isn't a multiple of the pagination page size
        for index in 0 ..< 1376 {
            let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                  sortKey: "sortId_\(index)")
            let payload = TestTypeA(firstly: "firstly_\(index)", secondly: "secondly_\(index)")
            let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

            try await table.insertItem(databaseItem)
            items.append(databaseItem)
        }

        var retrievedItems: [TestPolymorphicOperationReturnType] = []

        var exclusiveStartKey: String?

        // get everything back from the database
        while true {
            let paginatedItems: ([TestPolymorphicOperationReturnType], String?) =
                try await table.polymorphicQuery(forPartitionKey: "partitionId",
                                                 sortKeyCondition: nil,
                                                 limit: 100,
                                                 exclusiveStartKey: exclusiveStartKey)

            retrievedItems += paginatedItems.0

            // if there are more items
            if let lastEvaluatedKey = paginatedItems.1 {
                exclusiveStartKey = lastEvaluatedKey
            } else {
                // we have all the items
                break
            }
        }

        #expect(items.count == retrievedItems.count)
        // items are returned in sorted order
        let sortedItems = items.sorted { left, right in left.compositePrimaryKey.sortKey < right.compositePrimaryKey.sortKey }

        for index in 0 ..< sortedItems.count {
            let originalItem = sortedItems[index]
            let retrievedItem = retrievedItems[index]

            guard case let .testTypeA(databaseItem) = retrievedItem else {
                Issue.record("Unexpected type.")
                return
            }
            let retrievedValue = databaseItem.rowValue

            #expect(originalItem.compositePrimaryKey.sortKey == databaseItem.compositePrimaryKey.sortKey)
            #expect(originalItem.rowValue.firstly == retrievedValue.firstly)
            #expect(originalItem.rowValue.secondly == retrievedValue.secondly)
        }
    }

    @Test
    func reversedPaginatedPolymorphicQuery() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        var items: [StandardTypedDatabaseItem<TestTypeA>] = []

        // add to the database a lot of items - a number that isn't a multiple of the pagination page size
        for index in 0 ..< 1376 {
            let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                  sortKey: "sortId_\(index)")
            let payload = TestTypeA(firstly: "firstly_\(index)", secondly: "secondly_\(index)")
            let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

            try await table.insertItem(databaseItem)
            items.append(databaseItem)
        }

        var retrievedItems: [TestPolymorphicOperationReturnType] = []

        var exclusiveStartKey: String?

        // get everything back from the database
        while true {
            let paginatedItems: ([TestPolymorphicOperationReturnType], String?) =
                try await table.polymorphicQuery(forPartitionKey: "partitionId",
                                                 sortKeyCondition: nil,
                                                 limit: 100,
                                                 scanIndexForward: false,
                                                 exclusiveStartKey: exclusiveStartKey)

            retrievedItems += paginatedItems.0

            // if there are more items
            if let lastEvaluatedKey = paginatedItems.1 {
                exclusiveStartKey = lastEvaluatedKey
            } else {
                // we have all the items
                break
            }
        }

        #expect(items.count == retrievedItems.count)
        // items are returned in reversed sorted order
        let sortedItems = items.sorted { left, right in left.compositePrimaryKey.sortKey > right.compositePrimaryKey.sortKey }

        for index in 0 ..< sortedItems.count {
            let originalItem = sortedItems[index]
            let retrievedItem = retrievedItems[index]

            guard case let .testTypeA(databaseItem) = retrievedItem else {
                Issue.record("Unexpected type.")
                return
            }
            let retrievedValue = databaseItem.rowValue

            #expect(originalItem.compositePrimaryKey.sortKey == databaseItem.compositePrimaryKey.sortKey)
            #expect(originalItem.rowValue.firstly == retrievedValue.firstly)
            #expect(originalItem.rowValue.secondly == retrievedValue.secondly)
        }
    }

    @Test
    func polymorphicQuery() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        var items: [StandardTypedDatabaseItem<TestTypeA>] = []

        // add to the database a lot of items - a number that isn't a multiple of the pagination page size
        for index in 0 ..< 1376 {
            let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                  sortKey: "sortId_\(index)")
            let payload = TestTypeA(firstly: "firstly_\(index)", secondly: "secondly_\(index)")
            let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

            try await table.insertItem(databaseItem)
            items.append(databaseItem)
        }

        let retrievedItems: [TestPolymorphicOperationReturnType] =
            try await table.polymorphicQuery(forPartitionKey: "partitionId",
                                             sortKeyCondition: nil)

        #expect(items.count == retrievedItems.count)

        for index in 0 ..< items.count {
            let originalItem = items[index]
            let retrievedItem = items[index]

            #expect(originalItem.compositePrimaryKey.sortKey == retrievedItem.compositePrimaryKey.sortKey)
            #expect(originalItem.rowValue.firstly == retrievedItem.rowValue.firstly)
            #expect(originalItem.rowValue.secondly == retrievedItem.rowValue.secondly)
        }
    }

    @Test
    func monomorphicQuery() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        var items: [StandardTypedDatabaseItem<TestTypeA>] = []

        // add to the database a lot of items - a number that isn't a multiple of the pagination page size
        for index in 0 ..< 1376 {
            let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                  sortKey: "sortId_\(index)")
            let payload = TestTypeA(firstly: "firstly_\(index)", secondly: "secondly_\(index)")
            let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

            try await table.insertItem(databaseItem)
            items.append(databaseItem)
        }

        let retrievedItems: [StandardTypedDatabaseItem<TestTypeA>] =
            try await table.query(forPartitionKey: "partitionId",
                                  sortKeyCondition: nil)

        #expect(items.count == retrievedItems.count)

        for index in 0 ..< items.count {
            let originalItem = items[index]
            let retrievedItem = items[index]

            #expect(originalItem.compositePrimaryKey.sortKey == retrievedItem.compositePrimaryKey.sortKey)
            #expect(originalItem.rowValue.firstly == retrievedItem.rowValue.firstly)
            #expect(originalItem.rowValue.secondly == retrievedItem.rowValue.secondly)
        }
    }

    @Test
    func deleteForKey() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        _ = try await table.insertItem(databaseItem)

        let retrievedItem1: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: key)
        #expect(retrievedItem1 != nil)

        try await table.deleteItem(forKey: key)

        let retrievedItem2: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: key)
        #expect(retrievedItem2 == nil)
    }

    @Test
    func deleteForExistingItem() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        _ = try await table.insertItem(databaseItem)

        let retrievedItem1: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: key)
        #expect(retrievedItem1 != nil)

        try await table.deleteItem(existingItem: databaseItem)

        let retrievedItem2: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: key)
        #expect(retrievedItem2 == nil)
    }

    @Test
    func deleteForExistingItemAfterUpdate() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let updatedPayload = TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        let updatedItem = databaseItem.createUpdatedItem(withValue: updatedPayload)

        _ = try await table.insertItem(databaseItem)

        let retrievedItem1: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: key)
        #expect(retrievedItem1 != nil)

        try await table.updateItem(newItem: updatedItem, existingItem: databaseItem)

        do {
            try await table.deleteItem(existingItem: databaseItem)
        } catch DynamoDBTableError.conditionalCheckFailed {
            // expected error
        }

        let retrievedItem2: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: key)
        // the table should still contain the item
        #expect(retrievedItem2 != nil)
    }

    @Test
    func deleteForExistingItemAfterRecreation() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        _ = try await table.insertItem(databaseItem)

        let retrievedItem1: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: key)
        #expect(retrievedItem1 != nil)

        try await table.deleteItem(existingItem: databaseItem)

        // suspend for a small interval so that the creation time of the recreated item is different
        try await Task.sleep(for: .milliseconds(10))

        let recreatedItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        _ = try await table.insertItem(recreatedItem)

        do {
            try await table.deleteItem(existingItem: databaseItem)

            Issue.record("Expected error was not thrown")
        } catch DynamoDBTableError.conditionalCheckFailed {
            // expected error
        } catch {
            Issue.record("Expected error was not thrown")
        }

        let retrievedItem2: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: key)
        // the table should still contain the item
        #expect(retrievedItem2 != nil)
    }

    @Test
    func polymorphicGetItems() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payload2 = TestTypeB(thirdly: "thirdly", fourthly: "fourthly")

        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        _ = try await table.insertItem(databaseItem1)
        _ = try await table.insertItem(databaseItem2)

        let batch: [StandardCompositePrimaryKey: TestQueryableTypes] = try await table.polymorphicGetItems(forKeys: [key1, key2])

        guard case let .testTypeA(retrievedDatabaseItem1) = batch[key1] else {
            Issue.record()
            return
        }

        guard case let .testTypeB(retrievedDatabaseItem2) = batch[key2] else {
            Issue.record()
            return
        }

        #expect(payload1 == retrievedDatabaseItem1.rowValue)
        #expect(payload2 == retrievedDatabaseItem2.rowValue)
    }

    @Test
    func getItems() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payload2 = TestTypeA(firstly: "thirdly", secondly: "fourthly")

        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        _ = try await table.insertItem(databaseItem1)
        _ = try await table.insertItem(databaseItem2)

        let batch: [StandardCompositePrimaryKey: StandardTypedDatabaseItem<TestTypeA>]
            = try await table.getItems(forKeys: [key1, key2])

        guard let retrievedDatabaseItem1 = batch[key1] else {
            Issue.record()
            return
        }

        guard let retrievedDatabaseItem2 = batch[key2] else {
            Issue.record()
            return
        }

        #expect(payload1 == retrievedDatabaseItem1.rowValue)
        #expect(payload2 == retrievedDatabaseItem2.rowValue)
    }

    @Test
    func transactWrite() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload2 = TestTypeA(firstly: "firstly2", secondly: "secondly2")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let entryList: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: databaseItem1),
            .insert(new: databaseItem2),
        ]

        try await table.transactWrite(entryList)

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
    func transactWriteWithMissingRequired() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload2 = TestTypeA(firstly: "firstly2", secondly: "secondly2")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let key3 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId3")
        let payload3 = TestTypeA(firstly: "firstly1A", secondly: "secondly1A")
        let databaseItem3 = StandardTypedDatabaseItem.newItem(withKey: key3, andValue: payload3)

        let key4 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId4")
        let payload4 = TestTypeA(firstly: "firstly2A", secondly: "secondly2A")
        let databaseItem4 = StandardTypedDatabaseItem.newItem(withKey: key4, andValue: payload4)

        let entryList: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: databaseItem1),
            .insert(new: databaseItem2),
        ]

        let constraintList: [StandardTransactionConstraintEntry<TestTypeA>] = [
            .required(existing: databaseItem3),
            .required(existing: databaseItem4),
        ]

        do {
            try await table.transactWrite(entryList, constraints: constraintList)

            Issue.record()
        } catch let DynamoDBTableError.transactionCanceled(reasons: reasons) {
            // both required items are missing
            #expect(reasons.count == 2)
        } catch {
            Issue.record()
        }
    }

    @Test
    func transactWriteTransactWriteWithExistingRequired() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload2 = TestTypeA(firstly: "firstly2", secondly: "secondly2")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let key3 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId3")
        let payload3 = TestTypeA(firstly: "firstly1A", secondly: "secondly1A")
        let databaseItem3 = StandardTypedDatabaseItem.newItem(withKey: key3, andValue: payload3)

        let key4 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId4")
        let payload4 = TestTypeA(firstly: "firstly2A", secondly: "secondly2A")
        let databaseItem4 = StandardTypedDatabaseItem.newItem(withKey: key4, andValue: payload4)

        let entryList: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: databaseItem1),
            .insert(new: databaseItem2),
        ]

        let constraintList: [StandardTransactionConstraintEntry<TestTypeA>] = [
            .required(existing: databaseItem3),
            .required(existing: databaseItem4),
        ]

        try await table.insertItem(databaseItem3)
        try await table.insertItem(databaseItem4)

        try await table.transactWrite(entryList, constraints: constraintList)

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
    func transactWriteWithIncorrectVersionForRequired() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload2 = TestTypeA(firstly: "firstly2", secondly: "secondly2")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let key3 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId3")
        let payload3 = TestTypeA(firstly: "firstly1A", secondly: "secondly1A")
        let databaseItem3 = StandardTypedDatabaseItem.newItem(withKey: key3, andValue: payload3)

        let key4 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId4")
        let payload4 = TestTypeA(firstly: "firstly2A", secondly: "secondly2A")
        let databaseItem4 = StandardTypedDatabaseItem.newItem(withKey: key4, andValue: payload4)

        let entryList: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: databaseItem1),
            .insert(new: databaseItem2),
        ]

        let constraintList: [StandardTransactionConstraintEntry<TestTypeA>] = [
            .required(existing: databaseItem3),
            .required(existing: databaseItem4),
        ]

        try await table.insertItem(databaseItem3)
        try await table.insertItem(databaseItem4)

        let payload5 = TestTypeA(firstly: "firstly2C", secondly: "secondly2C")
        let databaseItem5 = databaseItem4.createUpdatedItem(withValue: payload5)
        try await table.updateItem(newItem: databaseItem5, existingItem: databaseItem4)

        do {
            try await table.transactWrite(entryList, constraints: constraintList)

            Issue.record()
        } catch let DynamoDBTableError.transactionCanceled(reasons: reasons) {
            // one required item exists, one has an incorrect version
            #expect(reasons.count == 1)

            if let first = reasons.first {
                guard case .conditionalCheckFailed = first else {
                    Issue.record("Unexpected error")
                    return
                }
            }
        } catch {
            Issue.record()
        }
    }

    @Test
    func transactWriteWithIncorrectVersionForUpdate() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload2 = TestTypeA(firstly: "firstly2", secondly: "secondly2")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)
        try await table.insertItem(databaseItem2)

        let payload3 = TestTypeA(firstly: "firstly1A", secondly: "secondly1A")
        let databaseItem3 = databaseItem2.createUpdatedItem(withValue: payload3)
        try await table.updateItem(newItem: databaseItem3, existingItem: databaseItem2)

        let key4 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId4")
        let payload4 = TestTypeA(firstly: "firstly2A", secondly: "secondly2A")
        let databaseItem4 = StandardTypedDatabaseItem.newItem(withKey: key4, andValue: payload4)

        let entryList: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: databaseItem1),
            .update(new: databaseItem4, existing: databaseItem2),
        ]

        do {
            try await table.transactWrite(entryList)

            Issue.record()
        } catch let DynamoDBTableError.transactionCanceled(reasons: reasons) {
            // one required item exists, one has an incorrect version
            #expect(reasons.count == 1)

            if let first = reasons.first {
                guard case .conditionalCheckFailed = first else {
                    Issue.record("Unexpected error \(first)")
                    return
                }
            }
        } catch {
            Issue.record()
        }
    }

    @Test
    func polymorphicTransactWrite() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload2 = TestTypeB(thirdly: "thirdly", fourthly: "fourthly")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let entryList: [TestPolymorphicWriteEntry] = [
            .testTypeA(.insert(new: databaseItem1)),
            .testTypeB(.insert(new: databaseItem2)),
        ]

        try await table.polymorphicTransactWrite(entryList)

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
    func polymorphicTransactWriteWithMissingRequired() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload2 = TestTypeB(thirdly: "thirdly", fourthly: "fourthly")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let key3 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId3")
        let payload3 = TestTypeA(firstly: "firstlyB", secondly: "secondlyB")
        let databaseItem3 = StandardTypedDatabaseItem.newItem(withKey: key3, andValue: payload3)

        let key4 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId4")
        let payload4 = TestTypeB(thirdly: "thirdlyB", fourthly: "fourthlyB")
        let databaseItem4 = StandardTypedDatabaseItem.newItem(withKey: key4, andValue: payload4)

        let entryList: [TestPolymorphicWriteEntry] = [
            .testTypeA(.insert(new: databaseItem1)),
            .testTypeB(.insert(new: databaseItem2)),
        ]

        let constraintList: [TestPolymorphicTransactionConstraintEntry] = [
            .testTypeA(.required(existing: databaseItem3)),
            .testTypeB(.required(existing: databaseItem4)),
        ]

        do {
            try await table.polymorphicTransactWrite(entryList, constraints: constraintList)

            Issue.record()
        } catch let DynamoDBTableError.transactionCanceled(reasons: reasons) {
            // both required items are missing
            #expect(reasons.count == 2)
        } catch {
            Issue.record()
        }
    }

    @Test
    func polymorphicTransactWriteTransactWriteWithExistingRequired() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload2 = TestTypeB(thirdly: "thirdly", fourthly: "fourthly")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let key3 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId3")
        let payload3 = TestTypeA(firstly: "firstlyB", secondly: "secondlyB")
        let databaseItem3 = StandardTypedDatabaseItem.newItem(withKey: key3, andValue: payload3)

        let key4 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId4")
        let payload4 = TestTypeB(thirdly: "thirdlyB", fourthly: "fourthlyB")
        let databaseItem4 = StandardTypedDatabaseItem.newItem(withKey: key4, andValue: payload4)

        let entryList: [TestPolymorphicWriteEntry] = [
            .testTypeA(.insert(new: databaseItem1)),
            .testTypeB(.insert(new: databaseItem2)),
        ]

        let constraintList: [TestPolymorphicTransactionConstraintEntry] = [
            .testTypeA(.required(existing: databaseItem3)),
            .testTypeB(.required(existing: databaseItem4)),
        ]

        try await table.insertItem(databaseItem3)
        try await table.insertItem(databaseItem4)

        try await table.polymorphicTransactWrite(entryList, constraints: constraintList)

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
    func polymorphicTransactWriteWithIncorrectVersionForRequired() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload2 = TestTypeB(thirdly: "thirdly", fourthly: "fourthly")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let key3 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId3")
        let payload3 = TestTypeA(firstly: "firstlyB", secondly: "secondlyB")
        let databaseItem3 = StandardTypedDatabaseItem.newItem(withKey: key3, andValue: payload3)

        let key4 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId4")
        let payload4 = TestTypeB(thirdly: "thirdlyB", fourthly: "fourthlyB")
        let databaseItem4 = StandardTypedDatabaseItem.newItem(withKey: key4, andValue: payload4)

        let entryList: [TestPolymorphicWriteEntry] = [
            .testTypeA(.insert(new: databaseItem1)),
            .testTypeB(.insert(new: databaseItem2)),
        ]

        let constraintList: [TestPolymorphicTransactionConstraintEntry] = [
            .testTypeA(.required(existing: databaseItem3)),
            .testTypeB(.required(existing: databaseItem4)),
        ]

        try await table.insertItem(databaseItem3)
        try await table.insertItem(databaseItem4)

        let payload5 = TestTypeB(thirdly: "thirdlyC", fourthly: "fourthlyC")
        let databaseItem5 = databaseItem4.createUpdatedItem(withValue: payload5)
        try await table.updateItem(newItem: databaseItem5, existingItem: databaseItem4)

        do {
            try await table.polymorphicTransactWrite(entryList, constraints: constraintList)

            Issue.record()
        } catch let DynamoDBTableError.transactionCanceled(reasons: reasons) {
            // one required item exists, one has an incorrect version
            #expect(reasons.count == 1)

            if let first = reasons.first {
                guard case .conditionalCheckFailed = first else {
                    Issue.record("Unexpected error")
                    return
                }
            }
        } catch {
            Issue.record()
        }
    }

    @Test
    func polymorphicTransactWriteWithIncorrectVersionForUpdate() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload2 = TestTypeB(thirdly: "thirdly", fourthly: "fourthly")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)
        try await table.insertItem(databaseItem2)

        let payload3 = TestTypeB(thirdly: "thirdlyC", fourthly: "fourthlyC")
        let databaseItem3 = databaseItem2.createUpdatedItem(withValue: payload3)
        try await table.updateItem(newItem: databaseItem3, existingItem: databaseItem2)

        let payload4 = TestTypeB(thirdly: "thirdlyD", fourthly: "fourthlyD")
        let databaseItem4 = databaseItem2.createUpdatedItem(withValue: payload4)

        let entryList: [TestPolymorphicWriteEntry] = [
            .testTypeA(.insert(new: databaseItem1)),
            .testTypeB(.update(new: databaseItem4, existing: databaseItem2)),
        ]

        do {
            try await table.polymorphicTransactWrite(entryList)

            Issue.record()
        } catch let DynamoDBTableError.transactionCanceled(reasons: reasons) {
            // one required item exists, one has an incorrect version
            #expect(reasons.count == 1)

            if let first = reasons.first {
                guard case .conditionalCheckFailed = first else {
                    Issue.record("Unexpected error \(first)")
                    return
                }
            }
        } catch {
            Issue.record()
        }
    }

    private struct TestInMemoryTransactionDelegate: InMemoryTransactionDelegate {
        let errors: [DynamoDBTableError]

        init(errors: [DynamoDBTableError]) {
            self.errors = errors
        }

        func injectErrors(
            inputKeys _: [CompositePrimaryKey<some Any>?], table _: InMemoryDynamoDBCompositePrimaryKeyTable) async throws -> [DynamoDBTableError]
        {
            self.errors
        }
    }

    @Test
    func transactWriteWithInjectedErrors() async throws {
        let errors = [DynamoDBTableError.transactionConflict(message: "There is a Conflict!!")]
        let transactionDelegate = TestInMemoryTransactionDelegate(errors: errors)
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable(transactionDelegate: transactionDelegate)

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload2 = TestTypeA(firstly: "firstly2", secondly: "secondly2")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let key3 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId3")
        let payload3 = TestTypeA(firstly: "firstly1A", secondly: "secondly1A")
        let databaseItem3 = StandardTypedDatabaseItem.newItem(withKey: key3, andValue: payload3)

        let key4 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId4")
        let payload4 = TestTypeA(firstly: "firstly2A", secondly: "secondly2A")
        let databaseItem4 = StandardTypedDatabaseItem.newItem(withKey: key4, andValue: payload4)

        let entryList: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: databaseItem1),
            .insert(new: databaseItem2),
        ]

        let constraintList: [StandardTransactionConstraintEntry<TestTypeA>] = [
            .required(existing: databaseItem3),
            .required(existing: databaseItem4),
        ]

        do {
            try await table.transactWrite(entryList, constraints: constraintList)

            Issue.record()
        } catch let DynamoDBTableError.transactionCanceled(reasons: reasons) {
            // errors should match what was injected
            #expect(errors.count == reasons.count)

            for (error, reason) in zip(errors, reasons) {
                switch (error, reason) {
                case let (.transactionConflict(message1), .transactionConflict(message2)):
                    #expect(message1 == message2)
                default:
                    Issue.record()
                }
            }
        } catch {
            Issue.record()
        }
    }

    @Test
    func transactWriteWithExistingItem() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload2 = TestTypeA(firstly: "firstly2", secondly: "secondly2")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let entryList: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: databaseItem1),
            .insert(new: databaseItem2),
        ]

        try await table.insertItem(databaseItem1)

        do {
            try await table.transactWrite(entryList)

            Issue.record()
        } catch let DynamoDBTableError.transactionCanceled(reasons: reasons) {
            // one required item exists, one already exists
            #expect(reasons.count == 1)

            if let first = reasons.first {
                guard case .duplicateItem = first else {
                    Issue.record("Unexpected error")
                    return
                }
            }
        } catch {
            Issue.record()
        }
    }

    @Test
    func polymorphicTransactWriteWithInjectedErrors() async throws {
        let errors = [DynamoDBTableError.transactionConflict(message: "There is a Conflict!!")]
        let transactionDelegate = TestInMemoryTransactionDelegate(errors: errors)
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable(transactionDelegate: transactionDelegate)

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload2 = TestTypeB(thirdly: "thirdly", fourthly: "fourthly")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let key3 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId3")
        let payload3 = TestTypeA(firstly: "firstlyB", secondly: "secondlyB")
        let databaseItem3 = StandardTypedDatabaseItem.newItem(withKey: key3, andValue: payload3)

        let key4 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId4")
        let payload4 = TestTypeB(thirdly: "thirdlyB", fourthly: "fourthlyB")
        let databaseItem4 = StandardTypedDatabaseItem.newItem(withKey: key4, andValue: payload4)

        let entryList: [TestPolymorphicWriteEntry] = [
            .testTypeA(.insert(new: databaseItem1)),
            .testTypeB(.insert(new: databaseItem2)),
        ]

        let constraintList: [TestPolymorphicTransactionConstraintEntry] = [
            .testTypeA(.required(existing: databaseItem3)),
            .testTypeB(.required(existing: databaseItem4)),
        ]

        do {
            try await table.polymorphicTransactWrite(entryList, constraints: constraintList)

            Issue.record()
        } catch let DynamoDBTableError.transactionCanceled(reasons: reasons) {
            // errors should match what was injected
            #expect(errors.count == reasons.count)

            for (error, reason) in zip(errors, reasons) {
                switch (error, reason) {
                case let (.transactionConflict(message1), .transactionConflict(message2)):
                    #expect(message1 == message2)
                default:
                    Issue.record()
                }
            }
        } catch {
            Issue.record()
        }
    }

    @Test
    func polymorphicTransactWriteWithExistingItem() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload2 = TestTypeB(thirdly: "thirdly", fourthly: "fourthly")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let entryList: [TestPolymorphicWriteEntry] = [
            .testTypeA(.insert(new: databaseItem1)),
            .testTypeB(.insert(new: databaseItem2)),
        ]

        try await table.insertItem(databaseItem1)

        do {
            try await table.polymorphicTransactWrite(entryList)

            Issue.record()
        } catch let DynamoDBTableError.transactionCanceled(reasons: reasons) {
            // one required item exists, one already exists
            #expect(reasons.count == 1)

            if let first = reasons.first {
                guard case .duplicateItem = first else {
                    Issue.record("Unexpected error")
                    return
                }
            }
        } catch {
            Issue.record()
        }
    }

    @Test
    func bulkWrite() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
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

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
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

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
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

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
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

    @Test
    func transactWriteForKeys() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
        let payload3 = TestTypeA(firstly: "firstly3", secondly: "secondly3")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload2 = TestTypeA(firstly: "firstly2", secondly: "secondly2")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        try await table.insertItem(databaseItem1)

        func writeEntryProvider(key: StandardCompositePrimaryKey, existingItem: StandardTypedDatabaseItem<TestTypeA>?) throws
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

        try await table.transactWrite(forKeys: [key1, key2], writeEntryProvider: writeEntryProvider)

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

        let key1 = StandardCompositePrimaryKey(partitionKey: "partitionId1",
                                               sortKey: "sortId1")
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payload3 = TestTypeA(firstly: "firstly3", secondly: "secondly3")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(partitionKey: "partitionId2",
                                               sortKey: "sortId2")
        let payload2 = TestTypeB(thirdly: "thirdly", fourthly: "fourthly")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        try await table.insertItem(databaseItem1)

        func writeEntryProvider(key: StandardCompositePrimaryKey, existingItem: TestQueryableTypes?) throws
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

        try await table.polymorphicTransactWrite(forKeys: [key1, key2], writeEntryProvider: writeEntryProvider)

        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try await table.getItem(forKey: key1)!

        #expect(databaseItem1.compositePrimaryKey.sortKey == retrievedItem.compositePrimaryKey.sortKey)
        #expect(payload3.firstly == retrievedItem.rowValue.firstly)
        #expect(payload3.secondly == retrievedItem.rowValue.secondly)

        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeB> = try await table.getItem(forKey: key2)!

        #expect(databaseItem2.compositePrimaryKey.sortKey == secondRetrievedItem.compositePrimaryKey.sortKey)
        #expect(databaseItem2.rowValue.thirdly == secondRetrievedItem.rowValue.thirdly)
        #expect(databaseItem2.rowValue.fourthly == secondRetrievedItem.rowValue.fourthly)
    }
}
