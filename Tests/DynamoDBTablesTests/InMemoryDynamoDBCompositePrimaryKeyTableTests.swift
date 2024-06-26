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
import XCTest

enum TestPolymorphicOperationReturnType: PolymorphicOperationReturnType {
    typealias AttributesType = StandardPrimaryKeyAttributes

    static let types: [(Codable.Type, PolymorphicOperationReturnOption<StandardPrimaryKeyAttributes, Self>)] = [
        (TestTypeA.self, .init { .testTypeA($0) }),
    ]

    case testTypeA(StandardTypedDatabaseItem<TestTypeA>)
}

class InMemoryDynamoDBCompositePrimaryKeyTableTests: XCTestCase {
    func testInsertAndUpdate() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(databaseItem)

        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try await table.getItem(forKey: key)!

        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)

        let updatedPayload = TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
        let updatedDatabaseItem = retrievedItem.createUpdatedItem(withValue: updatedPayload)
        try await table.updateItem(newItem: updatedDatabaseItem, existingItem: retrievedItem)

        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try await table.getItem(forKey: key)!

        XCTAssertEqual(updatedDatabaseItem.compositePrimaryKey.sortKey, secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(updatedDatabaseItem.rowValue.firstly, secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual(updatedDatabaseItem.rowValue.secondly, secondRetrievedItem.rowValue.secondly)
    }

    func testDoubleInsert() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(databaseItem)

        do {
            try await table.insertItem(databaseItem)
            XCTFail()
        } catch DynamoDBTableError.conditionalCheckFailed {
            // expected error
        } catch {
            XCTFail("Unexpected error \(error)")
        }
    }

    func testUpdateWithoutInsert() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let updatedPayload = TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        do {
            try await table.updateItem(newItem: databaseItem.createUpdatedItem(withValue: updatedPayload),
                                       existingItem: databaseItem)
            XCTFail()
        } catch DynamoDBTableError.conditionalCheckFailed {
            // expected error
        } catch {
            XCTFail("Unexpected error \(error)")
        }
    }

    func testPaginatedPolymorphicQuery() async throws {
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

        XCTAssertEqual(items.count, retrievedItems.count)
        // items are returned in sorted order
        let sortedItems = items.sorted { left, right in left.compositePrimaryKey.sortKey < right.compositePrimaryKey.sortKey }

        for index in 0 ..< sortedItems.count {
            let originalItem = sortedItems[index]
            let retrievedItem = retrievedItems[index]

            guard case let .testTypeA(databaseItem) = retrievedItem else {
                XCTFail("Unexpected type.")
                return
            }
            let retrievedValue = databaseItem.rowValue

            XCTAssertEqual(originalItem.compositePrimaryKey.sortKey, databaseItem.compositePrimaryKey.sortKey)
            XCTAssertEqual(originalItem.rowValue.firstly, retrievedValue.firstly)
            XCTAssertEqual(originalItem.rowValue.secondly, retrievedValue.secondly)
        }
    }

    func testReversedPaginatedPolymorphicQuery() async throws {
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

        XCTAssertEqual(items.count, retrievedItems.count)
        // items are returned in reversed sorted order
        let sortedItems = items.sorted { left, right in left.compositePrimaryKey.sortKey > right.compositePrimaryKey.sortKey }

        for index in 0 ..< sortedItems.count {
            let originalItem = sortedItems[index]
            let retrievedItem = retrievedItems[index]

            guard case let .testTypeA(databaseItem) = retrievedItem else {
                XCTFail("Unexpected type.")
                return
            }
            let retrievedValue = databaseItem.rowValue

            XCTAssertEqual(originalItem.compositePrimaryKey.sortKey, databaseItem.compositePrimaryKey.sortKey)
            XCTAssertEqual(originalItem.rowValue.firstly, retrievedValue.firstly)
            XCTAssertEqual(originalItem.rowValue.secondly, retrievedValue.secondly)
        }
    }

    func testPolymorphicQuery() async throws {
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

        XCTAssertEqual(items.count, retrievedItems.count)

        for index in 0 ..< items.count {
            let originalItem = items[index]
            let retrievedItem = items[index]

            XCTAssertEqual(originalItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
            XCTAssertEqual(originalItem.rowValue.firstly, retrievedItem.rowValue.firstly)
            XCTAssertEqual(originalItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        }
    }

    func testMonomorphicQuery() async throws {
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

        XCTAssertEqual(items.count, retrievedItems.count)

        for index in 0 ..< items.count {
            let originalItem = items[index]
            let retrievedItem = items[index]

            XCTAssertEqual(originalItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
            XCTAssertEqual(originalItem.rowValue.firstly, retrievedItem.rowValue.firstly)
            XCTAssertEqual(originalItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        }
    }

    func testDeleteForKey() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        _ = try await table.insertItem(databaseItem)

        let retrievedItem1: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: key)
        XCTAssertNotNil(retrievedItem1)

        try await table.deleteItem(forKey: key)

        let retrievedItem2: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: key)
        XCTAssertNil(retrievedItem2)
    }

    func testDeleteForExistingItem() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        _ = try await table.insertItem(databaseItem)

        let retrievedItem1: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: key)
        XCTAssertNotNil(retrievedItem1)

        try await table.deleteItem(existingItem: databaseItem)

        let retrievedItem2: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: key)
        XCTAssertNil(retrievedItem2)
    }

    func testDeleteForExistingItemAfterUpdate() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let updatedPayload = TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        let updatedItem = databaseItem.createUpdatedItem(withValue: updatedPayload)

        _ = try await table.insertItem(databaseItem)

        let retrievedItem1: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: key)
        XCTAssertNotNil(retrievedItem1)

        try await table.updateItem(newItem: updatedItem, existingItem: databaseItem)

        do {
            try await table.deleteItem(existingItem: databaseItem)
        } catch DynamoDBTableError.conditionalCheckFailed {
            // expected error
        }

        let retrievedItem2: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: key)
        // the table should still contain the item
        XCTAssertNotNil(retrievedItem2)
    }

    func testDeleteForExistingItemAfterRecreation() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        _ = try await table.insertItem(databaseItem)

        let retrievedItem1: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: key)
        XCTAssertNotNil(retrievedItem1)

        try await table.deleteItem(existingItem: databaseItem)
        sleep(1)
        let recreatedItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        _ = try await table.insertItem(recreatedItem)

        do {
            try await table.deleteItem(existingItem: databaseItem)
        } catch DynamoDBTableError.conditionalCheckFailed {
            // expected error
        }

        let retrievedItem2: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: key)
        // the table should still contain the item
        XCTAssertNotNil(retrievedItem2)
    }

    func testPolymorphicGetItems() async throws {
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
            XCTFail()
            return
        }

        guard case let .testTypeB(retrievedDatabaseItem2) = batch[key2] else {
            XCTFail()
            return
        }

        XCTAssertEqual(payload1, retrievedDatabaseItem1.rowValue)
        XCTAssertEqual(payload2, retrievedDatabaseItem2.rowValue)
    }

    func testGetItems() async throws {
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
            XCTFail()
            return
        }

        guard let retrievedDatabaseItem2 = batch[key2] else {
            XCTFail()
            return
        }

        XCTAssertEqual(payload1, retrievedDatabaseItem1.rowValue)
        XCTAssertEqual(payload2, retrievedDatabaseItem2.rowValue)
    }

    func testBulkWriteWithoutThrowing() async throws {
        typealias TestObject = TestTypeA
        typealias TestObjectDatabaseItem = StandardTypedDatabaseItem<TestObject>
        typealias TestObjectWriteEntry = StandardWriteEntry<TestObject>

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        var entryList: [TestObjectWriteEntry] = []
        var index = 0
        while index < 26 {
            let key = StandardCompositePrimaryKey(partitionKey: "partitionId\(index % 25)", sortKey: "sortId\(index % 25)")
            let test = TestObject(firstly: "firstly", secondly: "secondly")
            let testItem = TestObjectDatabaseItem.newItem(withKey: key, andValue: test)
            entryList.append(TestObjectWriteEntry.insert(new: testItem))
            index += 1
        }

        let result1 = try await table.bulkWriteWithoutThrowing(entryList)
        XCTAssertEqual(result1.count, 1)
        if result1.contains(DynamoDBClientTypes.BatchStatementErrorCodeEnum.duplicateitem) {
            return
        } else {
            XCTFail("should contain duplicateitem error")
        }
    }

    func testPolymorphicTransactWrite() async throws {
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

        XCTAssertEqual(databaseItem1.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem1.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem1.rowValue.secondly, retrievedItem.rowValue.secondly)

        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeB> = try await table.getItem(forKey: key2)!

        XCTAssertEqual(databaseItem2.compositePrimaryKey.sortKey, secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem2.rowValue.thirdly, secondRetrievedItem.rowValue.thirdly)
        XCTAssertEqual(databaseItem2.rowValue.fourthly, secondRetrievedItem.rowValue.fourthly)
    }

    func testPolymorphicTransactWriteWithMissingRequired() async throws {
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

            XCTFail()
        } catch let DynamoDBTableError.transactionCanceled(reasons: reasons) {
            // both required items are missing
            XCTAssertEqual(2, reasons.count)
        } catch {
            XCTFail()
        }
    }

    func testPolymorphicTransactWriteTransactWriteWithExistingRequired() async throws {
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

        XCTAssertEqual(databaseItem1.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem1.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem1.rowValue.secondly, retrievedItem.rowValue.secondly)

        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeB> = try await table.getItem(forKey: key2)!

        XCTAssertEqual(databaseItem2.compositePrimaryKey.sortKey, secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem2.rowValue.thirdly, secondRetrievedItem.rowValue.thirdly)
        XCTAssertEqual(databaseItem2.rowValue.fourthly, secondRetrievedItem.rowValue.fourthly)
    }

    func testPolymorphicTransactWriteWithIncorrectVersionForRequired() async throws {
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

            XCTFail()
        } catch let DynamoDBTableError.transactionCanceled(reasons: reasons) {
            // one required item exists, one has an incorrect version
            XCTAssertEqual(1, reasons.count)

            if let first = reasons.first {
                guard case .transactionConditionalCheckFailed = first else {
                    XCTFail("Unexpected error")
                    return
                }
            }
        } catch {
            XCTFail()
        }
    }

    func testPolymorphicTransactWriteWithIncorrectVersionForUpdate() async throws {
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

            XCTFail()
        } catch let DynamoDBTableError.transactionCanceled(reasons: reasons) {
            // one required item exists, one has an incorrect version
            XCTAssertEqual(1, reasons.count)

            if let first = reasons.first {
                guard case .transactionConditionalCheckFailed = first else {
                    XCTFail("Unexpected error \(first)")
                    return
                }
            }
        } catch {
            XCTFail()
        }
    }

    private struct TestInMemoryTransactionDelegate: InMemoryTransactionDelegate {
        let errors: [DynamoDBTableError]

        init(errors: [DynamoDBTableError]) {
            self.errors = errors
        }

        func injectErrors(
            _: [some PolymorphicWriteEntry], constraints _: [some PolymorphicTransactionConstraintEntry],
            table _: InMemoryDynamoDBCompositePrimaryKeyTable) async -> [DynamoDBTableError]
        {
            self.errors
        }
    }

    func testPolymorphicTransactWriteWithInjectedErrors() async throws {
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

            XCTFail()
        } catch let DynamoDBTableError.transactionCanceled(reasons: reasons) {
            // errors should match what was injected
            XCTAssertEqual(errors.count, reasons.count)

            for (error, reason) in zip(errors, reasons) {
                switch (error, reason) {
                case let (.transactionConflict(message1), .transactionConflict(message2)):
                    XCTAssertEqual(message1, message2)
                default:
                    XCTFail()
                }
            }
        } catch {
            XCTFail()
        }
    }

    func testPolymorphicTransactWriteWithExistingItem() async throws {
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

            XCTFail()
        } catch let DynamoDBTableError.transactionCanceled(reasons: reasons) {
            // one required item exists, one already exists
            XCTAssertEqual(1, reasons.count)

            if let first = reasons.first {
                guard case .duplicateItem = first else {
                    XCTFail("Unexpected error")
                    return
                }
            }
        } catch {
            XCTFail()
        }
    }

    func testPolymorphicBulkWrite() async throws {
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

        XCTAssertEqual(databaseItem1.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem1.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem1.rowValue.secondly, retrievedItem.rowValue.secondly)

        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeB> = try await table.getItem(forKey: key2)!

        XCTAssertEqual(databaseItem2.compositePrimaryKey.sortKey, secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem2.rowValue.thirdly, secondRetrievedItem.rowValue.thirdly)
        XCTAssertEqual(databaseItem2.rowValue.fourthly, secondRetrievedItem.rowValue.fourthly)
    }

    func testBulkWriteWithExistingItem() async throws {
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

            XCTFail()
        } catch let DynamoDBTableError.batchErrorsReturned(errorCount, _) {
            // one required item exists, one already exists
            XCTAssertEqual(1, errorCount)
        } catch {
            XCTFail()
        }
    }
}
