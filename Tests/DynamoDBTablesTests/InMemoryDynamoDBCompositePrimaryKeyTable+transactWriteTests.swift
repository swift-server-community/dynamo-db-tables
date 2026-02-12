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
//  InMemoryDynamoDBCompositePrimaryKeyTable+transactWriteTests.swift
//  DynamoDBTablesTests
//

import AWSDynamoDB
import Testing

@testable import DynamoDBTables

struct InMemoryDynamoDBCompositePrimaryKeyTableTransactWriteTests {
    @Test
    func transactWrite() async throws {
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable()

        let key1 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId1"
        )
        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
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

        let key1 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId1"
        )
        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId2"
        )
        let payload2 = TestTypeA(firstly: "firstly2", secondly: "secondly2")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let key3 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId3"
        )
        let payload3 = TestTypeA(firstly: "firstly1A", secondly: "secondly1A")
        let databaseItem3 = StandardTypedDatabaseItem.newItem(withKey: key3, andValue: payload3)

        let key4 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId4"
        )
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

        let key1 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId1"
        )
        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId2"
        )
        let payload2 = TestTypeA(firstly: "firstly2", secondly: "secondly2")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let key3 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId3"
        )
        let payload3 = TestTypeA(firstly: "firstly1A", secondly: "secondly1A")
        let databaseItem3 = StandardTypedDatabaseItem.newItem(withKey: key3, andValue: payload3)

        let key4 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId4"
        )
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

        let key1 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId1"
        )
        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId2"
        )
        let payload2 = TestTypeA(firstly: "firstly2", secondly: "secondly2")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let key3 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId3"
        )
        let payload3 = TestTypeA(firstly: "firstly1A", secondly: "secondly1A")
        let databaseItem3 = StandardTypedDatabaseItem.newItem(withKey: key3, andValue: payload3)

        let key4 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId4"
        )
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

        let key1 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId1"
        )
        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId2"
        )
        let payload2 = TestTypeA(firstly: "firstly2", secondly: "secondly2")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)
        try await table.insertItem(databaseItem2)

        let payload3 = TestTypeA(firstly: "firstly1A", secondly: "secondly1A")
        let databaseItem3 = databaseItem2.createUpdatedItem(withValue: payload3)
        try await table.updateItem(newItem: databaseItem3, existingItem: databaseItem2)

        let key4 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId4"
        )
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

        let key3 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId3"
        )
        let payload3 = TestTypeA(firstly: "firstlyB", secondly: "secondlyB")
        let databaseItem3 = StandardTypedDatabaseItem.newItem(withKey: key3, andValue: payload3)

        let key4 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId4"
        )
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

        let key3 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId3"
        )
        let payload3 = TestTypeA(firstly: "firstlyB", secondly: "secondlyB")
        let databaseItem3 = StandardTypedDatabaseItem.newItem(withKey: key3, andValue: payload3)

        let key4 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId4"
        )
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

        let key3 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId3"
        )
        let payload3 = TestTypeA(firstly: "firstlyB", secondly: "secondlyB")
        let databaseItem3 = StandardTypedDatabaseItem.newItem(withKey: key3, andValue: payload3)

        let key4 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId4"
        )
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
            inputKeys _: [CompositePrimaryKey<some Any>?],
            table _: InMemoryDynamoDBCompositePrimaryKeyTable
        ) async throws -> [DynamoDBTableError] {
            self.errors
        }
    }

    @Test
    func transactWriteWithInjectedErrors() async throws {
        let errors = [DynamoDBTableError.transactionConflict(message: "There is a Conflict!!")]
        let transactionDelegate = TestInMemoryTransactionDelegate(errors: errors)
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable(
            transactionDelegate: transactionDelegate
        )

        let key1 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId1"
        )
        let payload1 = TestTypeA(firstly: "firstly1", secondly: "secondly1")
        let databaseItem1 = StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1)

        let key2 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId2"
        )
        let payload2 = TestTypeA(firstly: "firstly2", secondly: "secondly2")
        let databaseItem2 = StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2)

        let key3 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId3"
        )
        let payload3 = TestTypeA(firstly: "firstly1A", secondly: "secondly1A")
        let databaseItem3 = StandardTypedDatabaseItem.newItem(withKey: key3, andValue: payload3)

        let key4 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId4"
        )
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
        let table: DynamoDBCompositePrimaryKeyTable = InMemoryDynamoDBCompositePrimaryKeyTable(
            transactionDelegate: transactionDelegate
        )

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

        let key3 = StandardCompositePrimaryKey(
            partitionKey: "partitionId1",
            sortKey: "sortId3"
        )
        let payload3 = TestTypeA(firstly: "firstlyB", secondly: "secondlyB")
        let databaseItem3 = StandardTypedDatabaseItem.newItem(withKey: key3, andValue: payload3)

        let key4 = StandardCompositePrimaryKey(
            partitionKey: "partitionId2",
            sortKey: "sortId4"
        )
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
}
