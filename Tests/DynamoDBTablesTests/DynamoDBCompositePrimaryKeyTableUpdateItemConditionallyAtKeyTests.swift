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

import XCTest
@testable import DynamoDBTables

class DynamoDBCompositePrimaryKeyTableUpdateItemConditionallyAtKeyTests: XCTestCase {
    
    func updatedPayloadProvider(item: TestTypeA) -> TestTypeA {
        return TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
    }
    
    typealias TestTypeADatabaseItem = StandardTypedDatabaseItem<TestTypeA>
    func updatedItemProvider(item: TestTypeADatabaseItem) -> TestTypeADatabaseItem {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        return TestTypeADatabaseItem.newItem(
            withKey: key,
            andValue: TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2"))
    }
    
    func testUpdateItemConditionallyAtKey() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        try await table.insertItem(databaseItem)
        
        let retrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        _ = try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: updatedPayloadProvider)
        
        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithItemProvider() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        try await table.insertItem(databaseItem)
        
        let retrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        _ = try await table.conditionallyUpdateItem(forKey: key, updatedItemProvider: updatedItemProvider)
        
        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithAcceptableConcurrency() async throws {
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
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        _ = try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: updatedPayloadProvider)
        
        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithAcceptableConcurrencyWithItemProvider() async throws {
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
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        _ = try await table.conditionallyUpdateItem(forKey: key, updatedItemProvider: updatedItemProvider)
        
        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnacceptableConcurrency() async throws {
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
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: updatedPayloadProvider)
            
            XCTFail("Expected concurrency error not thrown.")
        } catch DynamoDBTableError.concurrencyError {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnacceptableConcurrencyWithItemProvider() async throws {
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
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedItemProvider: updatedItemProvider)
            
            XCTFail("Expected concurrency error not thrown.")
        } catch DynamoDBTableError.concurrencyError {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnacceptableConcurrencyWithPayloadProvider() async throws {
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
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: updatedPayloadProvider)
            
            XCTFail("Expected concurrency error not thrown.")
        } catch DynamoDBTableError.concurrencyError {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    enum TestError: Error {
        case everythingIsWrong
    }
    
    func testUpdateItemConditionallyAtKeyWithFailingUpdate() async throws {
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
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        var passCount = 0
        
        func failingUpdatedPayloadProvider(item: TestTypeA) throws -> TestTypeA {
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
            
            XCTFail("Expected everythingIsWrong error not thrown.")
        } catch TestError.everythingIsWrong {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithFailingUpdateWithItemProvider() async throws {
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
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        var passCount = 0
        
        func failingUpdatedItemProvider(item: TestTypeADatabaseItem) throws -> TestTypeADatabaseItem {
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
            
            XCTFail("Expected everythingIsWrong error not thrown.")
        } catch TestError.everythingIsWrong {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem = try await table.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnknownItem() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        
        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: updatedPayloadProvider)
            
            XCTFail("Expected concurrency error not thrown.")
        } catch DynamoDBTableError.conditionalCheckFailed {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem? = try await table.getItem(forKey: key)
        
        XCTAssertNil(secondRetrievedItem)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnknownItemWithItemProvider() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        
        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedItemProvider: updatedItemProvider)
            
            XCTFail("Expected concurrency error not thrown.")
        } catch DynamoDBTableError.conditionalCheckFailed {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem? = try await table.getItem(forKey: key)
        
        XCTAssertNil(secondRetrievedItem)
    }
}
