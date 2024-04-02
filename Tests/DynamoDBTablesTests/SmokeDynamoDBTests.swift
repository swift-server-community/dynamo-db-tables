//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Tests/SmokeDynamoDBTests/SmokeDynamoDBTests.swift
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
// DynamoDBTablesTests.swift
// DynamoDBTablesTests
//

import XCTest
@testable import DynamoDBTables
import AWSDynamoDB

fileprivate let dynamodbEncoder = DynamoDBEncoder()
fileprivate let dynamodbDecoder = DynamoDBDecoder()

fileprivate func createDecoder() -> JSONDecoder {
    let jsonDecoder = JSONDecoder()
    #if os (Linux)
        jsonDecoder.dateDecodingStrategy = .iso8601
    #elseif os (OSX)
        if #available(OSX 10.12, *) {
            jsonDecoder.dateDecodingStrategy = .iso8601
        }
    #endif
    
    return jsonDecoder
}

fileprivate let jsonDecoder = createDecoder()

fileprivate func assertNoThrow<T>(_ body: @autoclosure () throws -> T) -> T? {
    do {
        return try body()
    } catch {
        XCTFail(error.localizedDescription)
    }
    
    return nil
}

class DynamoDBTablesTests: XCTestCase {
    
    func testEncodeTypedItem() {
        let inputData = serializedTypeADatabaseItem.data(using: .utf8)!
        
        guard let jsonAttributeValue = assertNoThrow(
            try jsonDecoder.decode(DynamoDBClientTypes.AttributeValue.self, from: inputData)) else {
                return
        }
        
        guard let databaseItem: StandardTypedDatabaseItem<TypeA> = assertNoThrow(
            try dynamodbDecoder.decode(jsonAttributeValue)) else {
                return
        }
        
        guard let decodeAttributeValue = assertNoThrow(
            try dynamodbEncoder.encode(databaseItem)) else {
                return
        }
        
        switch (decodeAttributeValue, jsonAttributeValue) {
        case (.m(let left), .m(let right)):
            XCTAssertEqual(left.count, right.count)
        default:
            XCTFail()
        }
    }
    
    func testEncodeTypedItemWithTimeToLive() {
        let inputData = serializedTypeADatabaseItemWithTimeToLive.data(using: .utf8)!
        
        guard let jsonAttributeValue = assertNoThrow(
            try jsonDecoder.decode(DynamoDBClientTypes.AttributeValue.self, from: inputData)) else {
            return
        }
        
        guard let databaseItem: StandardTypedDatabaseItem<TypeA> = assertNoThrow(
            try dynamodbDecoder.decode(jsonAttributeValue)) else {
            return
        }
        
        guard let decodeAttributeValue = assertNoThrow(
            try dynamodbEncoder.encode(databaseItem)) else {
            return
        }
        
        switch (decodeAttributeValue, jsonAttributeValue) {
        case (.m(let left), .m(let right)):
            XCTAssertEqual(left.count, right.count)
        default:
            XCTFail()
        }
    }

    func testTypedDatabaseItem() {
        let inputData = serializedTypeADatabaseItem.data(using: .utf8)!
        
        guard let attributeValue = assertNoThrow(
                try jsonDecoder.decode(DynamoDBClientTypes.AttributeValue.self, from: inputData)) else {
            return
        }
        
        guard let databaseItem: StandardTypedDatabaseItem<TypeA> = assertNoThrow(
                try dynamodbDecoder.decode(attributeValue)) else {
            return
        }
        
        XCTAssertEqual(databaseItem.rowValue.firstly, "aaa")
        XCTAssertEqual(databaseItem.rowValue.secondly, "bbb")
        XCTAssertEqual(databaseItem.rowStatus.rowVersion, 5)
        XCTAssertNil(databaseItem.timeToLive)
        
        // create an updated item from the decoded one
        let newItem = TypeA(firstly: "hello", secondly: "world!!")
        let updatedItem = databaseItem.createUpdatedItem(withValue: newItem)
        XCTAssertEqual(updatedItem.rowStatus.rowVersion, 6)
    }
    
    func testTypedDatabaseItemWithTimeToLive() {
        let inputData = serializedTypeADatabaseItemWithTimeToLive.data(using: .utf8)!
        
        guard let attributeValue = assertNoThrow(
            try jsonDecoder.decode(DynamoDBClientTypes.AttributeValue.self, from: inputData)) else {
            return
        }
        
        guard let databaseItem: StandardTypedDatabaseItem<TypeA> = assertNoThrow(
            try dynamodbDecoder.decode(attributeValue)) else {
            return
        }
        
        XCTAssertEqual(databaseItem.rowValue.firstly, "aaa")
        XCTAssertEqual(databaseItem.rowValue.secondly, "bbb")
        XCTAssertEqual(databaseItem.rowStatus.rowVersion, 5)
        XCTAssertEqual(databaseItem.timeToLive?.timeToLiveTimestamp, 123456789)
        
        // create an updated item from the decoded one
        let newItem = TypeA(firstly: "hello", secondly: "world!!")
        let updatedItem = databaseItem.createUpdatedItem(withValue: newItem,
                                                         andTimeToLive: StandardTimeToLive(timeToLiveTimestamp: 234567890))
        XCTAssertEqual(updatedItem.rowValue.firstly, "hello")
        XCTAssertEqual(updatedItem.rowValue.secondly, "world!!")
        XCTAssertEqual(updatedItem.rowStatus.rowVersion, 6)
        XCTAssertEqual(updatedItem.timeToLive?.timeToLiveTimestamp, 234567890)
    }
  
    func testPolymorphicDatabaseItemList() {
        let inputData = serializedPolymorphicDatabaseItemList.data(using: .utf8)!
        
        guard let attributeValues = assertNoThrow(
                try jsonDecoder.decode([DynamoDBClientTypes.AttributeValue].self, from: inputData)) else {
            return
        }
        
        let itemsOptional: [ReturnTypeDecodable<AllQueryableTypes>]? = assertNoThrow(
            try attributeValues.map { value in
            return try dynamodbDecoder.decode(value)
        })
        
        guard let items = itemsOptional else {
            XCTFail("No items returned.")
            
            return
        }
        
        XCTAssertEqual(items.count, 2)
        
        guard case let .typeA(firstDatabaseItem) = items[0].decodedValue else {
            XCTFail("Unexpected type returned")
            return
        }
        
        guard case let .typeB(secondDatabaseItem) = items[1].decodedValue else {
            XCTFail("Unexpected type returned")
            return
        }
        
        let first = firstDatabaseItem.rowValue
        let second = secondDatabaseItem.rowValue
        
        XCTAssertEqual(first.firstly, "aaa")
        XCTAssertEqual(first.secondly, "bbb")
        XCTAssertEqual(firstDatabaseItem.rowStatus.rowVersion, 5)
        
        XCTAssertEqual(second.thirdly, "ccc")
        XCTAssertEqual(second.fourthly, "ddd")
        XCTAssertEqual(secondDatabaseItem.rowStatus.rowVersion, 12)
        
        // try to create up updated item with the correct type
        let newItem = TypeA(firstly: "hello", secondly: "world!!")
        
        let updatedItem = firstDatabaseItem.createUpdatedItem(withValue: newItem)
        
        XCTAssertEqual(updatedItem.rowStatus.rowVersion, 6)
    }
    
    func testPolymorphicDatabaseItemListUnknownType() {
        let inputData = serializedPolymorphicDatabaseItemList.data(using: .utf8)!
        
        guard let attributeValues = assertNoThrow(
                try jsonDecoder.decode([DynamoDBClientTypes.AttributeValue].self, from: inputData)) else {
            return
        }
        
        do {
            let _: [ReturnTypeDecodable<SomeQueryableTypes>] = try attributeValues.map { value in
                return try dynamodbDecoder.decode(value)
            }
        } catch DynamoDBTableError.unexpectedType(provided: let provided) {
            XCTAssertEqual(provided, "TypeBCustom")
            
            return
        } catch {
            XCTFail("Incorrect error thrown.")
        }
        
        XCTFail("Decoding error expected.")
    }
    
    func testPolymorphicDatabaseItemListWithIndex() {
        let inputData = serializedPolymorphicDatabaseItemListWithIndex.data(using: .utf8)!
        
        guard let attributeValues = assertNoThrow(
                try jsonDecoder.decode([DynamoDBClientTypes.AttributeValue].self, from: inputData)) else {
            return
        }
        
        let itemsOptional: [ReturnTypeDecodable<AllQueryableTypesWithIndex>]? = assertNoThrow(
            try attributeValues.map { value in
            return try dynamodbDecoder.decode(value)
        })
        
        guard let items = itemsOptional else {
            XCTFail("No items returned.")
            
            return
        }
        
        XCTAssertEqual(items.count, 2)
        
        guard case let .typeAWithIndex(firstDatabaseItem) = items[0].decodedValue else {
            XCTFail("Unexpected type returned")
            return
        }
        
        guard case let .typeB(secondDatabaseItem) = items[1].decodedValue else {
            XCTFail("Unexpected type returned")
            return
        }
        
        let first = firstDatabaseItem.rowValue
        let second = secondDatabaseItem.rowValue
        
        XCTAssertEqual(first.rowValue.firstly, "aaa")
        XCTAssertEqual(first.rowValue.secondly, "bbb")
        XCTAssertEqual(first.indexValue, "gsi-index")
        XCTAssertEqual(firstDatabaseItem.rowStatus.rowVersion, 5)
        
        XCTAssertEqual(second.thirdly, "ccc")
        XCTAssertEqual(second.fourthly, "ddd")
        XCTAssertEqual(secondDatabaseItem.rowStatus.rowVersion, 12)
        
        // try to create up updated item with the correct type
        let newItem = TypeA(firstly: "hello", secondly: "world!!")
        let newRowWithIndex = first.createUpdatedItem(withValue: newItem)
        
        let updatedItem = firstDatabaseItem.createUpdatedItem(withValue: newRowWithIndex)
        
        XCTAssertEqual(updatedItem.rowStatus.rowVersion, 6)
    }
}
