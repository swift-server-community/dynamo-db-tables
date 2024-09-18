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

import AWSDynamoDB
@testable import DynamoDBTables
import Foundation
import Testing

private func createDecoder() -> JSONDecoder {
    let jsonDecoder = JSONDecoder()
    #if os(Linux)
        jsonDecoder.dateDecodingStrategy = .iso8601
    #elseif os(OSX)
        if #available(OSX 10.12, *) {
            jsonDecoder.dateDecodingStrategy = .iso8601
        }
    #endif

    return jsonDecoder
}

private let jsonDecoder = createDecoder()

private func assertNoThrow<T>(_ body: @autoclosure () throws -> T) -> T? {
    do {
        return try body()
    } catch {
        Issue.record("\(error.localizedDescription)")
    }

    return nil
}

struct DynamoDBTablesTests {
    @Test
    func encodeTypedItem() {
        let inputData = serializedTypeADatabaseItem.data(using: .utf8)!

        guard let jsonAttributeValue = try assertNoThrow(
            jsonDecoder.decode(DynamoDBClientTypes.AttributeValue.self, from: inputData))
        else {
            return
        }

        guard let databaseItem: StandardTypedDatabaseItem<TypeA> = try assertNoThrow(
            DynamoDBDecoder().decode(jsonAttributeValue))
        else {
            return
        }

        guard let decodeAttributeValue = try assertNoThrow(
            DynamoDBEncoder().encode(databaseItem))
        else {
            return
        }

        switch (decodeAttributeValue, jsonAttributeValue) {
        case let (.m(left), .m(right)):
            #expect(left.count == right.count)
        default:
            Issue.record()
        }
    }

    @Test
    func encodeTypedItemWithTimeToLive() {
        let inputData = serializedTypeADatabaseItemWithTimeToLive.data(using: .utf8)!

        guard let jsonAttributeValue = try assertNoThrow(
            jsonDecoder.decode(DynamoDBClientTypes.AttributeValue.self, from: inputData))
        else {
            return
        }

        guard let databaseItem: StandardTypedDatabaseItem<TypeA> = try assertNoThrow(
            DynamoDBDecoder().decode(jsonAttributeValue))
        else {
            return
        }

        guard let decodeAttributeValue = try assertNoThrow(
            DynamoDBEncoder().encode(databaseItem))
        else {
            return
        }

        switch (decodeAttributeValue, jsonAttributeValue) {
        case let (.m(left), .m(right)):
            #expect(left.count == right.count)
        default:
            Issue.record()
        }
    }

    @Test
    func typedDatabaseItem() {
        let inputData = serializedTypeADatabaseItem.data(using: .utf8)!

        guard let attributeValue = try assertNoThrow(
            jsonDecoder.decode(DynamoDBClientTypes.AttributeValue.self, from: inputData))
        else {
            return
        }

        guard let databaseItem: StandardTypedDatabaseItem<TypeA> = try assertNoThrow(
            DynamoDBDecoder().decode(attributeValue))
        else {
            return
        }

        #expect(databaseItem.rowValue.firstly == "aaa")
        #expect(databaseItem.rowValue.secondly == "bbb")
        #expect(databaseItem.rowStatus.rowVersion == 5)
        #expect(databaseItem.timeToLive == nil)

        // create an updated item from the decoded one
        let newItem = TypeA(firstly: "hello", secondly: "world!!")
        let updatedItem = databaseItem.createUpdatedItem(withValue: newItem)
        #expect(updatedItem.rowStatus.rowVersion == 6)
    }

    @Test
    func typedDatabaseItemWithTimeToLive() {
        let inputData = serializedTypeADatabaseItemWithTimeToLive.data(using: .utf8)!

        guard let attributeValue = try assertNoThrow(
            jsonDecoder.decode(DynamoDBClientTypes.AttributeValue.self, from: inputData))
        else {
            return
        }

        guard let databaseItem: StandardTypedDatabaseItem<TypeA> = try assertNoThrow(
            DynamoDBDecoder().decode(attributeValue))
        else {
            return
        }

        #expect(databaseItem.rowValue.firstly == "aaa")
        #expect(databaseItem.rowValue.secondly == "bbb")
        #expect(databaseItem.rowStatus.rowVersion == 5)
        #expect(databaseItem.timeToLive?.timeToLiveTimestamp == 123_456_789)

        // create an updated item from the decoded one
        let newItem = TypeA(firstly: "hello", secondly: "world!!")
        let updatedItem = databaseItem.createUpdatedItem(withValue: newItem,
                                                         andTimeToLive: StandardTimeToLive(timeToLiveTimestamp: 234_567_890))
        #expect(updatedItem.rowValue.firstly == "hello")
        #expect(updatedItem.rowValue.secondly == "world!!")
        #expect(updatedItem.rowStatus.rowVersion == 6)
        #expect(updatedItem.timeToLive?.timeToLiveTimestamp == 234_567_890)
    }

    @Test
    func polymorphicDatabaseItemList() {
        let inputData = serializedPolymorphicDatabaseItemList.data(using: .utf8)!

        guard let attributeValues = try assertNoThrow(
            jsonDecoder.decode([DynamoDBClientTypes.AttributeValue].self, from: inputData))
        else {
            return
        }

        let itemsOptional: [ReturnTypeDecodable<AllQueryableTypes>]? = try assertNoThrow(
            attributeValues.map { value in
                try DynamoDBDecoder().decode(value)
            })

        guard let items = itemsOptional else {
            Issue.record("No items returned.")

            return
        }

        #expect(items.count == 2)

        guard case let .typeA(firstDatabaseItem) = items[0].decodedValue else {
            Issue.record("Unexpected type returned")
            return
        }

        guard case let .typeB(secondDatabaseItem) = items[1].decodedValue else {
            Issue.record("Unexpected type returned")
            return
        }

        let first = firstDatabaseItem.rowValue
        let second = secondDatabaseItem.rowValue

        #expect(first.firstly == "aaa")
        #expect(first.secondly == "bbb")
        #expect(firstDatabaseItem.rowStatus.rowVersion == 5)

        #expect(second.thirdly == "ccc")
        #expect(second.fourthly == "ddd")
        #expect(secondDatabaseItem.rowStatus.rowVersion == 12)

        // try to create up updated item with the correct type
        let newItem = TypeA(firstly: "hello", secondly: "world!!")

        let updatedItem = firstDatabaseItem.createUpdatedItem(withValue: newItem)

        #expect(updatedItem.rowStatus.rowVersion == 6)
    }

    @Test
    func polymorphicDatabaseItemListUnknownType() {
        let inputData = serializedPolymorphicDatabaseItemList.data(using: .utf8)!

        guard let attributeValues = try assertNoThrow(
            jsonDecoder.decode([DynamoDBClientTypes.AttributeValue].self, from: inputData))
        else {
            return
        }

        do {
            let _: [ReturnTypeDecodable<SomeQueryableTypes>] = try attributeValues.map { value in
                try DynamoDBDecoder().decode(value)
            }
        } catch let DynamoDBTableError.unexpectedType(provided: provided) {
            #expect(provided == "TypeBCustom")

            return
        } catch {
            Issue.record("Incorrect error thrown.")
        }

        Issue.record("Decoding error expected.")
    }

    @Test
    func polymorphicDatabaseItemListWithIndex() {
        let inputData = serializedPolymorphicDatabaseItemListWithIndex.data(using: .utf8)!

        guard let attributeValues = try assertNoThrow(
            jsonDecoder.decode([DynamoDBClientTypes.AttributeValue].self, from: inputData))
        else {
            return
        }

        let itemsOptional: [ReturnTypeDecodable<AllQueryableTypesWithIndex>]? = try assertNoThrow(
            attributeValues.map { value in
                try DynamoDBDecoder().decode(value)
            })

        guard let items = itemsOptional else {
            Issue.record("No items returned.")

            return
        }

        #expect(items.count == 2)

        guard case let .typeAWithIndex(firstDatabaseItem) = items[0].decodedValue else {
            Issue.record("Unexpected type returned")
            return
        }

        guard case let .typeB(secondDatabaseItem) = items[1].decodedValue else {
            Issue.record("Unexpected type returned")
            return
        }

        let first = firstDatabaseItem.rowValue
        let second = secondDatabaseItem.rowValue

        #expect(first.rowValue.firstly == "aaa")
        #expect(first.rowValue.secondly == "bbb")
        #expect(first.indexValue == "gsi-index")
        #expect(firstDatabaseItem.rowStatus.rowVersion == 5)

        #expect(second.thirdly == "ccc")
        #expect(second.fourthly == "ddd")
        #expect(secondDatabaseItem.rowStatus.rowVersion == 12)

        // try to create up updated item with the correct type
        let newItem = TypeA(firstly: "hello", secondly: "world!!")
        let newRowWithIndex = first.createUpdatedItem(withValue: newItem)

        let updatedItem = firstDatabaseItem.createUpdatedItem(withValue: newRowWithIndex)

        #expect(updatedItem.rowStatus.rowVersion == 6)
    }
}
