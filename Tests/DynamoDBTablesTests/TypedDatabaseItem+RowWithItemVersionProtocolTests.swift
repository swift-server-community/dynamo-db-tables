//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Tests/SmokeDynamoDBTests/TypedDatabaseItem+RowWithItemVersionProtocolTests.swift
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
//  TypedDatabaseItem+RowWithItemVersionProtocolTests.swift
//  DynamoDBTablesTests
//

import Foundation

@testable import DynamoDBTables
import XCTest

private let ORIGINAL_PAYLOAD = "Payload"
private let ORIGINAL_TIME_TO_LIVE: Int64 = 123_456_789
private let UPDATED_PAYLOAD = "Updated"
private let UPDATED_TIME_TO_LIVE: Int64 = 234_567_890

class TypedDatabaseItemRowWithItemVersionProtocolTests: XCTestCase {
    func testCreateUpdatedRowWithItemVersion() throws {
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: rowWithItemVersion)

        let updatedItem = try databaseItem.createUpdatedRowWithItemVersion(withValue: "Updated",
                                                                           conditionalStatusVersion: nil)

        XCTAssertEqual(1, databaseItem.rowStatus.rowVersion)
        XCTAssertEqual(1, databaseItem.rowValue.itemVersion)
        XCTAssertEqual(ORIGINAL_PAYLOAD, databaseItem.rowValue.rowValue)
        XCTAssertNil(databaseItem.timeToLive)
        XCTAssertEqual(2, updatedItem.rowStatus.rowVersion)
        XCTAssertEqual(2, updatedItem.rowValue.itemVersion)
        XCTAssertEqual(UPDATED_PAYLOAD, updatedItem.rowValue.rowValue)
        XCTAssertNil(updatedItem.timeToLive)
    }

    func testCreateUpdatedRowWithItemVersionWithTimeToLive() throws {
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = TypedDatabaseItem.newItem(withKey: compositeKey,
                                                     andValue: rowWithItemVersion,
                                                     andTimeToLive: StandardTimeToLive(timeToLiveTimestamp: 123_456_789))

        let updatedItem = try databaseItem.createUpdatedRowWithItemVersion(withValue: "Updated",
                                                                           conditionalStatusVersion: nil,
                                                                           andTimeToLive: StandardTimeToLive(timeToLiveTimestamp: 234_567_890))

        XCTAssertEqual(1, databaseItem.rowStatus.rowVersion)
        XCTAssertEqual(1, databaseItem.rowValue.itemVersion)
        XCTAssertEqual(ORIGINAL_PAYLOAD, databaseItem.rowValue.rowValue)
        XCTAssertEqual(ORIGINAL_TIME_TO_LIVE, databaseItem.timeToLive?.timeToLiveTimestamp)
        XCTAssertEqual(2, updatedItem.rowStatus.rowVersion)
        XCTAssertEqual(2, updatedItem.rowValue.itemVersion)
        XCTAssertEqual(UPDATED_PAYLOAD, updatedItem.rowValue.rowValue)
        XCTAssertEqual(UPDATED_TIME_TO_LIVE, updatedItem.timeToLive?.timeToLiveTimestamp)
    }

    func testCreateUpdatedRowWithItemVersionWithCorrectConditionalVersion() throws {
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: rowWithItemVersion)

        let updatedItem = try databaseItem.createUpdatedRowWithItemVersion(withValue: "Updated",
                                                                           conditionalStatusVersion: 1)

        XCTAssertEqual(1, databaseItem.rowStatus.rowVersion)
        XCTAssertEqual(1, databaseItem.rowValue.itemVersion)
        XCTAssertEqual(ORIGINAL_PAYLOAD, databaseItem.rowValue.rowValue)
        XCTAssertEqual(2, updatedItem.rowStatus.rowVersion)
        XCTAssertEqual(2, updatedItem.rowValue.itemVersion)
        XCTAssertEqual(UPDATED_PAYLOAD, updatedItem.rowValue.rowValue)
    }

    func testCreateUpdatedRowWithItemVersionWithIncorrectConditionalVersion() {
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: rowWithItemVersion)

        do {
            _ = try databaseItem.createUpdatedRowWithItemVersion(withValue: "Updated",
                                                                 conditionalStatusVersion: 8)

            XCTFail("Expected error not thrown.")
        } catch DynamoDBTableError.concurrencyError {
            return
        } catch {
            XCTFail("Unexpected error thrown: '\(error)'.")
        }
    }

    func testStringFieldDifference() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "eigthly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap

        XCTAssertEqual(pathMap["\"theString\""], .update(path: "\"theString\"", value: "'eigthly'"))
        XCTAssertEqual(pathMap["\"RowVersion\""], .update(path: "\"RowVersion\"", value: "2"))
    }

    func testStringFieldDifferenceWithEscapedQuotes() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "f'ir''st'''ly", theNumber: 4, theStruct: theStruct, theList: ["t'hi''rdly", "fourt''hl'y"])
        let payloadB = TestTypeC(theString: "e'ig''thl'''y", theNumber: 4, theStruct: theStruct, theList: ["t'hi''rdly", "fourt''hl'y"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "par'titio''nKey",
                                                       sortKey: "so'rt'''Key")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable(escapeSingleQuoteInPartiQL: true)

        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap

        XCTAssertEqual(pathMap["\"theString\""], .update(path: "\"theString\"", value: "'e''ig''''thl''''''y'"))
        XCTAssertEqual(pathMap["\"RowVersion\""], .update(path: "\"RowVersion\"", value: "2"))
    }

    func testNumberFieldDifference() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 12, theStruct: theStruct, theList: ["thirdly", "fourthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap

        XCTAssertEqual(pathMap["\"theNumber\""], .update(path: "\"theNumber\"", value: "12"))
        XCTAssertEqual(pathMap["\"RowVersion\""], .update(path: "\"RowVersion\"", value: "2"))
    }

    func testStructFieldDifference() throws {
        let theStructA = TestTypeA(firstly: "firstly", secondly: "secondly")
        let theStructB = TestTypeA(firstly: "eigthly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStructA, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStructB, theList: ["thirdly", "fourthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap

        XCTAssertEqual(pathMap["\"theStruct\".\"firstly\""], .update(path: "\"theStruct\".\"firstly\"", value: "'eigthly'"))
        XCTAssertEqual(pathMap["\"RowVersion\""], .update(path: "\"RowVersion\"", value: "2"))
    }

    func testStructFieldDifferenceWithEscapedQuotes() throws {
        let theStructA = TestTypeA(firstly: "f'ir''st'''ly", secondly: "se'con''dl'''y")
        let theStructB = TestTypeA(firstly: "e'ig''thly'''", secondly: "se'con''dl'''y")
        let payloadA = TestTypeC(theString: "fi''rst'ly", theNumber: 4, theStruct: theStructA, theList: ["t'hi'rdl'y", "f'ou'rthl'y"])
        let payloadB = TestTypeC(theString: "fi''rstl'y", theNumber: 4, theStruct: theStructB, theList: ["t'hi'rdl'y", "f'ou'rthl'y"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "par'titio''nKey",
                                                       sortKey: "so'rt'''Key")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable(escapeSingleQuoteInPartiQL: true)

        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap

        XCTAssertEqual(pathMap["\"theStruct\".\"firstly\""], .update(path: "\"theStruct\".\"firstly\"", value: "'e''ig''''thly'''''''"))
        XCTAssertEqual(pathMap["\"RowVersion\""], .update(path: "\"RowVersion\"", value: "2"))
    }

    func testListFieldDifference() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "eigthly", "ninthly", "tenthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap

        XCTAssertEqual(pathMap["\"theList\"[1]"], .update(path: "\"theList\"[1]", value: "'eigthly'"))
        XCTAssertEqual(pathMap["\"theList\"[2]"], .update(path: "\"theList\"[2]", value: "'ninthly'"))
        XCTAssertEqual(pathMap["\"theList\"[3]"], .update(path: "\"theList\"[3]", value: "'tenthly'"))
        XCTAssertEqual(pathMap["\"RowVersion\""], .update(path: "\"RowVersion\"", value: "2"))
    }

    func testNestedListFieldDifference() throws {
        struct NestedLists: Codable {
            let nestedList: [[String]]
        }

        let payloadA = NestedLists(nestedList: [["one", "two"], ["three", "four"]])
        let payloadB = NestedLists(nestedList: [["one", "five"], ["three", "four", "eight"], ["six", "seven"]])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap

        XCTAssertEqual(pathMap["\"nestedList\"[0][1]"], .update(path: "\"nestedList\"[0][1]", value: "'five'"))
        XCTAssertEqual(pathMap["\"nestedList\"[1][2]"], .update(path: "\"nestedList\"[1][2]", value: "'eight'"))
        XCTAssertEqual(pathMap["\"nestedList\"[2]"], .update(path: "\"nestedList\"[2]", value: "['six', 'seven']"))
        XCTAssertEqual(pathMap["\"RowVersion\""], .update(path: "\"RowVersion\"", value: "2"))
    }

    func testListFieldDifferenceWithEscapedQuotes() throws {
        let theStruct = TestTypeA(firstly: "f'irstl''y", secondly: "se'cond''ly")
        let payloadA = TestTypeC(theString: "f'irst''ly", theNumber: 4, theStruct: theStruct, theList: ["th'irdly",
                                                                                                        "fo''urthly"])
        let payloadB = TestTypeC(theString: "f'irst''ly", theNumber: 4, theStruct: theStruct, theList: ["th'irdly",
                                                                                                        "eigth'''ly",
                                                                                                        "ni'n''thly",
                                                                                                        "ten'thly'"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "par'titio''nKey",
                                                       sortKey: "so'rt'''Key")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable(escapeSingleQuoteInPartiQL: true)

        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap

        XCTAssertEqual(pathMap["\"theList\"[1]"], .update(path: "\"theList\"[1]", value: "'eigth''''''ly'"))
        XCTAssertEqual(pathMap["\"theList\"[2]"], .update(path: "\"theList\"[2]", value: "'ni''n''''thly'"))
        XCTAssertEqual(pathMap["\"theList\"[3]"], .update(path: "\"theList\"[3]", value: "'ten''thly'''"))
        XCTAssertEqual(pathMap["\"RowVersion\""], .update(path: "\"RowVersion\"", value: "2"))
    }

    func testStringFieldAddition() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: nil, theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "eigthly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap

        XCTAssertEqual(pathMap["\"theString\""], .update(path: "\"theString\"", value: "'eigthly'"))
        XCTAssertEqual(pathMap["\"RowVersion\""], .update(path: "\"RowVersion\"", value: "2"))
    }

    func testNumberFieldAddition() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: nil, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 12, theStruct: theStruct, theList: ["thirdly", "fourthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap

        XCTAssertEqual(pathMap["\"theNumber\""], .update(path: "\"theNumber\"", value: "12"))
        XCTAssertEqual(pathMap["\"RowVersion\""], .update(path: "\"RowVersion\"", value: "2"))
    }

    func testStructFieldAddition() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: nil, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap

        guard case let .update(_, value) = pathMap["\"theStruct\""] else {
            XCTFail()
            return
        }

        let valueMatches = (value == "{'firstly': 'firstly', 'secondly': 'secondly'}") ||
            (value == "{'secondly': 'secondly', 'firstly': 'firstly'}")

        XCTAssertTrue(valueMatches)
        XCTAssertEqual(pathMap["\"RowVersion\""], .update(path: "\"RowVersion\"", value: "2"))
    }

    func testListFieldAddition() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: nil)
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "eigthly", "ninthly", "tenthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap

        XCTAssertEqual(pathMap["\"theList\""], .update(path: "\"theList\"", value: "['thirdly', 'eigthly', 'ninthly', 'tenthly']"))
        XCTAssertEqual(pathMap["\"RowVersion\""], .update(path: "\"RowVersion\"", value: "2"))
    }

    func testStringFieldRemoval() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: nil, theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap

        XCTAssertEqual(pathMap["\"theString\""], .remove(path: "\"theString\""))
        XCTAssertEqual(pathMap["\"RowVersion\""], .update(path: "\"RowVersion\"", value: "2"))
    }

    func testNumberFieldRemoval() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: nil, theStruct: theStruct, theList: ["thirdly", "fourthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap

        XCTAssertEqual(pathMap["\"theNumber\""], .remove(path: "\"theNumber\""))
        XCTAssertEqual(pathMap["\"RowVersion\""], .update(path: "\"RowVersion\"", value: "2"))
    }

    func testStructFieldRemoval() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: nil, theList: ["thirdly", "fourthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap

        XCTAssertEqual(pathMap["\"theStruct\""], .remove(path: "\"theStruct\""))
        XCTAssertEqual(pathMap["\"RowVersion\""], .update(path: "\"RowVersion\"", value: "2"))
    }

    func testListFieldRemoval() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: nil)

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap

        XCTAssertEqual(pathMap["\"theList\""], .remove(path: "\"theList\""))
        XCTAssertEqual(pathMap["\"RowVersion\""], .update(path: "\"RowVersion\"", value: "2"))
    }

    func testListFieldDifferenceExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "eigthly", "ninthly", "tenthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let expression = try table.getUpdateExpression(tableName: tableName,
                                                       newItem: databaseItemB,
                                                       existingItem: databaseItemA)
        XCTAssertEqual(expression, "UPDATE \"TableName\" "
            + "SET \"theList\"[1]='eigthly' "
            + "SET \"theList\"[2]='ninthly' "
            + "SET \"theList\"[3]='tenthly' "
            + "WHERE PK='partitionKey' AND SK='sortKey' "
            + "AND RowVersion=1")
    }

    func testListFieldDifferenceExpressionWithEscapedQuotes() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "f'irstly", secondly: "s'econdly")
        let payloadA = TestTypeC(theString: "fi''rstly", theNumber: 4, theStruct: theStruct,
                                 theList: ["thirdl'y", "fourt''hly"])
        let payloadB = TestTypeC(theString: "fi''rstly", theNumber: 4, theStruct: theStruct,
                                 theList: ["thirdl'y", "eigth''ly", "n'inthly", "tenth'''ly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "pa'rtition''Key",
                                                       sortKey: "so'rt''Key")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable(escapeSingleQuoteInPartiQL: true)

        let expression = try table.getUpdateExpression(tableName: tableName,
                                                       newItem: databaseItemB,
                                                       existingItem: databaseItemA)
        XCTAssertEqual(expression, "UPDATE \"TableName\" "
            + "SET \"theList\"[1]='eigth''''ly' "
            + "SET \"theList\"[2]='n''inthly' "
            + "SET \"theList\"[3]='tenth''''''ly' "
            + "WHERE PK='pa''rtition''''Key' AND SK='so''rt''''Key' "
            + "AND RowVersion=1")
    }

    func testListFieldAdditionExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: nil)
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "eigthly", "ninthly", "tenthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let expression = try table.getUpdateExpression(tableName: tableName,
                                                       newItem: databaseItemB,
                                                       existingItem: databaseItemA)
        XCTAssertEqual(expression, "UPDATE \"TableName\" "
            + "SET \"theList\"=['thirdly', 'eigthly', 'ninthly', 'tenthly'] "
            + "WHERE PK='partitionKey' AND SK='sortKey' "
            + "AND RowVersion=1")
    }

    func testListFieldRemovalExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: nil)

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadB)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let expression = try table.getUpdateExpression(tableName: tableName,
                                                       newItem: databaseItemB,
                                                       existingItem: databaseItemA)
        XCTAssertEqual(expression, "UPDATE \"TableName\" "
            + "REMOVE \"theList\" "
            + "WHERE PK='partitionKey' AND SK='sortKey' "
            + "AND RowVersion=1")
    }

    func testDeleteItemExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let expression = try table.getDeleteExpression(tableName: tableName,
                                                       existingItem: databaseItemA)
        XCTAssertEqual(expression, "DELETE FROM \"TableName\" "
            + "WHERE PK='partitionKey' AND SK='sortKey' "
            + "AND RowVersion=1")
    }

    func testDeleteItemExpressionWithEscapedQuotes() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "fir'stly", secondly: "secondl'y")
        let payloadA = TestTypeC(theString: "f'irstly", theNumber: 4, theStruct: theStruct, theList: ["third''ly", "fou'''rthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "p'artitionKe''y",
                                                       sortKey: "sort'''Key")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable(escapeSingleQuoteInPartiQL: true)

        let expression = try table.getDeleteExpression(tableName: tableName,
                                                       existingItem: databaseItemA)
        XCTAssertEqual(expression, "DELETE FROM \"TableName\" "
            + "WHERE PK='p''artitionKe''''y' AND SK='sort''''''Key' "
            + "AND RowVersion=1")
    }

    func testDeleteKeyExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let expression = try table.getDeleteExpression(tableName: tableName,
                                                       existingItem: databaseItemA)
        XCTAssertEqual(expression, "DELETE FROM \"TableName\" "
            + "WHERE PK='partitionKey' AND SK='sortKey' "
            + "AND RowVersion=1")
    }
}

extension [AttributeDifference] {
    var pathMap: [String: AttributeDifference] {
        var newPathMap: [String: AttributeDifference] = [:]
        for attributeDifference in self {
            newPathMap[attributeDifference.path] = attributeDifference
        }

        return newPathMap
    }
}
