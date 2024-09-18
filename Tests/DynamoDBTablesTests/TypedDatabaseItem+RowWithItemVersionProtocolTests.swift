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
import Testing

private let ORIGINAL_PAYLOAD = "Payload"
private let ORIGINAL_TIME_TO_LIVE: Int64 = 123_456_789
private let UPDATED_PAYLOAD = "Updated"
private let UPDATED_TIME_TO_LIVE: Int64 = 234_567_890

struct TypedDatabaseItemRowWithItemVersionProtocolTests {
    @Test
    func createUpdatedRowWithItemVersion() throws {
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: rowWithItemVersion)

        let updatedItem = try databaseItem.createUpdatedRowWithItemVersion(withValue: "Updated",
                                                                           conditionalStatusVersion: nil)

        #expect(1 == databaseItem.rowStatus.rowVersion)
        #expect(1 == databaseItem.rowValue.itemVersion)
        #expect(ORIGINAL_PAYLOAD == databaseItem.rowValue.rowValue)
        #expect(databaseItem.timeToLive == nil)
        #expect(2 == updatedItem.rowStatus.rowVersion)
        #expect(2 == updatedItem.rowValue.itemVersion)
        #expect(UPDATED_PAYLOAD == updatedItem.rowValue.rowValue)
        #expect(updatedItem.timeToLive == nil)
    }

    @Test
    func createUpdatedRowWithItemVersionWithTimeToLive() throws {
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = TypedDatabaseItem.newItem(withKey: compositeKey,
                                                     andValue: rowWithItemVersion,
                                                     andTimeToLive: StandardTimeToLive(timeToLiveTimestamp: 123_456_789))

        let updatedItem = try databaseItem.createUpdatedRowWithItemVersion(withValue: "Updated",
                                                                           conditionalStatusVersion: nil,
                                                                           andTimeToLive: StandardTimeToLive(timeToLiveTimestamp: 234_567_890))

        #expect(1 == databaseItem.rowStatus.rowVersion)
        #expect(1 == databaseItem.rowValue.itemVersion)
        #expect(ORIGINAL_PAYLOAD == databaseItem.rowValue.rowValue)
        #expect(ORIGINAL_TIME_TO_LIVE == databaseItem.timeToLive?.timeToLiveTimestamp)
        #expect(2 == updatedItem.rowStatus.rowVersion)
        #expect(2 == updatedItem.rowValue.itemVersion)
        #expect(UPDATED_PAYLOAD == updatedItem.rowValue.rowValue)
        #expect(UPDATED_TIME_TO_LIVE == updatedItem.timeToLive?.timeToLiveTimestamp)
    }

    @Test
    func createUpdatedRowWithItemVersionWithCorrectConditionalVersion() throws {
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: rowWithItemVersion)

        let updatedItem = try databaseItem.createUpdatedRowWithItemVersion(withValue: "Updated",
                                                                           conditionalStatusVersion: 1)

        #expect(1 == databaseItem.rowStatus.rowVersion)
        #expect(1 == databaseItem.rowValue.itemVersion)
        #expect(ORIGINAL_PAYLOAD == databaseItem.rowValue.rowValue)
        #expect(2 == updatedItem.rowStatus.rowVersion)
        #expect(2 == updatedItem.rowValue.itemVersion)
        #expect(UPDATED_PAYLOAD == updatedItem.rowValue.rowValue)
    }

    @Test
    func createUpdatedRowWithItemVersionWithIncorrectConditionalVersion() {
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: rowWithItemVersion)

        do {
            _ = try databaseItem.createUpdatedRowWithItemVersion(withValue: "Updated",
                                                                 conditionalStatusVersion: 8)

            Issue.record("Expected error not thrown.")
        } catch DynamoDBTableError.concurrencyError {
            return
        } catch {
            Issue.record("Unexpected error thrown: '\(error)'.")
        }
    }

    @Test
    func stringFieldDifference() throws {
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

        #expect(pathMap["\"theString\""] == .update(path: "\"theString\"", value: "'eigthly'"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func stringFieldDifferenceWithEscapedQuotes() throws {
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

        #expect(pathMap["\"theString\""] == .update(path: "\"theString\"", value: "'e''ig''''thl''''''y'"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func numberFieldDifference() throws {
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

        #expect(pathMap["\"theNumber\""] == .update(path: "\"theNumber\"", value: "12"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func structFieldDifference() throws {
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

        #expect(pathMap["\"theStruct\".\"firstly\""] == .update(path: "\"theStruct\".\"firstly\"", value: "'eigthly'"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func structFieldDifferenceWithEscapedQuotes() throws {
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

        #expect(pathMap["\"theStruct\".\"firstly\""] == .update(path: "\"theStruct\".\"firstly\"", value: "'e''ig''''thly'''''''"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func listFieldDifference() throws {
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

        #expect(pathMap["\"theList\"[1]"] == .update(path: "\"theList\"[1]", value: "'eigthly'"))
        #expect(pathMap["\"theList\"[2]"] == .update(path: "\"theList\"[2]", value: "'ninthly'"))
        #expect(pathMap["\"theList\"[3]"] == .update(path: "\"theList\"[3]", value: "'tenthly'"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func nestedListFieldDifference() throws {
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

        #expect(pathMap["\"nestedList\"[0][1]"] == .update(path: "\"nestedList\"[0][1]", value: "'five'"))
        #expect(pathMap["\"nestedList\"[1][2]"] == .update(path: "\"nestedList\"[1][2]", value: "'eight'"))
        #expect(pathMap["\"nestedList\"[2]"] == .update(path: "\"nestedList\"[2]", value: "['six', 'seven']"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func listFieldDifferenceWithEscapedQuotes() throws {
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

        #expect(pathMap["\"theList\"[1]"] == .update(path: "\"theList\"[1]", value: "'eigth''''''ly'"))
        #expect(pathMap["\"theList\"[2]"] == .update(path: "\"theList\"[2]", value: "'ni''n''''thly'"))
        #expect(pathMap["\"theList\"[3]"] == .update(path: "\"theList\"[3]", value: "'ten''thly'''"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func stringFieldAddition() throws {
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

        #expect(pathMap["\"theString\""] == .update(path: "\"theString\"", value: "'eigthly'"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func numberFieldAddition() throws {
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

        #expect(pathMap["\"theNumber\""] == .update(path: "\"theNumber\"", value: "12"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func structFieldAddition() throws {
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
            Issue.record("Value not in path map")
            return
        }

        let valueMatches = (value == "{'firstly': 'firstly', 'secondly': 'secondly'}") ||
            (value == "{'secondly': 'secondly', 'firstly': 'firstly'}")

        #expect(valueMatches)
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func listFieldAddition() throws {
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

        #expect(pathMap["\"theList\""] == .update(path: "\"theList\"", value: "['thirdly', 'eigthly', 'ninthly', 'tenthly']"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func stringFieldRemoval() throws {
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

        #expect(pathMap["\"theString\""] == .remove(path: "\"theString\""))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func numberFieldRemoval() throws {
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

        #expect(pathMap["\"theNumber\""] == .remove(path: "\"theNumber\""))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func structFieldRemoval() throws {
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

        #expect(pathMap["\"theStruct\""] == .remove(path: "\"theStruct\""))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func listFieldRemoval() throws {
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

        #expect(pathMap["\"theList\""] == .remove(path: "\"theList\""))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func listFieldDifferenceExpression() throws {
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
        #expect(expression == "UPDATE \"TableName\" "
            + "SET \"theList\"[1]='eigthly' "
            + "SET \"theList\"[2]='ninthly' "
            + "SET \"theList\"[3]='tenthly' "
            + "WHERE PK='partitionKey' AND SK='sortKey' "
            + "AND RowVersion=1")
    }

    @Test
    func listFieldDifferenceExpressionWithEscapedQuotes() throws {
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
        #expect(expression == "UPDATE \"TableName\" "
            + "SET \"theList\"[1]='eigth''''ly' "
            + "SET \"theList\"[2]='n''inthly' "
            + "SET \"theList\"[3]='tenth''''''ly' "
            + "WHERE PK='pa''rtition''''Key' AND SK='so''rt''''Key' "
            + "AND RowVersion=1")
    }

    @Test
    func listFieldAdditionExpression() throws {
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
        #expect(expression == "UPDATE \"TableName\" "
            + "SET \"theList\"=['thirdly', 'eigthly', 'ninthly', 'tenthly'] "
            + "WHERE PK='partitionKey' AND SK='sortKey' "
            + "AND RowVersion=1")
    }

    @Test
    func listFieldRemovalExpression() throws {
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
        #expect(expression == "UPDATE \"TableName\" "
            + "REMOVE \"theList\" "
            + "WHERE PK='partitionKey' AND SK='sortKey' "
            + "AND RowVersion=1")
    }

    @Test
    func deleteItemExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let expression = try table.getDeleteExpression(tableName: tableName,
                                                       existingItem: databaseItemA)
        #expect(expression == "DELETE FROM \"TableName\" "
            + "WHERE PK='partitionKey' AND SK='sortKey' "
            + "AND RowVersion=1")
    }

    @Test
    func deleteItemExpressionWithEscapedQuotes() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "fir'stly", secondly: "secondl'y")
        let payloadA = TestTypeC(theString: "f'irstly", theNumber: 4, theStruct: theStruct, theList: ["third''ly", "fou'''rthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "p'artitionKe''y",
                                                       sortKey: "sort'''Key")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable(escapeSingleQuoteInPartiQL: true)

        let expression = try table.getDeleteExpression(tableName: tableName,
                                                       existingItem: databaseItemA)
        #expect(expression == "DELETE FROM \"TableName\" "
            + "WHERE PK='p''artitionKe''''y' AND SK='sort''''''Key' "
            + "AND RowVersion=1")
    }

    @Test
    func deleteKeyExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let expression = try table.getDeleteExpression(tableName: tableName,
                                                       existingItem: databaseItemA)
        #expect(expression == "DELETE FROM \"TableName\" "
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
