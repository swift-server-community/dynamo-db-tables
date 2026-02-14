//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Tests/SmokeDynamoDBTests/TypedTTLDatabaseItem+RowWithItemVersionProtocolTests.swift
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
//  GenericDynamoDBCompositePrimaryKeyTable+diffItemsTests.swift
//  DynamoDBTablesTests
//

import AWSDynamoDB
import Foundation
import Testing

@testable import DynamoDBTables

struct AWSDynamoDBCompositePrimaryKeyTableDiffItemsTests {
    func getTable(
        escapeSingleQuoteInPartiQL: Bool = false
    ) throws -> GenericDynamoDBCompositePrimaryKeyTable<MockTestDynamoDBClientProtocol> {
        GenericDynamoDBCompositePrimaryKeyTable(
            tableName: "DummyTable",
            client: MockTestDynamoDBClientProtocol(),
            tableConfiguration: .init(escapeSingleQuoteInPartiQL: escapeSingleQuoteInPartiQL)
        )
    }

    @Test
    func stringFieldDifference() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(
            theString: "firstly",
            theNumber: 4,
            theStruct: theStruct,
            theList: ["thirdly", "fourthly"]
        )
        let payloadB = TestTypeC(
            theString: "eigthly",
            theNumber: 4,
            theStruct: theStruct,
            theList: ["thirdly", "fourthly"]
        )

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "partitionKey",
            sortKey: "sortKey"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = try getTable()

        let differences = try table.diffItems(
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        let pathMap = differences.pathMap

        #expect(pathMap["\"theString\""] == .update(path: "\"theString\"", value: "'eigthly'"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func stringFieldDifferenceWithEscapedQuotes() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(
            theString: "f'ir''st'''ly",
            theNumber: 4,
            theStruct: theStruct,
            theList: ["t'hi''rdly", "fourt''hl'y"]
        )
        let payloadB = TestTypeC(
            theString: "e'ig''thl'''y",
            theNumber: 4,
            theStruct: theStruct,
            theList: ["t'hi''rdly", "fourt''hl'y"]
        )

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "par'titio''nKey",
            sortKey: "so'rt'''Key"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = try getTable(escapeSingleQuoteInPartiQL: true)

        let differences = try table.diffItems(
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        let pathMap = differences.pathMap

        #expect(pathMap["\"theString\""] == .update(path: "\"theString\"", value: "'e''ig''''thl''''''y'"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func numberFieldDifference() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(
            theString: "firstly",
            theNumber: 4,
            theStruct: theStruct,
            theList: ["thirdly", "fourthly"]
        )
        let payloadB = TestTypeC(
            theString: "firstly",
            theNumber: 12,
            theStruct: theStruct,
            theList: ["thirdly", "fourthly"]
        )

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "partitionKey",
            sortKey: "sortKey"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = try getTable()

        let differences = try table.diffItems(
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        let pathMap = differences.pathMap

        #expect(pathMap["\"theNumber\""] == .update(path: "\"theNumber\"", value: "12"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func structFieldDifference() throws {
        let theStructA = TestTypeA(firstly: "firstly", secondly: "secondly")
        let theStructB = TestTypeA(firstly: "eigthly", secondly: "secondly")
        let payloadA = TestTypeC(
            theString: "firstly",
            theNumber: 4,
            theStruct: theStructA,
            theList: ["thirdly", "fourthly"]
        )
        let payloadB = TestTypeC(
            theString: "firstly",
            theNumber: 4,
            theStruct: theStructB,
            theList: ["thirdly", "fourthly"]
        )

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "partitionKey",
            sortKey: "sortKey"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = try getTable()

        let differences = try table.diffItems(
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        let pathMap = differences.pathMap

        #expect(pathMap["\"theStruct\".\"firstly\""] == .update(path: "\"theStruct\".\"firstly\"", value: "'eigthly'"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func structFieldDifferenceWithEscapedQuotes() throws {
        let theStructA = TestTypeA(firstly: "f'ir''st'''ly", secondly: "se'con''dl'''y")
        let theStructB = TestTypeA(firstly: "e'ig''thly'''", secondly: "se'con''dl'''y")
        let payloadA = TestTypeC(
            theString: "fi''rst'ly",
            theNumber: 4,
            theStruct: theStructA,
            theList: ["t'hi'rdl'y", "f'ou'rthl'y"]
        )
        let payloadB = TestTypeC(
            theString: "fi''rstl'y",
            theNumber: 4,
            theStruct: theStructB,
            theList: ["t'hi'rdl'y", "f'ou'rthl'y"]
        )

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "par'titio''nKey",
            sortKey: "so'rt'''Key"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = try getTable(escapeSingleQuoteInPartiQL: true)

        let differences = try table.diffItems(
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        let pathMap = differences.pathMap

        #expect(
            pathMap["\"theStruct\".\"firstly\""]
                == .update(path: "\"theStruct\".\"firstly\"", value: "'e''ig''''thly'''''''")
        )
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func listFieldDifference() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(
            theString: "firstly",
            theNumber: 4,
            theStruct: theStruct,
            theList: ["thirdly", "fourthly"]
        )
        let payloadB = TestTypeC(
            theString: "firstly",
            theNumber: 4,
            theStruct: theStruct,
            theList: ["thirdly", "eigthly", "ninthly", "tenthly"]
        )

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "partitionKey",
            sortKey: "sortKey"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = try getTable()

        let differences = try table.diffItems(
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
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

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "partitionKey",
            sortKey: "sortKey"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = try getTable()

        let differences = try table.diffItems(
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        let pathMap = differences.pathMap

        #expect(pathMap["\"nestedList\"[0][1]"] == .update(path: "\"nestedList\"[0][1]", value: "'five'"))
        #expect(pathMap["\"nestedList\"[1][2]"] == .update(path: "\"nestedList\"[1][2]", value: "'eight'"))
        #expect(pathMap["\"nestedList\"[2]"] == .update(path: "\"nestedList\"[2]", value: "['six', 'seven']"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func listFieldDifferenceWithEscapedQuotes() throws {
        let theStruct = TestTypeA(firstly: "f'irstl''y", secondly: "se'cond''ly")
        let payloadA = TestTypeC(
            theString: "f'irst''ly",
            theNumber: 4,
            theStruct: theStruct,
            theList: [
                "th'irdly",
                "fo''urthly",
            ]
        )
        let payloadB = TestTypeC(
            theString: "f'irst''ly",
            theNumber: 4,
            theStruct: theStruct,
            theList: [
                "th'irdly",
                "eigth'''ly",
                "ni'n''thly",
                "ten'thly'",
            ]
        )

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "par'titio''nKey",
            sortKey: "so'rt'''Key"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = try getTable(escapeSingleQuoteInPartiQL: true)

        let differences = try table.diffItems(
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
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
        let payloadB = TestTypeC(
            theString: "eigthly",
            theNumber: 4,
            theStruct: theStruct,
            theList: ["thirdly", "fourthly"]
        )

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "partitionKey",
            sortKey: "sortKey"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = try getTable()

        let differences = try table.diffItems(
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        let pathMap = differences.pathMap

        #expect(pathMap["\"theString\""] == .update(path: "\"theString\"", value: "'eigthly'"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func numberFieldAddition() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(
            theString: "firstly",
            theNumber: nil,
            theStruct: theStruct,
            theList: ["thirdly", "fourthly"]
        )
        let payloadB = TestTypeC(
            theString: "firstly",
            theNumber: 12,
            theStruct: theStruct,
            theList: ["thirdly", "fourthly"]
        )

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "partitionKey",
            sortKey: "sortKey"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = try getTable()

        let differences = try table.diffItems(
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        let pathMap = differences.pathMap

        #expect(pathMap["\"theNumber\""] == .update(path: "\"theNumber\"", value: "12"))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func structFieldAddition() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: nil, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(
            theString: "firstly",
            theNumber: 4,
            theStruct: theStruct,
            theList: ["thirdly", "fourthly"]
        )

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "partitionKey",
            sortKey: "sortKey"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = try getTable()

        let differences = try table.diffItems(
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        let pathMap = differences.pathMap

        guard case let .update(_, value) = pathMap["\"theStruct\""] else {
            Issue.record("Value not in path map")
            return
        }

        let valueMatches =
            (value == "{'firstly': 'firstly', 'secondly': 'secondly'}")
            || (value == "{'secondly': 'secondly', 'firstly': 'firstly'}")

        #expect(valueMatches)
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func listFieldAddition() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: nil)
        let payloadB = TestTypeC(
            theString: "firstly",
            theNumber: 4,
            theStruct: theStruct,
            theList: ["thirdly", "eigthly", "ninthly", "tenthly"]
        )

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "partitionKey",
            sortKey: "sortKey"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = try getTable()

        let differences = try table.diffItems(
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        let pathMap = differences.pathMap

        #expect(
            pathMap["\"theList\""]
                == .update(path: "\"theList\"", value: "['thirdly', 'eigthly', 'ninthly', 'tenthly']")
        )
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func stringFieldRemoval() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(
            theString: "firstly",
            theNumber: 4,
            theStruct: theStruct,
            theList: ["thirdly", "fourthly"]
        )
        let payloadB = TestTypeC(theString: nil, theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "partitionKey",
            sortKey: "sortKey"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = try getTable()

        let differences = try table.diffItems(
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        let pathMap = differences.pathMap

        #expect(pathMap["\"theString\""] == .remove(path: "\"theString\""))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func numberFieldRemoval() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(
            theString: "firstly",
            theNumber: 4,
            theStruct: theStruct,
            theList: ["thirdly", "fourthly"]
        )
        let payloadB = TestTypeC(
            theString: "firstly",
            theNumber: nil,
            theStruct: theStruct,
            theList: ["thirdly", "fourthly"]
        )

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "partitionKey",
            sortKey: "sortKey"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = try getTable()

        let differences = try table.diffItems(
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        let pathMap = differences.pathMap

        #expect(pathMap["\"theNumber\""] == .remove(path: "\"theNumber\""))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func structFieldRemoval() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(
            theString: "firstly",
            theNumber: 4,
            theStruct: theStruct,
            theList: ["thirdly", "fourthly"]
        )
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: nil, theList: ["thirdly", "fourthly"])

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "partitionKey",
            sortKey: "sortKey"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = try getTable()

        let differences = try table.diffItems(
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        let pathMap = differences.pathMap

        #expect(pathMap["\"theStruct\""] == .remove(path: "\"theStruct\""))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
    }

    @Test
    func listFieldRemoval() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(
            theString: "firstly",
            theNumber: 4,
            theStruct: theStruct,
            theList: ["thirdly", "fourthly"]
        )
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: nil)

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "partitionKey",
            sortKey: "sortKey"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)

        let table = try getTable()

        let differences = try table.diffItems(
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        let pathMap = differences.pathMap

        #expect(pathMap["\"theList\""] == .remove(path: "\"theList\""))
        #expect(pathMap["\"RowVersion\""] == .update(path: "\"RowVersion\"", value: "2"))
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
