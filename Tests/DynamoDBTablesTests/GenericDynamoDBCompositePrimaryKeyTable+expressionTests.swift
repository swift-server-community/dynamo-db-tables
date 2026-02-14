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
//  GenericDynamoDBCompositePrimaryKeyTable+expressionTests.swift
//  DynamoDBTablesTests
//

import AWSDynamoDB
import Foundation
import Testing

@testable import DynamoDBTables

struct AWSDynamoDBCompositePrimaryKeyTableExpressionTests {
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
    func listFieldDifferenceExpression() throws {
        let tableName = "TableName"
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
        let databaseItemB = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadB)

        let table = try getTable()

        let expression = try table.getUpdateExpression(
            tableName: tableName,
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        #expect(
            expression == "UPDATE \"TableName\" "
                + "SET \"theList\"[1]='eigthly' "
                + "SET \"theList\"[2]='ninthly' "
                + "SET \"theList\"[3]='tenthly' "
                + "WHERE PK='partitionKey' AND SK='sortKey' "
                + "AND RowVersion=1"
        )
    }

    @Test
    func listFieldDifferenceExpressionWithEscapedQuotes() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "f'irstly", secondly: "s'econdly")
        let payloadA = TestTypeC(
            theString: "fi''rstly",
            theNumber: 4,
            theStruct: theStruct,
            theList: ["thirdl'y", "fourt''hly"]
        )
        let payloadB = TestTypeC(
            theString: "fi''rstly",
            theNumber: 4,
            theStruct: theStruct,
            theList: ["thirdl'y", "eigth''ly", "n'inthly", "tenth'''ly"]
        )

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "pa'rtition''Key",
            sortKey: "so'rt''Key"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadB)

        let table = try getTable(escapeSingleQuoteInPartiQL: true)

        let expression = try table.getUpdateExpression(
            tableName: tableName,
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        #expect(
            expression == "UPDATE \"TableName\" "
                + "SET \"theList\"[1]='eigth''''ly' "
                + "SET \"theList\"[2]='n''inthly' "
                + "SET \"theList\"[3]='tenth''''''ly' "
                + "WHERE PK='pa''rtition''''Key' AND SK='so''rt''''Key' "
                + "AND RowVersion=1"
        )
    }

    @Test
    func listFieldAdditionExpression() throws {
        let tableName = "TableName"
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
        let databaseItemB = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadB)

        let table = try getTable()

        let expression = try table.getUpdateExpression(
            tableName: tableName,
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        #expect(
            expression == "UPDATE \"TableName\" "
                + "SET \"theList\"=['thirdly', 'eigthly', 'ninthly', 'tenthly'] "
                + "WHERE PK='partitionKey' AND SK='sortKey' "
                + "AND RowVersion=1"
        )
    }

    @Test
    func listFieldRemovalExpression() throws {
        let tableName = "TableName"
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
        let databaseItemB = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadB)

        let table = try getTable()

        let expression = try table.getUpdateExpression(
            tableName: tableName,
            newItem: databaseItemB,
            existingItem: databaseItemA
        )
        #expect(
            expression == "UPDATE \"TableName\" "
                + "REMOVE \"theList\" "
                + "WHERE PK='partitionKey' AND SK='sortKey' "
                + "AND RowVersion=1"
        )
    }

    @Test
    func deleteItemExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(
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

        let table = try getTable()

        let expression = try table.getDeleteExpression(
            tableName: tableName,
            existingItem: databaseItemA
        )
        #expect(
            expression == "DELETE FROM \"TableName\" "
                + "WHERE PK='partitionKey' AND SK='sortKey' "
                + "AND RowVersion=1"
        )
    }

    @Test
    func deleteItemExpressionWithEscapedQuotes() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "fir'stly", secondly: "secondl'y")
        let payloadA = TestTypeC(
            theString: "f'irstly",
            theNumber: 4,
            theStruct: theStruct,
            theList: ["third''ly", "fou'''rthly"]
        )

        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "p'artitionKe''y",
            sortKey: "sort'''Key"
        )
        let databaseItemA = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)

        let table = try getTable(escapeSingleQuoteInPartiQL: true)

        let expression = try table.getDeleteExpression(
            tableName: tableName,
            existingItem: databaseItemA
        )
        #expect(
            expression == "DELETE FROM \"TableName\" "
                + "WHERE PK='p''artitionKe''''y' AND SK='sort''''''Key' "
                + "AND RowVersion=1"
        )
    }

    @Test
    func deleteKeyExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(
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

        let table = try getTable()

        let expression = try table.getDeleteExpression(
            tableName: tableName,
            existingItem: databaseItemA
        )
        #expect(
            expression == "DELETE FROM \"TableName\" "
                + "WHERE PK='partitionKey' AND SK='sortKey' "
                + "AND RowVersion=1"
        )
    }
}
