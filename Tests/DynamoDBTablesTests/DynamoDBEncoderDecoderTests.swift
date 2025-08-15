//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Tests/SmokeDynamoDBTests/DynamoDBEncoderDecoderTests.swift
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
//  DynamoDBEncoderDecoderTests.swift
//  DynamoDBTablesTests
//

import Testing

@testable import DynamoDBTables

struct CoreAccountAttributes: Codable {
    var description: String
    var mappedValues: [String: String]
    var notificationTargets: NotificationTargets
}

extension CoreAccountAttributes: Equatable {
    static func == (lhs: CoreAccountAttributes, rhs: CoreAccountAttributes) -> Bool {
        lhs.description == rhs.description && lhs.notificationTargets == rhs.notificationTargets
            && lhs.mappedValues == rhs.mappedValues
    }
}

struct NotificationTargets: Codable {
    var currentIDs: [String]
    var maximum: Int
}

extension NotificationTargets: Equatable {
    static func == (lhs: NotificationTargets, rhs: NotificationTargets) -> Bool {
        lhs.currentIDs == rhs.currentIDs && lhs.maximum == rhs.maximum
    }
}

typealias DatabaseItemType = StandardTypedDatabaseItem<CoreAccountAttributes>

struct DynamoDBEncoderDecoderTests {
    let partitionKey = "partitionKey"
    let sortKey = "sortKey"
    let attributes = CoreAccountAttributes(
        description: "Description",
        mappedValues: ["A": "one", "B": "two"],
        notificationTargets: NotificationTargets(currentIDs: [], maximum: 20)
    )

    @Test
    func encoderDecoder() throws {
        // create key and database item to create
        let key = StandardCompositePrimaryKey(partitionKey: partitionKey, sortKey: sortKey)
        let newDatabaseItem: DatabaseItemType = StandardTypedDatabaseItem.newItem(
            withKey: key,
            andValue: self.attributes
        )

        let encodedAttributeValue = try DynamoDBEncoder().encode(newDatabaseItem)

        let output: DatabaseItemType = try DynamoDBDecoder().decode(encodedAttributeValue)

        #expect(newDatabaseItem.rowValue == output.rowValue)
        #expect(self.partitionKey == output.compositePrimaryKey.partitionKey)
        #expect(self.sortKey == output.compositePrimaryKey.sortKey)
        #expect(self.attributes == output.rowValue)
        #expect(output.timeToLive == nil)
    }

    @Test
    func encoderDecoderWithTimeToLive() throws {
        let timeToLiveTimestamp: Int64 = 123_456_789
        let timeToLive = StandardTimeToLive(timeToLiveTimestamp: timeToLiveTimestamp)

        // create key and database item to create
        let key = StandardCompositePrimaryKey(partitionKey: partitionKey, sortKey: sortKey)
        let newDatabaseItem: DatabaseItemType = StandardTypedDatabaseItem.newItem(
            withKey: key,
            andValue: self.attributes,
            andTimeToLive: timeToLive
        )

        let encodedAttributeValue = try DynamoDBEncoder().encode(newDatabaseItem)

        let output: DatabaseItemType = try DynamoDBDecoder().decode(encodedAttributeValue)

        #expect(newDatabaseItem.rowValue == output.rowValue)
        #expect(self.partitionKey == output.compositePrimaryKey.partitionKey)
        #expect(self.sortKey == output.compositePrimaryKey.sortKey)
        #expect(self.attributes == output.rowValue)
        #expect(timeToLiveTimestamp == output.timeToLive?.timeToLiveTimestamp)
    }
}
