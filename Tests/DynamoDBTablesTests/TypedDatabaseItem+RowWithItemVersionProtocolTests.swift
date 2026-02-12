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
//  TypedTTLDatabaseItem+RowWithItemVersionProtocolTests.swift
//  DynamoDBTablesTests
//

import Foundation
import Testing

@testable import DynamoDBTables

private let ORIGINAL_PAYLOAD = "Payload"
private let ORIGINAL_TIME_TO_LIVE: Int64 = 123_456_789
private let UPDATED_PAYLOAD = "Updated"
private let UPDATED_TIME_TO_LIVE: Int64 = 234_567_890

struct TypedTTLDatabaseItemRowWithItemVersionProtocolTests {
    @Test
    func createUpdatedRowWithItemVersion() throws {
        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "partitionKey",
            sortKey: "sortKey"
        )
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: rowWithItemVersion)

        let updatedItem = try databaseItem.createUpdatedRowWithItemVersion(
            withValue: "Updated",
            conditionalStatusVersion: nil
        )

        #expect(databaseItem.rowStatus.rowVersion == 1)
        #expect(databaseItem.rowValue.itemVersion == 1)
        #expect(ORIGINAL_PAYLOAD == databaseItem.rowValue.rowValue)
        #expect(databaseItem.timeToLive == nil)
        #expect(updatedItem.rowStatus.rowVersion == 2)
        #expect(updatedItem.rowValue.itemVersion == 2)
        #expect(UPDATED_PAYLOAD == updatedItem.rowValue.rowValue)
        #expect(updatedItem.timeToLive == nil)
    }

    @Test
    func createUpdatedRowWithItemVersionWithTimeToLive() throws {
        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "partitionKey",
            sortKey: "sortKey"
        )
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = TypedTTLDatabaseItem.newItem(
            withKey: compositeKey,
            andValue: rowWithItemVersion,
            andTimeToLive: StandardTimeToLive(timeToLiveTimestamp: 123_456_789)
        )

        let updatedItem = try databaseItem.createUpdatedRowWithItemVersion(
            withValue: "Updated",
            conditionalStatusVersion: nil,
            andTimeToLive: StandardTimeToLive(timeToLiveTimestamp: 234_567_890)
        )

        #expect(databaseItem.rowStatus.rowVersion == 1)
        #expect(databaseItem.rowValue.itemVersion == 1)
        #expect(ORIGINAL_PAYLOAD == databaseItem.rowValue.rowValue)
        #expect(ORIGINAL_TIME_TO_LIVE == databaseItem.timeToLive?.timeToLiveTimestamp)
        #expect(updatedItem.rowStatus.rowVersion == 2)
        #expect(updatedItem.rowValue.itemVersion == 2)
        #expect(UPDATED_PAYLOAD == updatedItem.rowValue.rowValue)
        #expect(UPDATED_TIME_TO_LIVE == updatedItem.timeToLive?.timeToLiveTimestamp)
    }

    @Test
    func createUpdatedRowWithItemVersionWithCorrectConditionalVersion() throws {
        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "partitionKey",
            sortKey: "sortKey"
        )
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: rowWithItemVersion)

        let updatedItem = try databaseItem.createUpdatedRowWithItemVersion(
            withValue: "Updated",
            conditionalStatusVersion: 1
        )

        #expect(databaseItem.rowStatus.rowVersion == 1)
        #expect(databaseItem.rowValue.itemVersion == 1)
        #expect(ORIGINAL_PAYLOAD == databaseItem.rowValue.rowValue)
        #expect(updatedItem.rowStatus.rowVersion == 2)
        #expect(updatedItem.rowValue.itemVersion == 2)
        #expect(UPDATED_PAYLOAD == updatedItem.rowValue.rowValue)
    }

    @Test
    func createUpdatedRowWithItemVersionWithIncorrectConditionalVersion() {
        let compositeKey = StandardCompositePrimaryKey(
            partitionKey: "partitionKey",
            sortKey: "sortKey"
        )
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: compositeKey, andValue: rowWithItemVersion)

        do {
            _ = try databaseItem.createUpdatedRowWithItemVersion(
                withValue: "Updated",
                conditionalStatusVersion: 8
            )

            Issue.record("Expected error not thrown.")
        } catch DynamoDBTableError.concurrencyError {
            return
        } catch {
            Issue.record("Unexpected error thrown: '\(error)'.")
        }
    }
}
