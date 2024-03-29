//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Tests/SmokeDynamoDBTests/String+DynamoDBKeyTests.swift
// Copyright 2018-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// Licensed under Apache License v2.0
//
// Changes specified by
// https://github.com/swift-server-community/dynamo-db-tables/compare/6fec4c8..main
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
//  String+DynamoDBKeyTests.swift
//  DynamoDBTablesTests
//

import XCTest
@testable import DynamoDBTables

class StringDynamoDBKeyTests: XCTestCase {

    func testDynamoDBKeyTests() {
        XCTAssertEqual([].dynamodbKey, "")
        XCTAssertEqual(["one"].dynamodbKey, "one")
        XCTAssertEqual(["one", "two"].dynamodbKey, "one.two")
        XCTAssertEqual(["one", "two", "three", "four", "five", "six"].dynamodbKey, "one.two.three.four.five.six")
    }
    
    func testDropAsDynamoDBKeyPrefix() {
        XCTAssertEqual(["one", "two"].dropAsDynamoDBKeyPrefix(from: "one.two.three.four.five.six")!,
                       "three.four.five.six")
        XCTAssertEqual([].dropAsDynamoDBKeyPrefix(from: "one.two.three.four.five.six")!,
                       "one.two.three.four.five.six")
        XCTAssertEqual(["four", "two"].dropAsDynamoDBKeyPrefix(from: "one.two.three.four.five.six"), nil)
    }
    
    func testDynamoDBKeyPrefixTests() {
        XCTAssertEqual([].dynamodbKeyPrefix, "")
        XCTAssertEqual(["one"].dynamodbKeyPrefix, "one.")
        XCTAssertEqual(["one", "two"].dynamodbKeyPrefix, "one.two.")
        XCTAssertEqual(["one", "two", "three", "four", "five", "six"].dynamodbKeyPrefix, "one.two.three.four.five.six.")
    }
    
    func testDynamoDBKeyWithPrefixedVersionTests() {
        XCTAssertEqual([].dynamodbKeyWithPrefixedVersion(8, minimumFieldWidth: 5), "v00008")
        XCTAssertEqual(["one"].dynamodbKeyWithPrefixedVersion(8, minimumFieldWidth: 5), "v00008.one")
        XCTAssertEqual(["one", "two"].dynamodbKeyWithPrefixedVersion(8, minimumFieldWidth: 5), "v00008.one.two")
        XCTAssertEqual(["one", "two", "three", "four", "five", "six"].dynamodbKeyWithPrefixedVersion(8, minimumFieldWidth: 5),
                       "v00008.one.two.three.four.five.six")
        
        XCTAssertEqual(["one", "two"].dynamodbKeyWithPrefixedVersion(8, minimumFieldWidth: 2), "v08.one.two")
        XCTAssertEqual(["one", "two"].dynamodbKeyWithPrefixedVersion(4888, minimumFieldWidth: 2), "v4888.one.two")
    }
}
