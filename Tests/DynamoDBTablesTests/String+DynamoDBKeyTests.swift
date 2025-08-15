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
//  String+DynamoDBKeyTests.swift
//  DynamoDBTablesTests
//

import Testing

@testable import DynamoDBTables

struct StringDynamoDBKeyTests {
    @Test
    func dynamoDBKeyTests() {
        #expect([].dynamodbKey == "")
        #expect(["one"].dynamodbKey == "one")
        #expect(["one", "two"].dynamodbKey == "one.two")
        #expect(["one", "two", "three", "four", "five", "six"].dynamodbKey == "one.two.three.four.five.six")
    }

    @Test
    func dropAsDynamoDBKeyPrefix() {
        #expect(["one", "two"].dropAsDynamoDBKeyPrefix(from: "one.two.three.four.five.six")! == "three.four.five.six")
        #expect([].dropAsDynamoDBKeyPrefix(from: "one.two.three.four.five.six")! == "one.two.three.four.five.six")
        #expect(["four", "two"].dropAsDynamoDBKeyPrefix(from: "one.two.three.four.five.six") == nil)
    }

    @Test
    func dynamoDBKeyPrefixTests() {
        #expect([].dynamodbKeyPrefix == "")
        #expect(["one"].dynamodbKeyPrefix == "one.")
        #expect(["one", "two"].dynamodbKeyPrefix == "one.two.")
        #expect(["one", "two", "three", "four", "five", "six"].dynamodbKeyPrefix == "one.two.three.four.five.six.")
    }

    @Test
    func dynamoDBKeyWithPrefixedVersionTests() {
        #expect([].dynamodbKeyWithPrefixedVersion(8, minimumFieldWidth: 5) == "v00008")
        #expect(["one"].dynamodbKeyWithPrefixedVersion(8, minimumFieldWidth: 5) == "v00008.one")
        #expect(["one", "two"].dynamodbKeyWithPrefixedVersion(8, minimumFieldWidth: 5) == "v00008.one.two")
        #expect(
            ["one", "two", "three", "four", "five", "six"].dynamodbKeyWithPrefixedVersion(8, minimumFieldWidth: 5)
                == "v00008.one.two.three.four.five.six"
        )

        #expect(["one", "two"].dynamodbKeyWithPrefixedVersion(8, minimumFieldWidth: 2) == "v08.one.two")
        #expect(["one", "two"].dynamodbKeyWithPrefixedVersion(4888, minimumFieldWidth: 2) == "v4888.one.two")
    }
}
