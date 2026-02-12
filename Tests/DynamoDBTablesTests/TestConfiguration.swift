//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Tests/SmokeDynamoDBTests/TestConfiguration.swift
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
//  TestConfiguration.swift
//  DynamoDBTablesTests
//

import Foundation

@testable import DynamoDBTables

struct TestTypeA: Codable, Equatable {
    let firstly: String
    let secondly: String
}

struct TestTypeB: Codable, Equatable, CustomRowTypeIdentifier {
    static let rowTypeIdentifier: String? = "TypeBCustom"

    let thirdly: String
    let fourthly: String
}

struct TestTypeC: Codable {
    let theString: String?
    let theNumber: Int?
    let theStruct: TestTypeA?
    let theList: [String]?

    init(theString: String?, theNumber: Int?, theStruct: TestTypeA?, theList: [String]?) {
        self.theString = theString
        self.theNumber = theNumber
        self.theStruct = theStruct
        self.theList = theList
    }
}

@PolymorphicOperationReturnType
enum TestQueryableTypes {
    case testTypeA(StandardTypedDatabaseItem<TestTypeA>)
    case testTypeB(StandardTypedDatabaseItem<TestTypeB>)
}

typealias TestTypeAWriteEntry = StandardWriteEntry<TestTypeA>
typealias TestTypeBWriteEntry = StandardWriteEntry<TestTypeB>
typealias TestTypeAStandardTransactionConstraintEntry = StandardTransactionConstraintEntry<TestTypeA>
typealias TestTypeBStandardTransactionConstraintEntry = StandardTransactionConstraintEntry<TestTypeB>

@PolymorphicWriteEntry
enum TestPolymorphicWriteEntry {
    case testTypeA(TestTypeAWriteEntry)
    case testTypeB(TestTypeBWriteEntry)
}

@PolymorphicTransactionConstraintEntry
enum TestPolymorphicTransactionConstraintEntry {
    case testTypeA(TestTypeAStandardTransactionConstraintEntry)
    case testTypeB(TestTypeBStandardTransactionConstraintEntry)
}
