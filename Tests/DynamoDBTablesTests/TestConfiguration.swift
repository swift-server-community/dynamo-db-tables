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

@testable import DynamoDBTables
import Foundation

struct TestTypeA: Codable, Equatable {
    let firstly: String
    let secondly: String
}

struct TestTypeB: Codable, Equatable, CustomRowTypeIdentifier {
    static var rowTypeIdentifier: String? = "TypeBCustom"

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

enum TestQueryableTypes: PolymorphicOperationReturnType {
    typealias AttributesType = StandardPrimaryKeyAttributes

    static var types: [(Codable.Type, PolymorphicOperationReturnOption<StandardPrimaryKeyAttributes, Self>)] = [
        (TestTypeA.self, .init { .testTypeA($0) }),
        (TestTypeB.self, .init { .testTypeB($0) }),
    ]

    case testTypeA(StandardTypedDatabaseItem<TestTypeA>)
    case testTypeB(StandardTypedDatabaseItem<TestTypeB>)
}

extension TestQueryableTypes: BatchCapableReturnType {
    func getItemKey() -> CompositePrimaryKey<StandardPrimaryKeyAttributes> {
        switch self {
        case let .testTypeA(databaseItem):
            return databaseItem.compositePrimaryKey
        case let .testTypeB(databaseItem):
            return databaseItem.compositePrimaryKey
        }
    }
}

typealias TestTypeAWriteEntry = StandardWriteEntry<TestTypeA>
typealias TestTypeBWriteEntry = StandardWriteEntry<TestTypeB>
typealias TestTypeAStandardTransactionConstraintEntry = StandardTransactionConstraintEntry<TestTypeA>
typealias TestTypeBStandardTransactionConstraintEntry = StandardTransactionConstraintEntry<TestTypeB>

enum TestPolymorphicWriteEntry: PolymorphicWriteEntry {
    case testTypeA(TestTypeAWriteEntry)
    case testTypeB(TestTypeBWriteEntry)

    func handle<Context: PolymorphicWriteEntryContext>(context: Context) throws -> Context.WriteEntryTransformType {
        switch self {
        case let .testTypeA(writeEntry):
            return try context.transform(writeEntry)
        case let .testTypeB(writeEntry):
            return try context.transform(writeEntry)
        }
    }
}

enum TestPolymorphicTransactionConstraintEntry: PolymorphicTransactionConstraintEntry {
    case testTypeA(TestTypeAStandardTransactionConstraintEntry)
    case testTypeB(TestTypeBStandardTransactionConstraintEntry)

    func handle<Context: PolymorphicWriteEntryContext>(context: Context) throws -> Context.WriteTransactionConstraintType {
        switch self {
        case let .testTypeA(writeEntry):
            return try context.transform(writeEntry)
        case let .testTypeB(writeEntry):
            return try context.transform(writeEntry)
        }
    }
}
