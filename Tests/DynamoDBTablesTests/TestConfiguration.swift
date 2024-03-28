//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from https://github.com/amzn/smoke-dynamodb. Any commits
// prior to February 2024
// Copyright (c) 2021-2021 the DynamoDBTables authors
// Licensed under Apache License v2.0
//
// Subsequent commits
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
        (TestTypeA.self, .init( {.testTypeA($0)} )),
        (TestTypeB.self, .init( {.testTypeB($0)} )),
        ]
    
    case testTypeA(StandardTypedDatabaseItem<TestTypeA>)
    case testTypeB(StandardTypedDatabaseItem<TestTypeB>)
}

extension TestQueryableTypes: BatchCapableReturnType {
    func getItemKey() -> CompositePrimaryKey<StandardPrimaryKeyAttributes> {
        switch self {
        case .testTypeA(let databaseItem):
            return databaseItem.compositePrimaryKey
        case .testTypeB(let databaseItem):
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
        case .testTypeA(let writeEntry):
            return try context.transform(writeEntry)
        case .testTypeB(let writeEntry):
            return try context.transform(writeEntry)
        }
    }
}

enum TestPolymorphicTransactionConstraintEntry: PolymorphicTransactionConstraintEntry {
    case testTypeA(TestTypeAStandardTransactionConstraintEntry)
    case testTypeB(TestTypeBStandardTransactionConstraintEntry)

    func handle<Context: PolymorphicWriteEntryContext>(context: Context) throws -> Context.WriteTransactionConstraintType {
        switch self {
        case .testTypeA(let writeEntry):
            return try context.transform(writeEntry)
        case .testTypeB(let writeEntry):
            return try context.transform(writeEntry)
        }
    }
}
