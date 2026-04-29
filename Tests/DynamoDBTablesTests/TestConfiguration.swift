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
// Copyright (c) 2026 the DynamoDBTables authors
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

typealias TestTypeAItem = StandardTypedDatabaseItem<TestTypeA>

@PolymorphicOperationReturnType
enum TestQueryableTypes {
    case testTypeA(TestTypeAItem)
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

// Lock in that the macros work with non-Standard `PrimaryKeyAttributes` end-to-end. The rest of
// the test suite uses `Standard*` everywhere; these fixtures exist purely to verify the
// polymorphic behaviour of the macros.
struct CustomPrimaryKeyAttributes: PrimaryKeyAttributes {
    static var partitionKeyAttributeName: String { "CustomPK" }
    static var sortKeyAttributeName: String { "CustomSK" }
}

typealias CustomAttributesWriteEntry<ItemType: Codable & Sendable> = WriteEntry<
    CustomPrimaryKeyAttributes, ItemType, StandardTimeToLiveAttributes
>
typealias CustomAttributesTransactionConstraintEntry<ItemType: Codable & Sendable> = TransactionConstraintEntry<
    CustomPrimaryKeyAttributes, ItemType, StandardTimeToLiveAttributes
>
typealias CustomAttributesTypedDatabaseItem<RowType: Codable & Sendable> = TypedTTLDatabaseItem<
    CustomPrimaryKeyAttributes, RowType, StandardTimeToLiveAttributes
>

@PolymorphicWriteEntry
enum CustomAttributesPolymorphicWriteEntry {
    case testTypeA(CustomAttributesWriteEntry<TestTypeA>)
    case testTypeB(CustomAttributesWriteEntry<TestTypeB>)
}

@PolymorphicTransactionConstraintEntry
enum CustomAttributesPolymorphicTransactionConstraintEntry {
    case testTypeA(CustomAttributesTransactionConstraintEntry<TestTypeA>)
    case testTypeB(CustomAttributesTransactionConstraintEntry<TestTypeB>)
}

@PolymorphicOperationReturnType
enum CustomAttributesQueryableTypes {
    case testTypeA(CustomAttributesTypedDatabaseItem<TestTypeA>)
    case testTypeB(CustomAttributesTypedDatabaseItem<TestTypeB>)
}
