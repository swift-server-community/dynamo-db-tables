//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Tests/SmokeDynamoDBTests/SmokeDynamoDBTestInput.swift
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
//  SmokeDynamoDBTestInput.swift
//  DynamoDBTablesTests
//
@testable import DynamoDBTables
import Foundation

enum AllQueryableTypes: PolymorphicOperationReturnType {
    typealias AttributesType = StandardPrimaryKeyAttributes

    static let types: [(Codable.Type, PolymorphicOperationReturnOption<StandardPrimaryKeyAttributes, Self>)] = [
        (TypeA.self, .init { .typeA($0) }),
        (TypeB.self, .init { .typeB($0) }),
    ]

    case typeA(StandardTypedDatabaseItem<TypeA>)
    case typeB(StandardTypedDatabaseItem<TypeB>)
}

enum SomeQueryableTypes: PolymorphicOperationReturnType {
    typealias AttributesType = StandardPrimaryKeyAttributes

    static let types: [(Codable.Type, PolymorphicOperationReturnOption<StandardPrimaryKeyAttributes, Self>)] = [
        (TypeA.self, .init { .typeA($0) }),
    ]

    case typeA(StandardTypedDatabaseItem<TypeA>)
}

struct GSI1PKIndexIdentity: IndexIdentity, Sendable {
    static let codingKey = createRowWithIndexCodingKey(stringValue: "GSI-1-PK")
    static let identity = "GSI1PK"
}

enum AllQueryableTypesWithIndex: PolymorphicOperationReturnType {
    typealias AttributesType = StandardPrimaryKeyAttributes

    static let types: [(Codable.Type, PolymorphicOperationReturnOption<StandardPrimaryKeyAttributes, Self>)] = [
        (RowWithIndex<TypeA, GSI1PKIndexIdentity>.self, .init { .typeAWithIndex($0) }),
        (TypeB.self, .init { .typeB($0) }),
    ]

    case typeAWithIndex(StandardTypedDatabaseItem<RowWithIndex<TypeA, GSI1PKIndexIdentity>>)
    case typeB(StandardTypedDatabaseItem<TestTypeB>)
}

struct TypeA: Codable, Sendable {
    let firstly: String
    let secondly: String

    init(firstly: String, secondly: String) {
        self.firstly = firstly
        self.secondly = secondly
    }
}

struct TypeB: Codable, CustomRowTypeIdentifier {
    static let rowTypeIdentifier: String? = "TypeBCustom"

    let thirdly: String
    let fourthly: String
}

let serializedTypeADatabaseItem = """
{
    "M" : {
        "PK" : { "S": "partitionKey" },
        "SK" : { "S": "sortKey" },
        "CreateDate" : { "S" : "2018-01-06T23:36:20.355Z" },
        "RowVersion" : { "N": "5" },
        "LastUpdatedDate" : { "S" : "2018-01-06T23:36:20.355Z" },
        "RowType": { "S": "TypeA" },
        "firstly" : { "S": "aaa" },
        "secondly": { "S": "bbb" }
    }
}
"""

let serializedTypeADatabaseItemWithTimeToLive = """
{
    "M" : {
        "PK" : { "S": "partitionKey" },
        "SK" : { "S": "sortKey" },
        "CreateDate" : { "S" : "2018-01-06T23:36:20.355Z" },
        "RowVersion" : { "N": "5" },
        "LastUpdatedDate" : { "S" : "2018-01-06T23:36:20.355Z" },
        "RowType": { "S": "TypeA" },
        "firstly" : { "S": "aaa" },
        "secondly": { "S": "bbb" },
        "ExpireDate": { "N": "123456789" }
    }
}
"""

let serializedPolymorphicDatabaseItemList = """
[
    {
        "M" : {
            "PK" : { "S": "partitionKey1" },
            "SK" : { "S": "sortKey1" },
            "CreateDate" : { "S" : "2018-01-06T23:36:20.355Z" },
            "RowVersion" : { "N": "5" },
            "LastUpdatedDate" : { "S" : "2018-01-06T23:36:20.355Z" },
            "RowType": { "S": "TypeA" },
            "firstly" : { "S": "aaa" },
            "secondly": { "S": "bbb" }
        }
    },
    {
        "M" : {
            "PK" : { "S": "partitionKey2" },
            "SK" : { "S": "sortKey2" },
            "CreateDate" : { "S" : "2018-01-06T23:36:20.355Z" },
            "RowVersion" : { "N": "12" },
            "LastUpdatedDate" : { "S" : "2018-01-06T23:36:20.355Z" },
            "RowType": { "S": "TypeBCustom" },
            "thirdly" : { "S": "ccc" },
            "fourthly": { "S": "ddd" }
        }
    }
]
"""

let serializedPolymorphicDatabaseItemListWithIndex = """
[
    {
        "M" : {
            "PK" : { "S": "partitionKey1" },
            "SK" : { "S": "sortKey1" },
            "GSI-1-PK" : { "S": "gsi-index" },
            "CreateDate" : { "S" : "2018-01-06T23:36:20.355Z" },
            "RowVersion" : { "N": "5" },
            "LastUpdatedDate" : { "S" : "2018-01-06T23:36:20.355Z" },
            "RowType": { "S": "TypeAWithGSI1PKIndex" },
            "firstly" : { "S": "aaa" },
            "secondly": { "S": "bbb" }
        }
    },
    {
        "M" : {
            "PK" : { "S": "partitionKey2" },
            "SK" : { "S": "sortKey2" },
            "CreateDate" : { "S" : "2018-01-06T23:36:20.355Z" },
            "RowVersion" : { "N": "12" },
            "LastUpdatedDate" : { "S" : "2018-01-06T23:36:20.355Z" },
            "RowType": { "S": "TypeBCustom" },
            "thirdly" : { "S": "ccc" },
            "fourthly": { "S": "ddd" }
        }
    }
]
"""
