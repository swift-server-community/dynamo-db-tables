//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/RowWithIndex.swift
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
//  RowWithIndex.swift
//  DynamoDBTables
//

public protocol IndexIdentity {
    static var codingKey: RowWithIndexCodingKey { get }
    static var identity: String { get }
}

public struct RowWithIndexCodingKey: CodingKey {
    public var intValue: Int?
    public var stringValue: String

    public init?(stringValue: String) {
        self.stringValue = stringValue
        self.intValue = nil
    }

    public init?(intValue _: Int) {
        nil
    }

}

public func createRowWithIndexCodingKey(stringValue: String) -> RowWithIndexCodingKey {
    RowWithIndexCodingKey(stringValue: stringValue)!
}

public struct RowWithIndex<RowType: Codable & Sendable, Identity: IndexIdentity>: Codable, CustomRowTypeIdentifier,
    Sendable
{
    public static var rowTypeIdentifier: String? {
        let rowTypeIdentity = getTypeRowIdentifier(type: RowType.self)
        let indexIdentity = Identity.identity

        return "\(rowTypeIdentity)With\(indexIdentity)Index"
    }

    public let indexValue: String
    public let rowValue: RowType

    public static func newItem(
        withIndex indexValue: String,
        andValue rowValue: RowType
    ) -> RowWithIndex<RowType, Identity> {
        RowWithIndex<RowType, Identity>(
            indexValue: indexValue,
            rowValue: rowValue
        )
    }

    public func createUpdatedItem(withValue newRowValue: RowType) -> RowWithIndex<RowType, Identity> {
        RowWithIndex<RowType, Identity>(
            indexValue: self.indexValue,
            rowValue: newRowValue
        )
    }

    init(
        indexValue: String,
        rowValue: RowType
    ) {
        self.indexValue = indexValue
        self.rowValue = rowValue
    }

    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: RowWithIndexCodingKey.self)
        self.indexValue = try values.decode(String.self, forKey: Identity.codingKey)

        self.rowValue = try RowType(from: decoder)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: RowWithIndexCodingKey.self)
        try container.encode(self.indexValue, forKey: Identity.codingKey)

        try self.rowValue.encode(to: encoder)
    }
}
