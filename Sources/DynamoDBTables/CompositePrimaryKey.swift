//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/CompositePrimaryKey.swift
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
//  CompositePrimaryKey.swift
//  DynamoDBTables
//

import Foundation

public protocol PrimaryKeyAttributes {
    static var partitionKeyAttributeName: String { get }
    static var sortKeyAttributeName: String { get }
    static var indexName: String? { get }
}

public extension PrimaryKeyAttributes {
    static var indexName: String? {
        return nil
    }
}

public struct StandardPrimaryKeyAttributes: PrimaryKeyAttributes {
    public static var partitionKeyAttributeName: String {
        return "PK"
    }
    public static var sortKeyAttributeName: String {
        return "SK"
    }
}

public typealias StandardTypedDatabaseItem<RowType: Codable> = TypedDatabaseItem<StandardPrimaryKeyAttributes, RowType>
public typealias StandardCompositePrimaryKey = CompositePrimaryKey<StandardPrimaryKeyAttributes>

struct DynamoDBAttributesTypeCodingKey: CodingKey {
    var stringValue: String
    var intValue: Int?

    init?(stringValue: String) {
        self.stringValue = stringValue
        self.intValue = nil
    }

    init?(intValue: Int) {
        self.stringValue = "\(intValue)"
        self.intValue = intValue
    }
}

public struct CompositePrimaryKey<AttributesType: PrimaryKeyAttributes>: Codable, CustomStringConvertible, Hashable {
    public var description: String {
        return "CompositePrimaryKey(partitionKey: \(partitionKey), sortKey: \(sortKey))"
    }
    
    public let partitionKey: String
    public let sortKey: String
    
    public init(partitionKey: String, sortKey: String) {
        self.partitionKey = partitionKey
        self.sortKey = sortKey
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: DynamoDBAttributesTypeCodingKey.self)
        partitionKey = try values.decode(String.self, forKey: DynamoDBAttributesTypeCodingKey(stringValue: AttributesType.partitionKeyAttributeName)!)
        sortKey = try values.decode(String.self, forKey: DynamoDBAttributesTypeCodingKey(stringValue: AttributesType.sortKeyAttributeName)!)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: DynamoDBAttributesTypeCodingKey.self)
        try container.encode(partitionKey, forKey: DynamoDBAttributesTypeCodingKey(stringValue: AttributesType.partitionKeyAttributeName)!)
        try container.encode(sortKey, forKey: DynamoDBAttributesTypeCodingKey(stringValue: AttributesType.sortKeyAttributeName)!)
    }
}
