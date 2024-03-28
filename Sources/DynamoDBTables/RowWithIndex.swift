//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from https://github.com/amzn/smoke-dynamodb. Any commits
// prior to February 2024
// Copyright 2018-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
//  RowWithIndex.swift
//  DynamoDBTables
//

import Foundation

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

    public init?(intValue: Int) {
        return nil
    }

    static let index = InternalDynamoDBCodingKey(stringValue: "super")!
}

public func createRowWithIndexCodingKey(stringValue: String) -> RowWithIndexCodingKey {
    return RowWithIndexCodingKey.init(stringValue: stringValue)!
}

public struct RowWithIndex<RowType: Codable, Identity: IndexIdentity>: Codable, CustomRowTypeIdentifier {
    
    public static var rowTypeIdentifier: String? {
        let rowTypeIdentity = getTypeRowIdentifier(type: RowType.self)
        let indexIdentity = Identity.identity
        
        return "\(rowTypeIdentity)With\(indexIdentity)Index"
    }
    
    public let indexValue: String
    public let rowValue: RowType
    
    public static func newItem(withIndex indexValue: String,
                               andValue rowValue: RowType) -> RowWithIndex<RowType, Identity> {
        return RowWithIndex<RowType, Identity>(indexValue: indexValue,
                                               rowValue: rowValue)
    }
    
    public func createUpdatedItem(withValue newRowValue: RowType) -> RowWithIndex<RowType, Identity> {
        return RowWithIndex<RowType, Identity>(indexValue: indexValue,
                                               rowValue: newRowValue)
    }
    
    init(indexValue: String,
         rowValue: RowType) {
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
        try container.encode(indexValue, forKey: Identity.codingKey)
        
        try rowValue.encode(to: encoder)
    }
}
