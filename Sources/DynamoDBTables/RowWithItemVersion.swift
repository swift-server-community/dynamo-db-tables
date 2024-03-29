//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/RowWithItemVersion.swift
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
//  RowWithItemVersion.swift
//  DynamoDBTables
//

import Foundation

public struct RowWithItemVersion<RowType: Codable>: Codable, CustomRowTypeIdentifier {

    public static var rowTypeIdentifier: String? {
        let rowTypeIdentity = getTypeRowIdentifier(type: RowType.self)
        
        return "\(rowTypeIdentity)WithItemVersion"
    }
    
    enum CodingKeys: String, CodingKey {
        case itemVersion = "ItemVersion"
    }
    
    public let itemVersion: Int
    public let rowValue: RowType
    
    public static func newItem(withVersion itemVersion: Int = 1,
                               withValue rowValue: RowType) -> RowWithItemVersion<RowType> {
        return RowWithItemVersion<RowType>(itemVersion: itemVersion,
                                           rowValue: rowValue)
    }
    
    public func createUpdatedItem(withVersion itemVersion: Int? = nil,
                                  withValue newRowValue: RowType) -> RowWithItemVersion<RowType> {
        return RowWithItemVersion<RowType>(itemVersion: itemVersion != nil ? itemVersion! : self.itemVersion + 1,
                                           rowValue: newRowValue)
    }
    
    init(itemVersion: Int,
         rowValue: RowType) {
        self.itemVersion = itemVersion
        self.rowValue = rowValue
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        self.itemVersion = try values.decode(Int.self, forKey: .itemVersion)
        
        self.rowValue = try RowType(from: decoder)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(itemVersion, forKey: .itemVersion)
        
        try rowValue.encode(to: encoder)
    }
}
