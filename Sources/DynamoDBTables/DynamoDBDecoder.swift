//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/DynamoDBDecoder.swift
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
//  DynamoDBDecoder.swift
//  DynamoDBTables
//

import Foundation
import DynamoDBModel

public class DynamoDBDecoder {
    internal let attributeNameTransform: ((String) -> String)?

    public init(attributeNameTransform: ((String) -> String)? = nil) {
        self.attributeNameTransform = attributeNameTransform
    }
    
    public func decode<T: Swift.Decodable>(_ value: DynamoDBModel.AttributeValue, userInfo: [CodingUserInfoKey: Any] = [:]) throws -> T {
        let container = InternalSingleValueDecodingContainer(attributeValue: value,
                                                             codingPath: [],
                                                             userInfo: userInfo,
                                                             attributeNameTransform: attributeNameTransform)
        
        return try T(from: container)
    }
}
