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
//  DynamoDBEncoder.swift
//  DynamoDBTables
//

import Foundation
import DynamoDBModel

public class DynamoDBEncoder {
    private let attributeNameTransform: ((String) -> String)?

    public init(attributeNameTransform: ((String) -> String)? = nil) {
        self.attributeNameTransform = attributeNameTransform
    }
    
    public func encode<T: Swift.Encodable>(_ value: T, userInfo: [CodingUserInfoKey: Any] = [:]) throws -> DynamoDBModel.AttributeValue {
        let container = InternalSingleValueEncodingContainer(userInfo: userInfo,
                                                             codingPath: [],
                                                             attributeNameTransform: attributeNameTransform,
                                                             defaultValue: nil)
        try value.encode(to: container)
        
        return container.attributeValue
    }
}

internal protocol AttributeValueConvertable {
    var attributeValue: DynamoDBModel.AttributeValue { get }
}

extension DynamoDBModel.AttributeValue: AttributeValueConvertable {
    var attributeValue: DynamoDBModel.AttributeValue {
        return self
    }
}

internal enum ContainerValueType {
    case singleValue(AttributeValueConvertable)
    case unkeyedContainer([AttributeValueConvertable])
    case keyedContainer([String: AttributeValueConvertable])
}
