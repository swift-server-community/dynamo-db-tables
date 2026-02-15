//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/aws-sdk-swift-main/Sources/SmokeDynamoDB/DynamoDBEncoder.swift
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
//  DynamoDBEncoder.swift
//  DynamoDBTables
//

public class DynamoDBEncoder {
    private let attributeNameTransform: ((String) -> String)?

    public init(attributeNameTransform: ((String) -> String)? = nil) {
        self.attributeNameTransform = attributeNameTransform
    }

    public func encode(
        _ value: some Swift.Encodable,
        userInfo: [CodingUserInfoKey: Any] = [:]
    ) throws -> DynamoDBModel.AttributeValue {
        let container = InternalSingleValueEncodingContainer(
            userInfo: userInfo,
            codingPath: [],
            attributeNameTransform: attributeNameTransform,
            defaultValue: nil
        )
        try value.encode(to: container)

        return container.attributeValue
    }
}

protocol AttributeValueConvertable {
    var attributeValue: DynamoDBModel.AttributeValue { get }
}

extension DynamoDBModel.AttributeValue: AttributeValueConvertable {
    var attributeValue: DynamoDBModel.AttributeValue {
        self
    }
}

enum ContainerValueType {
    case singleValue(AttributeValueConvertable)
    case unkeyedContainer([AttributeValueConvertable])
    case keyedContainer([String: AttributeValueConvertable])
}
