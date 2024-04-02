//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/aws-sdk-swift-main/Sources/SmokeDynamoDB/InternalKeyedEncodingContainer.swift
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
//  InternalKeyedEncodingContainer.swift
//  DynamoDBTables
//

import AWSDynamoDB

internal struct InternalKeyedEncodingContainer<K: CodingKey>: KeyedEncodingContainerProtocol {
    typealias Key = K

    private let enclosingContainer: InternalSingleValueEncodingContainer
    
    init(enclosingContainer: InternalSingleValueEncodingContainer) {
        self.enclosingContainer = enclosingContainer
    }

    // MARK: - Swift.KeyedEncodingContainerProtocol Methods
    
    var codingPath: [CodingKey] {
        return enclosingContainer.codingPath
    }

    func encodeNil(forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBClientTypes.AttributeValue.null(true))
    }
    
    func encode(_ value: Bool, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBClientTypes.AttributeValue.bool(value))
    }
    
    func encode(_ value: Int, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: Int8, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: Int16, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: Int32, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: Int64, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: UInt, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: UInt8, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: UInt16, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: UInt32, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: UInt64, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: Float, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: Double, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: String, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBClientTypes.AttributeValue.s(value)) }
    
    func encode<T>(_ value: T, forKey key: Key)   throws where T: Encodable {
        let nestedContainer = createNestedContainer(for: key)
        
        try nestedContainer.encode(value)
    }

    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type,
                                    forKey key: Key) -> KeyedEncodingContainer<NestedKey> {
        let nestedContainer = createNestedContainer(for: key, defaultValue: .keyedContainer([:]))
        
        let nestedKeyContainer = InternalKeyedEncodingContainer<NestedKey>(enclosingContainer: nestedContainer)
        
        return KeyedEncodingContainer<NestedKey>(nestedKeyContainer)
    }

    func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedEncodingContainer {
        let nestedContainer = createNestedContainer(for: key, defaultValue: .unkeyedContainer([]))
        
        let nestedKeyContainer = InternalUnkeyedEncodingContainer(enclosingContainer: nestedContainer)
        
        return nestedKeyContainer
    }

    func superEncoder() -> Encoder { return createNestedContainer(for: InternalDynamoDBCodingKey.super) }
    func superEncoder(forKey key: Key) -> Encoder { return createNestedContainer(for: key) }

    // MARK: -
    private func createNestedContainer<NestedKey: CodingKey>(for key: NestedKey,
                                                             defaultValue: ContainerValueType? = nil)
        -> InternalSingleValueEncodingContainer {
        let nestedContainer = InternalSingleValueEncodingContainer(userInfo: enclosingContainer.userInfo,
                                                                   codingPath: enclosingContainer.codingPath + [key],
                                                                   attributeNameTransform: enclosingContainer.attributeNameTransform,
                                                                   defaultValue: defaultValue)
        enclosingContainer.addToKeyedContainer(key: key, value: nestedContainer)
        
        return nestedContainer
    }
}
