//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/aws-sdk-swift-main/Sources/SmokeDynamoDB/InternalSingleValueEncodingContainer.swift
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
//  InternalSingleValueEncodingContainer.swift
//  DynamoDBTables
//

import Foundation
import AWSDynamoDB
import Logging

internal class InternalSingleValueEncodingContainer: SingleValueEncodingContainer {
    internal private(set) var containerValue: ContainerValueType?
    internal let attributeNameTransform: ((String) -> String)?
    
    let codingPath: [CodingKey]
    let userInfo: [CodingUserInfoKey: Any]
    
    init(userInfo: [CodingUserInfoKey: Any],
         codingPath: [CodingKey],
         attributeNameTransform: ((String) -> String)?,
         defaultValue: ContainerValueType?) {
        self.containerValue = defaultValue
        self.userInfo = userInfo
        self.codingPath = codingPath
        self.attributeNameTransform = attributeNameTransform
    }
    
    func encodeNil() throws {
        containerValue = .singleValue(DynamoDBClientTypes.AttributeValue.null(true))
    }
    
    func encode(_ value: Bool) throws {
        containerValue = .singleValue(DynamoDBClientTypes.AttributeValue.bool(value))
    }
    
    func encode(_ value: Int) throws {
        containerValue = .singleValue(DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: Int8) throws {
        containerValue = .singleValue(DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: Int16) throws {
        containerValue = .singleValue(DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: Int32) throws {
        containerValue = .singleValue(DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: Int64) throws {
        containerValue = .singleValue(DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: UInt) throws {
        containerValue = .singleValue(DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: UInt8) throws {
        containerValue = .singleValue(DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: UInt16) throws {
        containerValue = .singleValue(DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: UInt32) throws {
        containerValue = .singleValue(DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: UInt64) throws {
        containerValue = .singleValue(DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: Float) throws {
        containerValue = .singleValue(DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: Double) throws {
        containerValue = .singleValue(DynamoDBClientTypes.AttributeValue.n(String(value)))
    }
    
    func encode(_ value: String) throws {
        containerValue = .singleValue(DynamoDBClientTypes.AttributeValue.s(value))
    }

    func encode<T>(_ value: T) throws where T: Encodable {
        if let date = value as? Foundation.Date {
            let dateAsString = date.iso8601
            
            containerValue = .singleValue(DynamoDBClientTypes.AttributeValue.s(dateAsString))
            return
        }
        
        try value.encode(to: self)
    }
    
    func addToKeyedContainer<KeyType: CodingKey>(key: KeyType, value: AttributeValueConvertable) {
        guard let currentContainerValue = containerValue else {
            fatalError("Attempted to add a keyed item to an unitinialized container.")
        }
        
        guard case .keyedContainer(var values) = currentContainerValue else {
            fatalError("Expected keyed container and there wasn't one.")
        }
        
        let attributeName = getAttributeName(key: key)
        
        values[attributeName] = value
        
        containerValue = .keyedContainer(values)
    }
    
    func addToUnkeyedContainer(value: AttributeValueConvertable) {
        guard let currentContainerValue = containerValue else {
            fatalError("Attempted to ad an unkeyed item to an uninitialized container.")
        }
        
        guard case .unkeyedContainer(var values) = currentContainerValue else {
            fatalError("Expected unkeyed container and there wasn't one.")
        }
        
        values.append(value)
        
        containerValue = .unkeyedContainer(values)
    }
    
    private func getAttributeName(key: CodingKey) -> String {
        let attributeName: String
        if let attributeNameTransform = attributeNameTransform {
            attributeName = attributeNameTransform(key.stringValue)
        } else {
            attributeName = key.stringValue
        }
        
        return attributeName
    }
}

extension InternalSingleValueEncodingContainer: AttributeValueConvertable {
    var attributeValue: DynamoDBClientTypes.AttributeValue {
        guard let containerValue = containerValue else {
            fatalError("Attempted to access uninitialized container.")
        }
        
        switch containerValue {
        case .singleValue(let value):
            return value.attributeValue
        case .unkeyedContainer(let values):
            let mappedValues = values.map { value in value.attributeValue }
            
            return DynamoDBClientTypes.AttributeValue.l(mappedValues)
        case .keyedContainer(let values):
            let mappedValues = values.mapValues { value in value.attributeValue }
        
            return DynamoDBClientTypes.AttributeValue.m(mappedValues)
        }
    }
}

extension InternalSingleValueEncodingContainer: Swift.Encoder {
    var unkeyedContainerCount: Int {
        guard let containerValue = containerValue else {
            fatalError("Attempted to access unitialized container.")
        }
        
        guard case .unkeyedContainer(let values) = containerValue else {
            fatalError("Expected unkeyed container and there wasn't one.")
        }
        
        return values.count
    }
    
    func container<Key>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> where Key: CodingKey {
        
        // if there container is already initialized
        if let currentContainerValue = containerValue {
            guard case .keyedContainer = currentContainerValue else {
                fatalError("Trying to use an already initialized container as a keyed container.")
            }
        } else {
            containerValue = .keyedContainer([:])
        }
        
        let container = InternalKeyedEncodingContainer<Key>(enclosingContainer: self)
        
        return KeyedEncodingContainer<Key>(container)
    }
    
    func unkeyedContainer() -> UnkeyedEncodingContainer {
        
        // if there container is already initialized
        if let currentContainerValue = containerValue {
            guard case .unkeyedContainer = currentContainerValue else {
                fatalError("Trying to use an already initialized container as an unkeyed container.")
            }
        } else {
            containerValue = .unkeyedContainer([])
        }
        
        let container = InternalUnkeyedEncodingContainer(enclosingContainer: self)
        
        return container
    }
    
    func singleValueContainer() -> SingleValueEncodingContainer {
        return self
    }
}
