//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/aws-sdk-swift-main/Sources/SmokeDynamoDB/InternalSingleValueDecodingContainer.swift
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
//  InternalSingleValueDecodingContainer.swift
//  DynamoDBTables
//

import Foundation
import AWSDynamoDB

internal struct InternalSingleValueDecodingContainer {
    internal let codingPath: [CodingKey]
    internal let userInfo: [CodingUserInfoKey: Any]
    internal let attributeValue: DynamoDBClientTypes.AttributeValue
    internal let attributeNameTransform: ((String) -> String)?
    
    init(attributeValue: DynamoDBClientTypes.AttributeValue,
         codingPath: [CodingKey],
         userInfo: [CodingUserInfoKey: Any],
         attributeNameTransform: ((String) -> String)?) {
        self.attributeValue = attributeValue
        self.codingPath = codingPath
        self.userInfo = userInfo
        self.attributeNameTransform = attributeNameTransform
    }
}

extension InternalSingleValueDecodingContainer: SingleValueDecodingContainer {
    func decodeNil() -> Bool {
        guard case .null(let value) = attributeValue else {
            return false
        }
                
        return value
    }
    
    func decode(_ type: Bool.Type)   throws -> Bool {
        guard case .bool(let value) = attributeValue else {
            throw getTypeMismatchError(expectation: Bool.self)
        }
        
        return value
    }
    
    func decode(_ type: Int.Type)    throws -> Int {
        guard case .n(let valueAsString) = attributeValue,
            let value = Int(valueAsString) else {
            throw getTypeMismatchError(expectation: Int.self)
        }
        
        return value
    }
    
    func decode(_ type: Int8.Type)   throws -> Int8 {
        guard case .n(let valueAsString) = attributeValue,
            let value = Int8(valueAsString) else {
            throw getTypeMismatchError(expectation: Int8.self)
        }
        
        return value
    }
    
    func decode(_ type: Int16.Type)  throws -> Int16 {
        guard case .n(let valueAsString) = attributeValue,
            let value = Int16(valueAsString) else {
            throw getTypeMismatchError(expectation: Int16.self)
        }
        
        return value
    }
    
    func decode(_ type: Int32.Type)  throws -> Int32 {
        guard case .n(let valueAsString) = attributeValue,
            let value = Int32(valueAsString) else {
            throw getTypeMismatchError(expectation: Int32.self)
        }
        
        return value
    }
    
    func decode(_ type: Int64.Type)  throws -> Int64 {
        guard case .n(let valueAsString) = attributeValue,
            let value = Int64(valueAsString) else {
            throw getTypeMismatchError(expectation: Int64.self)
        }
        
        return value
    }
    
    func decode(_ type: UInt.Type)   throws -> UInt {
        guard case .n(let valueAsString) = attributeValue,
            let value = UInt(valueAsString) else {
            throw getTypeMismatchError(expectation: UInt.self)
        }
        
        return value
    }
    
    func decode(_ type: UInt8.Type)  throws -> UInt8 {
        guard case .n(let valueAsString) = attributeValue,
            let value = UInt8(valueAsString) else {
            throw getTypeMismatchError(expectation: UInt8.self)
        }
        
        return value
    }
    
    func decode(_ type: UInt16.Type) throws -> UInt16 {
        guard case .n(let valueAsString) = attributeValue,
            let value = UInt16(valueAsString) else {
            throw getTypeMismatchError(expectation: UInt16.self)
        }
        
        return value
    }
    
    func decode(_ type: UInt32.Type) throws -> UInt32 {
        guard case .n(let valueAsString) = attributeValue,
            let value = UInt32(valueAsString) else {
            throw getTypeMismatchError(expectation: UInt32.self)
        }
        
        return value
    }
    
    func decode(_ type: UInt64.Type) throws -> UInt64 {
        guard case .n(let valueAsString) = attributeValue,
            let value = UInt64(valueAsString) else {
            throw getTypeMismatchError(expectation: UInt64.self)
        }
        
        return value
    }
    
    func decode(_ type: Float.Type)  throws -> Float {
        guard case .n(let valueAsString) = attributeValue,
            let value = Float(valueAsString) else {
            throw getTypeMismatchError(expectation: Float.self)
        }
        
        return value
    }
    
    func decode(_ type: Double.Type) throws -> Double {
        guard case .n(let valueAsString) = attributeValue,
            let value = Double(valueAsString) else {
            throw getTypeMismatchError(expectation: Double.self)
        }
        
        return value
    }
    
    func decode(_ type: String.Type) throws -> String {
        guard case .s(let value) = attributeValue else {
            throw getTypeMismatchError(expectation: String.self)
        }
        
        return value
    }
    
    func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        if type == Date.self {
            let dateAsString = try String(from: self)
            
            guard let date = dateAsString.dateFromISO8601 as? T else {
                throw getTypeMismatchError(expectation: Date.self)
            }
            
            return date
        }
        
        return try T(from: self)
    }
    
    private func getTypeMismatchError(expectation: Any.Type) -> DecodingError {
        let description = "Expected to decode \(expectation)."
        let context = DecodingError.Context(codingPath: codingPath, debugDescription: description)
        
        return DecodingError.typeMismatch(expectation, context)
    }
}

extension InternalSingleValueDecodingContainer: Swift.Decoder {
    func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key: CodingKey {
        let container = InternalKeyedDecodingContainer<Key>(decodingContainer: self)
        
        return KeyedDecodingContainer<Key>(container)
    }
    
    func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        let container = InternalUnkeyedDecodingContainer(decodingContainer: self)
        
        return container
    }
    
    func singleValueContainer() throws -> SingleValueDecodingContainer {
        return self
    }
}