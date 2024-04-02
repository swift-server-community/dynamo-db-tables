//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/aws-sdk-swift-main/Sources/SmokeDynamoDB/InternalKeyedDecodingContainer.swift
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
//  InternalKeyedDecodingContainer.swift
//  DynamoDBTables
//

import Foundation
import AWSDynamoDB

internal struct InternalKeyedDecodingContainer<K: CodingKey>: KeyedDecodingContainerProtocol {
    typealias Key = K

    private let decodingContainer: InternalSingleValueDecodingContainer
    
    init(decodingContainer: InternalSingleValueDecodingContainer) {
        self.decodingContainer = decodingContainer
    }

    // MARK: - Swift.KeyedEncodingContainerProtocol Methods
    
    var codingPath: [CodingKey] {
        return decodingContainer.codingPath
    }
    
    func decodeNil(forKey key: Key) throws -> Bool {
        return try createNestedContainer(for: key).decodeNil()
    }

    func decode(_ type: Bool.Type, forKey key: Key)   throws -> Bool {
        return try createNestedContainer(for: key).decode(Bool.self)
    }
    func decode(_ type: Int.Type, forKey key: Key)    throws -> Int {
        return try createNestedContainer(for: key).decode(Int.self)
    }
    
    func decode(_ type: Int8.Type, forKey key: Key)   throws -> Int8 {
        return try createNestedContainer(for: key).decode(Int8.self)
    }
    
    func decode(_ type: Int16.Type, forKey key: Key)  throws -> Int16 {
        return try createNestedContainer(for: key).decode(Int16.self)
    }
    
    func decode(_ type: Int32.Type, forKey key: Key)  throws -> Int32 {
        return try createNestedContainer(for: key).decode(Int32.self)
    }
    
    func decode(_ type: Int64.Type, forKey key: Key)  throws -> Int64 {
        return try createNestedContainer(for: key).decode(Int64.self)
    }
    
    func decode(_ type: UInt.Type, forKey key: Key)   throws -> UInt {
        return try createNestedContainer(for: key).decode(UInt.self)
    }
    
    func decode(_ type: UInt8.Type, forKey key: Key)  throws -> UInt8 {
        return try createNestedContainer(for: key).decode(UInt8.self)
    }
    
    func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 {
        return try createNestedContainer(for: key).decode(UInt16.self)
    }
    
    func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 {
        return try createNestedContainer(for: key).decode(UInt32.self)
    }
    
    func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 {
        return try createNestedContainer(for: key).decode(UInt64.self)
    }
    
    func decode(_ type: Float.Type, forKey key: Key)  throws -> Float {
        return try createNestedContainer(for: key).decode(Float.self)
    }
    
    func decode(_ type: Double.Type, forKey key: Key) throws -> Double {
        return try createNestedContainer(for: key).decode(Double.self)
    }
    
    func decode(_ type: String.Type, forKey key: Key) throws -> String {
        return try createNestedContainer(for: key).decode(String.self)
    }
    
    func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T: Decodable {
        return try createNestedContainer(for: key).decode(type)
    }
    
    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: K) throws
        -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
        return try createNestedContainer(for: key).container(keyedBy: type)
    }
    
    func nestedUnkeyedContainer(forKey key: K) throws -> UnkeyedDecodingContainer {
        return try createNestedContainer(for: key).unkeyedContainer()
    }

    private func getValues() -> [String: DynamoDBClientTypes.AttributeValue] {
        guard case .m(let values) = decodingContainer.attributeValue else {
            fatalError("Expected keyed container and there wasn't one.")
        }
        
        return values
    }
    
    var allKeys: [K] {
        return getValues().keys.map { key in K(stringValue: key) }
            .filter { key in key != nil }
            .map { key in key! }
    }
    
    func contains(_ key: K) -> Bool {
        let attributeName = getAttributeName(key: key)
        
        return getValues()[attributeName] != nil
    }
    
    func superDecoder() throws -> Decoder {
        return try createNestedContainer(for: InternalDynamoDBCodingKey.super)
    }
    
    func superDecoder(forKey key: K) throws -> Decoder {
        return try createNestedContainer(for: key)
    }
    
    // MARK: -

    private func createNestedContainer(for key: CodingKey) throws -> InternalSingleValueDecodingContainer {
        guard case .m(let values) = decodingContainer.attributeValue else {
            let description = "Expected to decode a map."
            let context = DecodingError.Context(codingPath: codingPath, debugDescription: description)
            throw DecodingError.dataCorrupted(context)
        }
        
        let attributeName = getAttributeName(key: key)
        
        guard let value = values[attributeName] else {
            let description = "Could not find value for \(key) and attributeName '\(attributeName)'."
            let context = DecodingError.Context(codingPath: codingPath, debugDescription: description)
            throw DecodingError.keyNotFound(key, context)
        }
        
        return InternalSingleValueDecodingContainer(attributeValue: value, codingPath: decodingContainer.codingPath + [key],
                                                    userInfo: decodingContainer.userInfo,
                                                    attributeNameTransform: decodingContainer.attributeNameTransform)
    }
    
    private func getAttributeName(key: CodingKey) -> String {
        let attributeName: String
        if let attributeNameTransform = decodingContainer.attributeNameTransform {
            attributeName = attributeNameTransform(key.stringValue)
        } else {
            attributeName = key.stringValue
        }
        
        return attributeName
    }
}
