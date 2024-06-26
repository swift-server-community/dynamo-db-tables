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

import AWSDynamoDB
import Foundation

struct InternalKeyedDecodingContainer<K: CodingKey>: KeyedDecodingContainerProtocol {
    typealias Key = K

    private let decodingContainer: InternalSingleValueDecodingContainer

    init(decodingContainer: InternalSingleValueDecodingContainer) {
        self.decodingContainer = decodingContainer
    }

    // MARK: - Swift.KeyedEncodingContainerProtocol Methods

    var codingPath: [CodingKey] {
        self.decodingContainer.codingPath
    }

    func decodeNil(forKey key: Key) throws -> Bool {
        try self.createNestedContainer(for: key).decodeNil()
    }

    func decode(_: Bool.Type, forKey key: Key) throws -> Bool {
        try self.createNestedContainer(for: key).decode(Bool.self)
    }

    func decode(_: Int.Type, forKey key: Key) throws -> Int {
        try self.createNestedContainer(for: key).decode(Int.self)
    }

    func decode(_: Int8.Type, forKey key: Key) throws -> Int8 {
        try self.createNestedContainer(for: key).decode(Int8.self)
    }

    func decode(_: Int16.Type, forKey key: Key) throws -> Int16 {
        try self.createNestedContainer(for: key).decode(Int16.self)
    }

    func decode(_: Int32.Type, forKey key: Key) throws -> Int32 {
        try self.createNestedContainer(for: key).decode(Int32.self)
    }

    func decode(_: Int64.Type, forKey key: Key) throws -> Int64 {
        try self.createNestedContainer(for: key).decode(Int64.self)
    }

    func decode(_: UInt.Type, forKey key: Key) throws -> UInt {
        try self.createNestedContainer(for: key).decode(UInt.self)
    }

    func decode(_: UInt8.Type, forKey key: Key) throws -> UInt8 {
        try self.createNestedContainer(for: key).decode(UInt8.self)
    }

    func decode(_: UInt16.Type, forKey key: Key) throws -> UInt16 {
        try self.createNestedContainer(for: key).decode(UInt16.self)
    }

    func decode(_: UInt32.Type, forKey key: Key) throws -> UInt32 {
        try self.createNestedContainer(for: key).decode(UInt32.self)
    }

    func decode(_: UInt64.Type, forKey key: Key) throws -> UInt64 {
        try self.createNestedContainer(for: key).decode(UInt64.self)
    }

    func decode(_: Float.Type, forKey key: Key) throws -> Float {
        try self.createNestedContainer(for: key).decode(Float.self)
    }

    func decode(_: Double.Type, forKey key: Key) throws -> Double {
        try self.createNestedContainer(for: key).decode(Double.self)
    }

    func decode(_: String.Type, forKey key: Key) throws -> String {
        try self.createNestedContainer(for: key).decode(String.self)
    }

    func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T: Decodable {
        try self.createNestedContainer(for: key).decode(type)
    }

    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: K) throws
        -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey
    {
        try self.createNestedContainer(for: key).container(keyedBy: type)
    }

    func nestedUnkeyedContainer(forKey key: K) throws -> UnkeyedDecodingContainer {
        try self.createNestedContainer(for: key).unkeyedContainer()
    }

    private func getValues() -> [String: DynamoDBClientTypes.AttributeValue] {
        guard case let .m(values) = decodingContainer.attributeValue else {
            fatalError("Expected keyed container and there wasn't one.")
        }

        return values
    }

    var allKeys: [K] {
        self.getValues().keys.map { key in K(stringValue: key) }
            .filter { key in key != nil }
            .map { key in key! }
    }

    func contains(_ key: K) -> Bool {
        let attributeName = self.getAttributeName(key: key)

        return self.getValues()[attributeName] != nil
    }

    func superDecoder() throws -> Decoder {
        try self.createNestedContainer(for: InternalDynamoDBCodingKey.super)
    }

    func superDecoder(forKey key: K) throws -> Decoder {
        try self.createNestedContainer(for: key)
    }

    // MARK: -

    private func createNestedContainer(for key: CodingKey) throws -> InternalSingleValueDecodingContainer {
        guard case let .m(values) = decodingContainer.attributeValue else {
            let description = "Expected to decode a map."
            let context = DecodingError.Context(codingPath: self.codingPath, debugDescription: description)
            throw DecodingError.dataCorrupted(context)
        }

        let attributeName = self.getAttributeName(key: key)

        guard let value = values[attributeName] else {
            let description = "Could not find value for \(key) and attributeName '\(attributeName)'."
            let context = DecodingError.Context(codingPath: self.codingPath, debugDescription: description)
            throw DecodingError.keyNotFound(key, context)
        }

        return InternalSingleValueDecodingContainer(attributeValue: value, codingPath: self.decodingContainer.codingPath + [key],
                                                    userInfo: self.decodingContainer.userInfo,
                                                    attributeNameTransform: self.decodingContainer.attributeNameTransform)
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
