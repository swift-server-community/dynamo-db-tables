//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/aws-sdk-swift-main/Sources/SmokeDynamoDB/InternalUnkeyedDecodingContainer.swift
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
//  InternalUnkeyedDecodingContainer.swift
//  DynamoDBTables
//

import Foundation

struct InternalUnkeyedDecodingContainer: UnkeyedDecodingContainer {
    private let decodingContainer: InternalSingleValueDecodingContainer
    private(set) var currentIndex: Int

    init(decodingContainer: InternalSingleValueDecodingContainer) {
        self.decodingContainer = decodingContainer
        self.currentIndex = 0
    }

    // MARK: - Swift.UnkeyedEncodingContainer Methods

    var codingPath: [CodingKey] {
        self.decodingContainer.codingPath
    }

    mutating func decodeNil() throws -> Bool {
        try self.createNestedContainer().decodeNil()
    }

    mutating func decode(_: Bool.Type) throws -> Bool {
        try self.createNestedContainer().decode(Bool.self)
    }

    mutating func decode(_: Int.Type) throws -> Int {
        try self.createNestedContainer().decode(Int.self)
    }

    mutating func decode(_: Int8.Type) throws -> Int8 {
        try self.createNestedContainer().decode(Int8.self)
    }

    mutating func decode(_: Int16.Type) throws -> Int16 {
        try self.createNestedContainer().decode(Int16.self)
    }

    mutating func decode(_: Int32.Type) throws -> Int32 {
        try self.createNestedContainer().decode(Int32.self)
    }

    mutating func decode(_: Int64.Type) throws -> Int64 {
        try self.createNestedContainer().decode(Int64.self)
    }

    mutating func decode(_: UInt.Type) throws -> UInt {
        try self.createNestedContainer().decode(UInt.self)
    }

    mutating func decode(_: UInt8.Type) throws -> UInt8 {
        try self.createNestedContainer().decode(UInt8.self)
    }

    mutating func decode(_: UInt16.Type) throws -> UInt16 {
        try self.createNestedContainer().decode(UInt16.self)
    }

    mutating func decode(_: UInt32.Type) throws -> UInt32 {
        try self.createNestedContainer().decode(UInt32.self)
    }

    mutating func decode(_: UInt64.Type) throws -> UInt64 {
        try self.createNestedContainer().decode(UInt64.self)
    }

    mutating func decode(_: Float.Type) throws -> Float {
        try self.createNestedContainer().decode(Float.self)
    }

    mutating func decode(_: Double.Type) throws -> Double {
        try self.createNestedContainer().decode(Double.self)
    }

    mutating func decode(_: String.Type) throws -> String {
        try self.createNestedContainer().decode(String.self)
    }

    mutating func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        try self.createNestedContainer().decode(type)
    }

    var count: Int? {
        guard case let .l(values) = decodingContainer.attributeValue else {
            return nil
        }

        return values.count
    }

    var isAtEnd: Bool {
        guard case let .l(values) = decodingContainer.attributeValue else {
            return true
        }

        return self.currentIndex >= values.count
    }

    mutating func nestedContainer<NestedKey>(
        keyedBy type: NestedKey.Type
    ) throws
        -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey
    {
        try self.createNestedContainer().container(keyedBy: type)
    }

    mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        try self.createNestedContainer().unkeyedContainer()
    }

    mutating func superDecoder() throws -> Decoder {
        try self.createNestedContainer()
    }

    // MARK: -

    private mutating func createNestedContainer() throws -> InternalSingleValueDecodingContainer {
        let index = self.currentIndex
        self.currentIndex += 1

        guard case let .l(values) = decodingContainer.attributeValue else {
            let description = "Expected to decode a list."
            let context = DecodingError.Context(codingPath: self.codingPath, debugDescription: description)
            throw DecodingError.dataCorrupted(context)
        }

        guard index < values.count else {
            let description = "Could not find key for index \(index)."
            let context = DecodingError.Context(codingPath: self.codingPath, debugDescription: description)
            throw DecodingError.valueNotFound(Any.self, context)
        }

        let value = values[index]

        return InternalSingleValueDecodingContainer(
            attributeValue: value,
            codingPath: self.decodingContainer.codingPath
                + [InternalDynamoDBCodingKey(index: index)],
            userInfo: self.decodingContainer.userInfo,
            attributeNameTransform: self.decodingContainer.attributeNameTransform
        )
    }
}

private func createISO8601DateFormatter() -> DateFormatter {
    let formatter = DateFormatter()
    formatter.calendar = Calendar(identifier: .iso8601)
    formatter.locale = Locale(identifier: "en_US_POSIX")
    formatter.timeZone = TimeZone(secondsFromGMT: 0)
    formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXXXX"
    return formatter
}

extension Date {
    var iso8601: String {
        createISO8601DateFormatter().string(from: self)
    }
}

extension String {
    var dateFromISO8601: Date? {
        createISO8601DateFormatter().date(from: self)  // "Mar 22, 2017, 10:22 AM"
    }
}
