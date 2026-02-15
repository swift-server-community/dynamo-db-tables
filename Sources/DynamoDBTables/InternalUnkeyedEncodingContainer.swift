//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/aws-sdk-swift-main/Sources/SmokeDynamoDB/InternalUnkeyedEncodingContainer.swift
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
//  InternalUnkeyedEncodingContainer.swift
//  DynamoDBTables
//

struct InternalUnkeyedEncodingContainer: UnkeyedEncodingContainer {
    private let enclosingContainer: InternalSingleValueEncodingContainer

    init(enclosingContainer: InternalSingleValueEncodingContainer) {
        self.enclosingContainer = enclosingContainer
    }

    // MARK: - Swift.UnkeyedEncodingContainer Methods

    var codingPath: [CodingKey] {
        self.enclosingContainer.codingPath
    }

    var count: Int { self.enclosingContainer.unkeyedContainerCount }

    func encodeNil() throws {
        self.enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue.null(true))
    }

    func encode(_ value: Bool) throws {
        self.enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue.bool(value))
    }

    func encode(_ value: Int) throws {
        self.enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue.n(String(value)))
    }

    func encode(_ value: Int8) throws {
        self.enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue.n(String(value)))
    }

    func encode(_ value: Int16) throws {
        self.enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue.n(String(value)))
    }

    func encode(_ value: Int32) throws {
        self.enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue.n(String(value)))
    }

    func encode(_ value: Int64) throws {
        self.enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue.n(String(value)))
    }

    func encode(_ value: UInt) throws {
        self.enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue.n(String(value)))
    }

    func encode(_ value: UInt8) throws {
        self.enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue.n(String(value)))
    }

    func encode(_ value: UInt16) throws {
        self.enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue.n(String(value)))
    }

    func encode(_ value: UInt32) throws {
        self.enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue.n(String(value)))
    }

    func encode(_ value: UInt64) throws {
        self.enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue.n(String(value)))
    }

    func encode(_ value: Float) throws {
        self.enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue.n(String(value)))
    }

    func encode(_ value: Double) throws {
        self.enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue.n(String(value)))
    }

    func encode(_ value: String) throws {
        self.enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue.s(value))
    }

    func encode(_ value: some Encodable) throws {
        try self.createNestedContainer().encode(value)
    }

    func nestedContainer<NestedKey>(keyedBy _: NestedKey.Type) -> KeyedEncodingContainer<NestedKey> {
        let nestedContainer = self.createNestedContainer(defaultValue: .keyedContainer([:]))

        let nestedKeyContainer = InternalKeyedEncodingContainer<NestedKey>(enclosingContainer: nestedContainer)

        return KeyedEncodingContainer<NestedKey>(nestedKeyContainer)
    }

    func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
        let nestedContainer = self.createNestedContainer(defaultValue: .unkeyedContainer([]))

        let nestedKeyContainer = InternalUnkeyedEncodingContainer(enclosingContainer: nestedContainer)

        return nestedKeyContainer
    }

    func superEncoder() -> Encoder { self.createNestedContainer() }

    // MARK: -

    private func createNestedContainer(
        defaultValue: ContainerValueType? = nil
    )
        -> InternalSingleValueEncodingContainer
    {
        let index = self.enclosingContainer.unkeyedContainerCount

        let nestedContainer = InternalSingleValueEncodingContainer(
            userInfo: enclosingContainer.userInfo,
            codingPath: self.enclosingContainer.codingPath + [InternalDynamoDBCodingKey(index: index)],
            attributeNameTransform: self.enclosingContainer.attributeNameTransform,
            defaultValue: defaultValue
        )
        self.enclosingContainer.addToUnkeyedContainer(value: nestedContainer)

        return nestedContainer
    }
}
