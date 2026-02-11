//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/PolymorphicOperationReturnType.swift
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
//  PolymorphicOperationReturnType.swift
//  DynamoDBTables
//

import AWSDynamoDB
import Foundation

public protocol BatchCapableReturnType {
    associatedtype AttributesType: PrimaryKeyAttributes

    func getItemKey() -> CompositePrimaryKey<AttributesType>
}

public protocol PolymorphicOperationReturnType: Sendable {
    associatedtype AttributesType: PrimaryKeyAttributes
    associatedtype TimeToLiveAttributesType: TimeToLiveAttributes

    static var types: [(Codable.Type, PolymorphicOperationReturnOption<AttributesType, Self, TimeToLiveAttributesType>)]
    { get }
}

public struct PolymorphicOperationReturnOption<
    AttributesType: PrimaryKeyAttributes,
    ReturnType,
    TimeToLiveAttributesType: TimeToLiveAttributes
>: Sendable {
    private let decodingPayloadHandler: @Sendable (Decoder) throws -> ReturnType
    private let typeConvertingPayloadHander: @Sendable (Any) throws -> ReturnType

    public init<RowType: Codable>(
        _ payloadHandler:
            @escaping @Sendable (TypedTTLDatabaseItem<AttributesType, RowType, TimeToLiveAttributesType>)
            -> ReturnType
    ) {
        @Sendable
        func newDecodingPayloadHandler(decoder: Decoder) throws -> ReturnType {
            let typedTTLDatabaseItem: TypedTTLDatabaseItem<AttributesType, RowType, TimeToLiveAttributesType> =
                try TypedTTLDatabaseItem(from: decoder)

            return payloadHandler(typedTTLDatabaseItem)
        }

        @Sendable
        func newTypeConvertingPayloadHandler(input: Any) throws -> ReturnType {
            guard
                let typedTTLDatabaseItem = input
                    as? TypedTTLDatabaseItem<AttributesType, RowType, TimeToLiveAttributesType>
            else {
                let description =
                    "Expected to use item type \(TypedTTLDatabaseItem<AttributesType, RowType, TimeToLiveAttributesType>.self)."
                let context = DecodingError.Context(codingPath: [], debugDescription: description)
                throw DecodingError.typeMismatch(
                    TypedTTLDatabaseItem<AttributesType, RowType, TimeToLiveAttributesType>.self,
                    context
                )
            }

            return payloadHandler(typedTTLDatabaseItem)
        }

        self.decodingPayloadHandler = newDecodingPayloadHandler
        self.typeConvertingPayloadHander = newTypeConvertingPayloadHandler
    }

    func getReturnType(from decoder: Decoder) throws -> ReturnType {
        try self.decodingPayloadHandler(decoder)
    }
}

struct ReturnTypeDecodable<ReturnType: PolymorphicOperationReturnType>: Decodable {
    let decodedValue: ReturnType

    enum CodingKeys: String, CodingKey {
        case rowType = "RowType"
    }

    init(decodedValue: ReturnType) {
        self.decodedValue = decodedValue
    }

    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        let storedRowTypeName = try values.decode(String.self, forKey: .rowType)

        var queryableTypeProviders:
            [String: PolymorphicOperationReturnOption<
                ReturnType.AttributesType, ReturnType, ReturnType.TimeToLiveAttributesType
            >] = [:]
        for (type, provider) in ReturnType.types {
            queryableTypeProviders[getTypeRowIdentifier(type: type)] = provider
        }

        if let provider = queryableTypeProviders[storedRowTypeName] {
            self.decodedValue = try provider.getReturnType(from: decoder)
        } else {
            // throw an exception, we don't know what this type is
            throw DynamoDBTableError.unexpectedType(provided: storedRowTypeName)
        }
    }
}
