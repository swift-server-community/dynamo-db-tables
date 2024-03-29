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
//  PolymorphicOperationReturnType.swift
//  DynamoDBTables
//
import Foundation
import SmokeHTTPClient
import DynamoDBModel

public protocol BatchCapableReturnType {
    associatedtype AttributesType: PrimaryKeyAttributes
    
    func getItemKey() -> CompositePrimaryKey<AttributesType>
}

public protocol PolymorphicOperationReturnType {
    associatedtype AttributesType: PrimaryKeyAttributes
        
    static var types: [(Codable.Type, PolymorphicOperationReturnOption<AttributesType, Self>)] { get }
}

public struct PolymorphicOperationReturnOption<AttributesType: PrimaryKeyAttributes, ReturnType> {
    private let decodingPayloadHandler: (Decoder) throws -> ReturnType
    private let typeConvertingPayloadHander: (Any) throws -> ReturnType
    
    public init<RowType: Codable>(
        _ payloadHandler: @escaping (TypedDatabaseItem<AttributesType, RowType>) -> ReturnType) {
        func newDecodingPayloadHandler(decoder: Decoder) throws -> ReturnType {
            let typedDatabaseItem: TypedDatabaseItem<AttributesType, RowType> = try TypedDatabaseItem(from: decoder)
            
            return payloadHandler(typedDatabaseItem)
        }
        
        func newTypeConvertingPayloadHandler(input: Any) throws -> ReturnType {
            guard let typedDatabaseItem = input as? TypedDatabaseItem<AttributesType, RowType> else {
                let description = "Expected to use item type \(TypedDatabaseItem<AttributesType, RowType>.self)."
                let context = DecodingError.Context(codingPath: [], debugDescription: description)
                throw DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, RowType>.self, context)
            }
                        
            return payloadHandler(typedDatabaseItem)
        }
        
        self.decodingPayloadHandler = newDecodingPayloadHandler
        self.typeConvertingPayloadHander = newTypeConvertingPayloadHandler
    }
    
    internal func getReturnType(from decoder: Decoder) throws -> ReturnType {
        return try self.decodingPayloadHandler(decoder)
    }
    
    internal func getReturnType(input: Any) throws -> ReturnType {
        return try self.typeConvertingPayloadHander(input)
    }
}

internal struct ReturnTypeDecodable<ReturnType: PolymorphicOperationReturnType>: Decodable {
    public let decodedValue: ReturnType

    enum CodingKeys: String, CodingKey {
        case rowType = "RowType"
    }

    init(decodedValue: ReturnType) {
        self.decodedValue = decodedValue
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        let storedRowTypeName = try values.decode(String.self, forKey: .rowType)
        
        var queryableTypeProviders: [String: PolymorphicOperationReturnOption<ReturnType.AttributesType, ReturnType>] = [:]
        ReturnType.types.forEach { (type, provider) in
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
