// swiftlint:disable cyclomatic_complexity
//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/main/Sources/SmokeDynamoDB/InMemoryDynamoDBCompositePrimaryKeysProjection.swift
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
//  InMemoryDynamoDBCompositePrimaryKeysProjection.swift
//  DynamoDBTables
//


public struct InMemoryDynamoDBCompositePrimaryKeysProjection: DynamoDBCompositePrimaryKeysProjection {
    let keysWrapper: InMemoryDynamoDBCompositePrimaryKeysProjectionStore

    public init(keys: [CompositePrimaryKey<some Any>] = []) {
        self.keysWrapper = InMemoryDynamoDBCompositePrimaryKeysProjectionStore(keys: keys)
    }

    public var keys: [TypeErasedCompositePrimaryKey] {
        get async {
            await self.keysWrapper.keys
        }
    }

    public func query<AttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?
    ) async throws
        -> [CompositePrimaryKey<AttributesType>]
    {
        try await self.keysWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition)
    }

    public func query<AttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        exclusiveStartKey: String?
    ) async throws
        -> (keys: [CompositePrimaryKey<AttributesType>], lastEvaluatedKey: String?)
    {
        try await self.keysWrapper.query(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition,
            limit: limit,
            exclusiveStartKey: exclusiveStartKey
        )
    }

    public func query<AttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        scanIndexForward: Bool,
        exclusiveStartKey: String?
    ) async throws
        -> (keys: [CompositePrimaryKey<AttributesType>], lastEvaluatedKey: String?)
    {
        try await self.keysWrapper.query(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition,
            limit: limit,
            scanIndexForward: scanIndexForward,
            exclusiveStartKey: exclusiveStartKey
        )
    }
}
