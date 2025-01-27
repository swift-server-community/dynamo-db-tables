//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/DynamoDBCompositePrimaryKeyTable+consistentReadQuery.swift
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
//  DynamoDBCompositePrimaryKeyTable+consistentReadQuery.swift
//  DynamoDBTables
//

import AWSDynamoDB
import Foundation

public extension DynamoDBCompositePrimaryKeyTable {
    func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                        sortKeyCondition: AttributeCondition?) async throws
        -> [ReturnedType]
    {
        try await self.polymorphicQuery(forPartitionKey: partitionKey,
                                        sortKeyCondition: sortKeyCondition,
                                        consistentRead: self.consistentRead)
    }

    func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                        sortKeyCondition: AttributeCondition?,
                                                                        limit: Int?,
                                                                        exclusiveStartKey: String?) async throws
        -> ([ReturnedType], String?)
    {
        try await self.polymorphicQuery(forPartitionKey: partitionKey,
                                        sortKeyCondition: sortKeyCondition,
                                        limit: limit,
                                        exclusiveStartKey: exclusiveStartKey,
                                        consistentRead: self.consistentRead)
    }

    func polymorphicQuery<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                        sortKeyCondition: AttributeCondition?,
                                                                        limit: Int?,
                                                                        scanIndexForward: Bool,
                                                                        exclusiveStartKey: String?) async throws
        -> ([ReturnedType], String?)
    {
        try await self.polymorphicQuery(forPartitionKey: partitionKey,
                                        sortKeyCondition: sortKeyCondition,
                                        limit: limit,
                                        scanIndexForward: scanIndexForward,
                                        exclusiveStartKey: exclusiveStartKey,
                                        consistentRead: self.consistentRead)
    }

    func query<AttributesType, ItemType, TimeToLiveAttributesType>(forPartitionKey partitionKey: String,
                                                                   sortKeyCondition: AttributeCondition?) async throws
        -> [TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        try await self.query(forPartitionKey: partitionKey,
                             sortKeyCondition: sortKeyCondition,
                             consistentRead: self.consistentRead)
    }

    func query<AttributesType, ItemType, TimeToLiveAttributesType>(forPartitionKey partitionKey: String,
                                                                   sortKeyCondition: AttributeCondition?,
                                                                   limit: Int?,
                                                                   scanIndexForward: Bool,
                                                                   exclusiveStartKey: String?) async throws
        -> ([TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>], String?)
    {
        try await self.query(forPartitionKey: partitionKey,
                             sortKeyCondition: sortKeyCondition,
                             limit: limit,
                             scanIndexForward: scanIndexForward,
                             exclusiveStartKey: exclusiveStartKey,
                             consistentRead: self.consistentRead)
    }
}
