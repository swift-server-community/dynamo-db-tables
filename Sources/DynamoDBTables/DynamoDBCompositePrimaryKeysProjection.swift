//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from https://github.com/amzn/smoke-dynamodb. Any commits
// prior to February 2024
// Copyright (c) 2021-2021 the DynamoDBTables authors
// Licensed under Apache License v2.0
//
// Subsequent commits
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
//  DynamoDBCompositePrimaryKeysProjection.swift
//  DynamoDBTables
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel
import NIO

/**
 Protocol presenting a Keys Only projection of a DynamoDB table such as a Keys Only GSI projection.
 Provides the ability to query the projection to get the list of keys without attempting to decode the row into a particular data type.
 */
public protocol DynamoDBCompositePrimaryKeysProjection {
    var eventLoop: EventLoop { get }

    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will potentially make multiple calls to DynamoDB to retrieve all results for
       the query.
     */
    func query<AttributesType>(forPartitionKey partitionKey: String,
                               sortKeyCondition: AttributeCondition?)
        -> EventLoopFuture<[CompositePrimaryKey<AttributesType>]>

    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will return paginated results based on the limit and exclusiveStartKey provided.
     */
    func query<AttributesType>(forPartitionKey partitionKey: String,
                               sortKeyCondition: AttributeCondition?,
                               limit: Int?,
                               exclusiveStartKey: String?)
        -> EventLoopFuture<([CompositePrimaryKey<AttributesType>], String?)>
    
    func query<AttributesType>(forPartitionKey partitionKey: String,
                               sortKeyCondition: AttributeCondition?,
                               limit: Int?,
                               scanIndexForward: Bool,
                               exclusiveStartKey: String?)
        -> EventLoopFuture<([CompositePrimaryKey<AttributesType>], String?)>
    
#if (os(Linux) && compiler(>=5.5)) || (!os(Linux) && compiler(>=5.5.2)) && canImport(_Concurrency)
    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will potentially make multiple calls to DynamoDB to retrieve all results for
       the query.
     */
    func query<AttributesType>(forPartitionKey partitionKey: String,
                               sortKeyCondition: AttributeCondition?) async throws
        -> [CompositePrimaryKey<AttributesType>]

    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will return paginated results based on the limit and exclusiveStartKey provided.
     */
    func query<AttributesType>(forPartitionKey partitionKey: String,
                               sortKeyCondition: AttributeCondition?,
                               limit: Int?,
                               exclusiveStartKey: String?) async throws
        -> ([CompositePrimaryKey<AttributesType>], String?)
    
    func query<AttributesType>(forPartitionKey partitionKey: String,
                               sortKeyCondition: AttributeCondition?,
                               limit: Int?,
                               scanIndexForward: Bool,
                               exclusiveStartKey: String?) async throws
        -> ([CompositePrimaryKey<AttributesType>], String?)
#endif
}

// For async/await APIs, simply delegate to the EventLoopFuture implementation until support is dropped for Swift <5.5
public extension DynamoDBCompositePrimaryKeysProjection {
#if (os(Linux) && compiler(>=5.5)) || (!os(Linux) && compiler(>=5.5.2)) && canImport(_Concurrency)
    func query<AttributesType>(forPartitionKey partitionKey: String,
                               sortKeyCondition: AttributeCondition?) async throws
    -> [CompositePrimaryKey<AttributesType>] {
        return try await query(forPartitionKey: partitionKey,
                               sortKeyCondition: sortKeyCondition).get()
    }

    func query<AttributesType>(forPartitionKey partitionKey: String,
                               sortKeyCondition: AttributeCondition?,
                               limit: Int?,
                               exclusiveStartKey: String?) async throws
    -> ([CompositePrimaryKey<AttributesType>], String?) {
        return try await query(forPartitionKey: partitionKey,
                               sortKeyCondition: sortKeyCondition,
                               limit: limit,
                               exclusiveStartKey: exclusiveStartKey).get()
    }
    
    func query<AttributesType>(forPartitionKey partitionKey: String,
                               sortKeyCondition: AttributeCondition?,
                               limit: Int?,
                               scanIndexForward: Bool,
                               exclusiveStartKey: String?) async throws
    -> ([CompositePrimaryKey<AttributesType>], String?) {
        return try await query(forPartitionKey: partitionKey,
                               sortKeyCondition: sortKeyCondition,
                               limit: limit,
                               scanIndexForward: scanIndexForward,
                               exclusiveStartKey: exclusiveStartKey).get()
    }
#endif
}
