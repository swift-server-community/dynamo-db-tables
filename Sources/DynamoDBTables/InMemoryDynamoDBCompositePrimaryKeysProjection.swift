// swiftlint:disable cyclomatic_complexity
//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/InMemoryDynamoDBCompositePrimaryKeysProjection.swift
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
//  InMemoryDynamoDBCompositePrimaryKeysProjection.swift
//  DynamoDBTables
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel
import NIO

public class InMemoryDynamoDBCompositePrimaryKeysProjection: DynamoDBCompositePrimaryKeysProjection {
    public var eventLoop: EventLoop

    internal let keysWrapper: InMemoryDynamoDBCompositePrimaryKeysProjectionStore
    
    public var keys: [Any] {
        do {
            return try keysWrapper.getKeys(eventLoop: self.eventLoop).wait()
        } catch {
            fatalError("Unable to retrieve InMemoryDynamoDBCompositePrimaryKeysProjection keys.")
        }
    }

    public init(keys: [Any] = [], eventLoop: EventLoop) {
        self.keysWrapper = InMemoryDynamoDBCompositePrimaryKeysProjectionStore(keys: keys)
        self.eventLoop = eventLoop
    }
    
    internal init(eventLoop: EventLoop,
                  keysWrapper: InMemoryDynamoDBCompositePrimaryKeysProjectionStore) {
        self.eventLoop = eventLoop
        self.keysWrapper = keysWrapper
    }
    
    public func on(eventLoop: EventLoop) -> InMemoryDynamoDBCompositePrimaryKeysProjection {
        return InMemoryDynamoDBCompositePrimaryKeysProjection(eventLoop: eventLoop,
                                                              keysWrapper: self.keysWrapper)
    }

    public func query<AttributesType>(forPartitionKey partitionKey: String,
                                      sortKeyCondition: AttributeCondition?)
    -> EventLoopFuture<[CompositePrimaryKey<AttributesType>]> {
        return keysWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition, eventLoop: self.eventLoop)
    }
    
    public func query<AttributesType>(forPartitionKey partitionKey: String,
                                      sortKeyCondition: AttributeCondition?,
                                      limit: Int?,
                                      exclusiveStartKey: String?)
    -> EventLoopFuture<([CompositePrimaryKey<AttributesType>], String?)>
            where AttributesType: PrimaryKeyAttributes {
        return keysWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                 limit: limit, exclusiveStartKey: exclusiveStartKey, eventLoop: self.eventLoop)
    }

    public func query<AttributesType>(forPartitionKey partitionKey: String,
                                      sortKeyCondition: AttributeCondition?,
                                      limit: Int?,
                                      scanIndexForward: Bool,
                                      exclusiveStartKey: String?)
    -> EventLoopFuture<([CompositePrimaryKey<AttributesType>], String?)>
    where AttributesType: PrimaryKeyAttributes {
        return keysWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                 limit: limit, scanIndexForward: scanIndexForward,
                                 exclusiveStartKey: exclusiveStartKey, eventLoop: self.eventLoop)
    }
}
