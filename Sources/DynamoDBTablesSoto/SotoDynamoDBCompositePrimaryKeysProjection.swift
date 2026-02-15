//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// Copyright (c) 2026 the DynamoDBTables authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of DynamoDBTables authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

#if SOTOSDK
import DynamoDBTables
import Logging
import SotoDynamoDB

/// A `DynamoDBCompositePrimaryKeysProjection` implementation backed by the DynamoDB
/// service provided by Soto.
///
/// ```swift
/// let client = AWSClient()
/// let projection = SotoDynamoDBCompositePrimaryKeysProjection(
///     tableName: "MyTable",
///     client: client,
///     region: .useast1
/// )
///
/// let projection = SotoDynamoDBCompositePrimaryKeysProjection(
///     tableName: "MyTable",
///     client: existingDynamoDB
/// )
/// ```
public struct SotoDynamoDBCompositePrimaryKeysProjection: DynamoDBCompositePrimaryKeysProjection, Sendable {
    // Wrapper struct rather than typealias so that GenericDynamoDBCompositePrimaryKeysProjection
    // can use package access level while this type remains public.
    private let wrapped: GenericDynamoDBCompositePrimaryKeysProjection<SotoDynamoDB.DynamoDB>

    public var tableConfiguration: DynamoDBTableConfiguration { self.wrapped.tableConfiguration }

    public init(
        tableName: String,
        client: AWSClient,
        region: Region? = nil,
        tableConfiguration: DynamoDBTableConfiguration = .init(),
        logger: Logging.Logger? = nil
    ) {
        self.init(
            tableName: tableName,
            client: DynamoDB(client: client, region: region),
            tableConfiguration: tableConfiguration,
            logger: logger
        )
    }

    public init(
        tableName: String,
        client: SotoDynamoDB.DynamoDB,
        tableConfiguration: DynamoDBTableConfiguration = .init(),
        logger: Logging.Logger? = nil
    ) {
        self.wrapped = GenericDynamoDBCompositePrimaryKeysProjection(
            tableName: tableName,
            client: client,
            tableConfiguration: tableConfiguration,
            logger: logger
        )
    }

    // MARK: - DynamoDBCompositePrimaryKeysProjection forwarding

    public func query<AttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?
    ) async throws
        -> [CompositePrimaryKey<AttributesType>]
    {
        try await self.wrapped.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition)
    }

    public func query<AttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        exclusiveStartKey: String?
    ) async throws
        -> (keys: [CompositePrimaryKey<AttributesType>], lastEvaluatedKey: String?)
    {
        try await self.wrapped.query(
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
        try await self.wrapped.query(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition,
            limit: limit,
            scanIndexForward: scanIndexForward,
            exclusiveStartKey: exclusiveStartKey
        )
    }
}
#endif
