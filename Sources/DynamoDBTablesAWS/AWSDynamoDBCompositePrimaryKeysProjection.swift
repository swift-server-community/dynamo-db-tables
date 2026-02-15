//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// Copyright (c) 2024 the DynamoDBTables authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of DynamoDBTables authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

#if AWSSDK
import AWSDynamoDB
import ClientRuntime
import DynamoDBTables
import Logging
import SmithyIdentity

/// A `DynamoDBCompositePrimaryKeysProjection` implementation backed by the AWS DynamoDB 
/// service provided by aws-sdk-swift.
///
/// ```swift
/// let projection = try AWSDynamoDBCompositePrimaryKeysProjection(
///     tableName: "MyTable",
///     region: "us-east-1"
/// )
///
/// let projection = AWSDynamoDBCompositePrimaryKeysProjection(
///     tableName: "MyTable",
///     client: existingDynamoDBClient
/// )
/// ```
public struct AWSDynamoDBCompositePrimaryKeysProjection: DynamoDBCompositePrimaryKeysProjection, Sendable {
    // Wrapper struct rather than typealias so that GenericDynamoDBCompositePrimaryKeysProjection
    // can use package access level while this type remains public.
    private let wrapped: GenericDynamoDBCompositePrimaryKeysProjection<AWSDynamoDB.DynamoDBClient>

    public var tableConfiguration: AWSDynamoDBTableConfiguration { self.wrapped.tableConfiguration }

    public init(
        tableName: String,
        region: Swift.String,
        awsCredentialIdentityResolver: (any SmithyIdentity.AWSCredentialIdentityResolver)? = nil,
        httpClientConfiguration: ClientRuntime.HttpClientConfiguration? = nil,
        tableConfiguration: AWSDynamoDBTableConfiguration = .init(),
        logger: Logging.Logger? = nil
    ) throws {
        let config = try DynamoDBClient.DynamoDBClientConfig(
            awsCredentialIdentityResolver: awsCredentialIdentityResolver,
            region: region,
            httpClientConfiguration: httpClientConfiguration
        )
        self.init(
            tableName: tableName,
            client: AWSDynamoDB.DynamoDBClient(config: config),
            tableConfiguration: tableConfiguration,
            logger: logger
        )
    }

    public init(
        tableName: String,
        client: AWSDynamoDB.DynamoDBClient,
        tableConfiguration: AWSDynamoDBTableConfiguration = .init(),
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
