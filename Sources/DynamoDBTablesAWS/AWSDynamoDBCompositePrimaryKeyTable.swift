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

/// A type alias for `GenericDynamoDBCompositePrimaryKeyTable` specialized with the AWS DynamoDB client.
///
/// This provides a convenient way to use the DynamoDB table implementation with the standard AWS DynamoDB client
/// without needing to specify the generic parameter explicitly.
///
/// ## Usage
///
/// Use this type alias when working with the real AWS DynamoDB service:
///
/// ```swift
/// // Create a table using region-based initialization
/// let table = try AWSDynamoDBCompositePrimaryKeyTable(
///     tableName: "MyTable",
///     region: "us-east-1"
/// )
///
/// // Create a table with an existing AWS client
/// let awsClient = AWSDynamoDB.DynamoDBClient(config: config)
/// let table = AWSDynamoDBCompositePrimaryKeyTable(
///     tableName: "MyTable",
///     client: awsClient
/// )
/// ```
public typealias AWSDynamoDBCompositePrimaryKeyTable = GenericDynamoDBCompositePrimaryKeyTable<
    AWSDynamoDB.DynamoDBClient
>

extension GenericDynamoDBCompositePrimaryKeyTable where Client == AWSDynamoDB.DynamoDBClient {
    public init(
        tableName: String,
        region: Swift.String,
        awsCredentialIdentityResolver: (any SmithyIdentity.AWSCredentialIdentityResolver)? = nil,
        httpClientConfiguration: ClientRuntime.HttpClientConfiguration? = nil,
        tableConfiguration: AWSDynamoDBTableConfiguration = .init(),
        tableMetrics: AWSDynamoDBTableMetrics = .init(),
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
            tableMetrics: tableMetrics,
            logger: logger
        )
    }
}
#endif
