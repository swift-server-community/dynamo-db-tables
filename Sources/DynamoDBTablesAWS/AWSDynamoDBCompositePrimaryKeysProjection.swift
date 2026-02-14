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
import DynamoDBTables

/// A type alias for `GenericDynamoDBCompositePrimaryKeysProjection` specialized with the AWS DynamoDB client.
///
/// This provides a convenient way to use the DynamoDB composite primary keys projection with the standard AWS DynamoDB client
/// without needing to specify the generic parameter explicitly.
///
/// ## Usage
///
/// Use this type alias when working with the real AWS DynamoDB service:
///
/// ```swift
/// // Create a projection using region-based initialization
/// let projection = try AWSDynamoDBCompositePrimaryKeysProjection(
///     tableName: "MyTable",
///     region: "us-east-1"
/// )
///
/// // Create a projection with an existing AWS client
/// let awsClient = AWSDynamoDB.DynamoDBClient(config: config)
/// let projection = AWSDynamoDBCompositePrimaryKeysProjection(
///     tableName: "MyTable",
///     client: awsClient
/// )
/// ```
public typealias AWSDynamoDBCompositePrimaryKeysProjection = GenericDynamoDBCompositePrimaryKeysProjection<
    AWSDynamoDB.DynamoDBClient
>
#endif
