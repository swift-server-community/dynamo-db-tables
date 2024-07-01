//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/aws-sdk-swift-main/Sources/SmokeDynamoDB/AWSDynamoDBCompositePrimaryKeysProjection.swift
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
//  AWSDynamoDBCompositePrimaryKeysProjection.swift
//  DynamoDBTables
//

import AwsCommonRuntimeKit
import AWSDynamoDB
import ClientRuntime
import Foundation
import Logging
import SmithyIdentity

public struct AWSDynamoDBCompositePrimaryKeysProjection: DynamoDBCompositePrimaryKeysProjection {
    let dynamodb: AWSDynamoDB.DynamoDBClient
    let targetTableName: String
    let logger: Logging.Logger

    class QueryPaginationResults<AttributesType: PrimaryKeyAttributes> {
        var items: [CompositePrimaryKey<AttributesType>] = []
        var exclusiveStartKey: String?
    }

    public init(tableName: String, region: Swift.String,
                awsCredentialIdentityResolver: (any SmithyIdentity.AWSCredentialIdentityResolver)? = nil,
                httpClientConfiguration: ClientRuntime.HttpClientConfiguration? = nil,
                logger: Logging.Logger? = nil) throws
    {
        self.logger = logger ?? Logging.Logger(label: "AWSDynamoDBCompositePrimaryKeysProjection")
        let config = try DynamoDBClient.DynamoDBClientConfiguration(
            awsCredentialIdentityResolver: awsCredentialIdentityResolver,
            region: region,
            httpClientConfiguration: httpClientConfiguration)
        self.dynamodb = AWSDynamoDB.DynamoDBClient(config: config)
        self.targetTableName = tableName

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(region)'")
    }

    public init(tableName: String,
                client: AWSDynamoDB.DynamoDBClient,
                logger: Logging.Logger? = nil)
    {
        self.logger = logger ?? Logging.Logger(label: "AWSDynamoDBCompositePrimaryKeysProjection")
        self.dynamodb = client
        self.targetTableName = tableName

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with existing client")
    }
}
