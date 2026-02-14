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
//  GenericDynamoDBCompositePrimaryKeysProjection.swift
//  DynamoDBTables
//

import AWSDynamoDB
import Logging

public struct GenericDynamoDBCompositePrimaryKeysProjection<Client: DynamoDBClientProtocol & Sendable>:
    DynamoDBCompositePrimaryKeysProjection, Sendable
{
    let dynamodb: Client
    let targetTableName: String
    public let tableConfiguration: AWSDynamoDBTableConfiguration
    let logger: Logging.Logger

    public init(
        tableName: String,
        client: Client,
        tableConfiguration: AWSDynamoDBTableConfiguration = .init(),
        logger: Logging.Logger? = nil
    ) {
        self.logger = logger ?? Logging.Logger(label: "AWSDynamoDBCompositePrimaryKeysProjection")
        self.dynamodb = client
        self.tableConfiguration = tableConfiguration
        self.targetTableName = tableName

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with existing client")
    }
}
