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

import Logging

package struct GenericDynamoDBCompositePrimaryKeysProjection<Client: DynamoDBClientProtocol & Sendable>:
    DynamoDBCompositePrimaryKeysProjection, Sendable
{
    let dynamodb: Client
    let targetTableName: String
    package let tableConfiguration: DynamoDBTableConfiguration
    let logger: Logging.Logger

    package init(
        tableName: String,
        client: Client,
        tableConfiguration: DynamoDBTableConfiguration = .init(),
        logger: Logging.Logger? = nil
    ) {
        self.logger = logger ?? Logging.Logger(label: "GenericDynamoDBCompositePrimaryKeysProjection")
        self.dynamodb = client
        self.tableConfiguration = tableConfiguration
        self.targetTableName = tableName

        self.logger.trace("GenericDynamoDBCompositePrimaryKeysProjection created with existing client")
    }
}
