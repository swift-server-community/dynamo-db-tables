//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/DynamoDBCompositePrimaryKeyTableHistoricalItemExtensions
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
//  DynamoDBCompositePrimaryKeyTableHistoricalItemExtensions.swift
//      Extensions which enable historical item multi-row update usecases.
//  DynamoDBTables
//

import AWSDynamoDB
import Foundation
import Logging

extension DynamoDBCompositePrimaryKeyTable {
    /**
     * Historical items exist across multiple rows. This method provides an interface to record all
     * rows in a single call.
     */
    public func insertItemWithHistoricalRow<AttributesType, ItemType, TimeToLiveAttributesType>(
        primaryItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        historicalItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>
    ) async throws {
        try await transactWrite([
            .insert(new: primaryItem),
            .insert(new: historicalItem),
        ])
    }

    public func updateItemWithHistoricalRow<AttributesType, ItemType, TimeToLiveAttributesType>(
        primaryItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        existingItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>,
        historicalItem: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>
    ) async throws {
        try await transactWrite([
            .update(new: primaryItem, existing: existingItem),
            .insert(new: historicalItem),
        ])
    }
}
