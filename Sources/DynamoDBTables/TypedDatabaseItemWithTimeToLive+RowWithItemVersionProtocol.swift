//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from https://github.com/amzn/smoke-dynamodb. Any commits
// prior to February 2024
// Copyright 2018-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
//  TypedDatabaseItemWithTimeToLive+RowWithItemVersionProtocol.swift
//  DynamoDBTables
//

import Foundation

/// An extension for TypedDatabaseItem that is constrained by the RowType conforming
/// to RowWithItemVersionProtocol
extension TypedDatabaseItemWithTimeToLive where RowType: RowWithItemVersionProtocol {
    /// Helper function wrapping createUpdatedItem that will verify if
    /// conditionalStatusVersion is provided that it matches the version
    /// of the current item
    public func createUpdatedRowWithItemVersion(withValue value: RowType.RowType,
                                                conditionalStatusVersion: Int?,
                                                andTimeToLive timeToLive: TimeToLive<TimeToLiveAttributesType>? = nil) throws
    -> TypedDatabaseItemWithTimeToLive<AttributesType, RowType, TimeToLiveAttributesType> {
        // if we can only update a particular version
        if let overwriteVersion = conditionalStatusVersion,
           rowValue.itemVersion != overwriteVersion {
            throw DynamoDBTableError.concurrencyError(partitionKey: compositePrimaryKey.partitionKey,
                                                      sortKey: compositePrimaryKey.sortKey,
                                                      message: "Current row did not have the required version '\(overwriteVersion)'")
        }
        
        let updatedPayloadWithVersion: RowType = rowValue.createUpdatedItem(withValue: value)
        return createUpdatedItem(withValue: updatedPayloadWithVersion, andTimeToLive: timeToLive)
    }
}
