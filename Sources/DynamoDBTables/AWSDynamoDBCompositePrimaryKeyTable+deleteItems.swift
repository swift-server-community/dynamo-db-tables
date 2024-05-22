//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/AWSDynamoDBCompositePrimaryKeyTable+deleteItems.swift
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
//  AWSDynamoDBCompositePrimaryKeyTable+deleteItems.swift
//  DynamoDBTables
//

import AWSDynamoDB
import Foundation
import Logging

// BatchExecuteStatement has a maximum of 25 statements
private let maximumUpdatesPerExecuteStatement = 25

/// DynamoDBTable conformance updateItems function
public extension AWSDynamoDBCompositePrimaryKeyTable {
    private func deleteChunkedItems(_ keys: [CompositePrimaryKey<some Any>]) async throws {
        // if there are no keys, there is nothing to update
        guard keys.count > 0 else {
            return
        }

        let statements = try keys.map { existingKey -> DynamoDBClientTypes.BatchStatementRequest in
            let statement = try getDeleteExpression(tableName: self.targetTableName,
                                                    existingKey: existingKey)

            return DynamoDBClientTypes.BatchStatementRequest(consistentRead: true, statement: statement)
        }

        let executeInput = BatchExecuteStatementInput(statements: statements)

        let response = try await self.dynamodb.batchExecuteStatement(input: executeInput)
        try throwOnBatchExecuteStatementErrors(response: response)
    }

    private func deleteChunkedItems(_ existingItems: [some DatabaseItem]) async throws {
        // if there are no items, there is nothing to update
        guard existingItems.count > 0 else {
            return
        }

        let statements = try existingItems.map { existingItem -> DynamoDBClientTypes.BatchStatementRequest in
            let statement = try getDeleteExpression(tableName: self.targetTableName,
                                                    existingItem: existingItem)

            return DynamoDBClientTypes.BatchStatementRequest(consistentRead: true, statement: statement)
        }

        let executeInput = BatchExecuteStatementInput(statements: statements)

        let response = try await self.dynamodb.batchExecuteStatement(input: executeInput)
        try throwOnBatchExecuteStatementErrors(response: response)
    }

    func deleteItems(forKeys keys: [CompositePrimaryKey<some Any>]) async throws {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedKeys = keys.chunked(by: maximumUpdatesPerExecuteStatement)
        try await chunkedKeys.concurrentForEach { chunk in
            try await self.deleteChunkedItems(chunk)
        }
    }

    func deleteItems(existingItems: [some DatabaseItem]) async throws {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedItems = existingItems.chunked(by: maximumUpdatesPerExecuteStatement)
        try await chunkedItems.concurrentForEach { chunk in
            try await self.deleteChunkedItems(chunk)
        }
    }
}
