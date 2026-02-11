//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// Copyright (c) 2025 the DynamoDBTables authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of DynamoDBTables authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

//
//  TestDynamoDBClientProtocol.swift
//  DynamoDBTablesTests
//

import AWSDynamoDB
import Smockable

@testable import DynamoDBTables

/// A shadow protocol for testing that re-declares the function requirements of
/// ``DynamoDBClientProtocol`` so the `@Smock` macro can generate a mock.
@Smock(
    additionalEquatableTypes: [
        PutItemInput.self,
        GetItemInput.self,
        DeleteItemInput.self,
        QueryInput.self,
        BatchGetItemInput.self,
        BatchExecuteStatementInput.self,
        ExecuteStatementInput.self,
        ExecuteTransactionInput.self,
    ]
)
protocol TestDynamoDBClientProtocol: DynamoDBClientProtocol {
    func putItem(input: PutItemInput) async throws -> PutItemOutput
    func getItem(input: GetItemInput) async throws -> GetItemOutput
    func deleteItem(input: DeleteItemInput) async throws -> DeleteItemOutput
    func query(input: QueryInput) async throws -> QueryOutput
    func batchGetItem(input: BatchGetItemInput) async throws -> BatchGetItemOutput
    func batchExecuteStatement(
        input: BatchExecuteStatementInput
    ) async throws -> BatchExecuteStatementOutput
    func executeStatement(input: ExecuteStatementInput) async throws -> ExecuteStatementOutput
    func executeTransaction(
        input: ExecuteTransactionInput
    ) async throws -> ExecuteTransactionOutput
}
