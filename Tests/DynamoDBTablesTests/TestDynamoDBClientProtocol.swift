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

import Smockable

@testable import DynamoDBTables

/// A shadow protocol for testing that re-declares the function requirements of
/// ``DynamoDBClientProtocol`` so the `@Smock` macro can generate a mock.
@Smock(
    additionalEquatableTypes: [
        DynamoDBModel.PutItemInput.self,
        DynamoDBModel.GetItemInput.self,
        DynamoDBModel.DeleteItemInput.self,
        DynamoDBModel.QueryInput.self,
        DynamoDBModel.BatchGetItemInput.self,
        DynamoDBModel.BatchExecuteStatementInput.self,
        DynamoDBModel.ExecuteStatementInput.self,
        DynamoDBModel.ExecuteTransactionInput.self,
    ]
)
protocol TestDynamoDBClientProtocol: DynamoDBClientProtocol {
    func putItem(input: DynamoDBModel.PutItemInput) async throws(DynamoDBClientError)
    func getItem(input: DynamoDBModel.GetItemInput) async throws(DynamoDBClientError) -> DynamoDBModel.GetItemOutput
    func deleteItem(input: DynamoDBModel.DeleteItemInput) async throws(DynamoDBClientError)
    func query(input: DynamoDBModel.QueryInput) async throws(DynamoDBClientError) -> DynamoDBModel.QueryOutput
    func batchGetItem(
        input: DynamoDBModel.BatchGetItemInput
    ) async throws(DynamoDBClientError) -> DynamoDBModel.BatchGetItemOutput
    func batchExecuteStatement(
        input: DynamoDBModel.BatchExecuteStatementInput
    ) async throws(DynamoDBClientError) -> DynamoDBModel.BatchExecuteStatementOutput
    func executeStatement(
        input: DynamoDBModel.ExecuteStatementInput
    ) async throws(DynamoDBClientError) -> DynamoDBModel.ExecuteStatementOutput
    func executeTransaction(input: DynamoDBModel.ExecuteTransactionInput) async throws(DynamoDBClientError)
}
