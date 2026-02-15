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
//  DynamoDBClientProtocol.swift
//  DynamoDBTables
//

public protocol DynamoDBClientProtocol {
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
