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
    func putItem(input: DynamoDBModel.PutItemInput) async throws
    func getItem(input: DynamoDBModel.GetItemInput) async throws -> DynamoDBModel.GetItemOutput
    func deleteItem(input: DynamoDBModel.DeleteItemInput) async throws
    func query(input: DynamoDBModel.QueryInput) async throws -> DynamoDBModel.QueryOutput
    func batchGetItem(input: DynamoDBModel.BatchGetItemInput) async throws -> DynamoDBModel.BatchGetItemOutput
    func batchExecuteStatement(
        input: DynamoDBModel.BatchExecuteStatementInput
    ) async throws -> DynamoDBModel.BatchExecuteStatementOutput
    func executeStatement(
        input: DynamoDBModel.ExecuteStatementInput
    ) async throws -> DynamoDBModel.ExecuteStatementOutput
    func executeTransaction(input: DynamoDBModel.ExecuteTransactionInput) async throws
}
