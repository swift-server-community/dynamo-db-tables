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
//  AWSDynamoDBCompositePrimaryKeyTable+deleteItems.swift
//  DynamoDBTables
//

import Foundation
import SmokeAWSCore
import DynamoDBModel
import SmokeHTTPClient
import Logging
import NIO

// BatchExecuteStatement has a maximum of 25 statements
private let maximumUpdatesPerExecuteStatement = 25

/// DynamoDBTable conformance updateItems function
public extension AWSDynamoDBCompositePrimaryKeyTable {
    private func deleteChunkedItems<AttributesType>(_ keys: [CompositePrimaryKey<AttributesType>])
    -> EventLoopFuture<Void> {
        // if there are no keys, there is nothing to update
        guard keys.count > 0 else {
            let promise = self.eventLoop.makePromise(of: Void.self)
            promise.succeed(())
            return promise.futureResult
        }
        
        let statements: [BatchStatementRequest]
        do {
            statements = try keys.map { existingKey -> BatchStatementRequest in
                let statement = try getDeleteExpression(tableName: self.targetTableName,
                                                        existingKey: existingKey)
                
                // doesn't require read consistency as no items are being read
                return BatchStatementRequest(consistentRead: false, statement: statement)
            }
        } catch {
            let promise = self.eventLoop.makePromise(of: Void.self)
            promise.fail(error)
            return promise.futureResult
        }
        
        let executeInput = BatchExecuteStatementInput(statements: statements)
        
        return dynamodb.batchExecuteStatement(input: executeInput).flatMapThrowing { response in
            try self.throwOnBatchExecuteStatementErrors(response: response)
        }
    }
    
    private func deleteChunkedItems<ItemType: DatabaseItem>(_ existingItems: [ItemType])
    -> EventLoopFuture<Void> {
        // if there are no items, there is nothing to update
        guard existingItems.count > 0 else {
            let promise = self.eventLoop.makePromise(of: Void.self)
            promise.succeed(())
            return promise.futureResult
        }
        
        let statements: [BatchStatementRequest]
        do {
            statements = try existingItems.map { existingItem -> BatchStatementRequest in
                let statement = try getDeleteExpression(tableName: self.targetTableName,
                                                        existingItem: existingItem)
                
                // doesn't require read consistency as no items are being read
                return BatchStatementRequest(consistentRead: false, statement: statement)
            }
        } catch {
            let promise = self.eventLoop.makePromise(of: Void.self)
            promise.fail(error)
            return promise.futureResult
        }
        
        let executeInput = BatchExecuteStatementInput(statements: statements)
        
        return dynamodb.batchExecuteStatement(input: executeInput).flatMapThrowing { response in
            try self.throwOnBatchExecuteStatementErrors(response: response)
        }
    }
    
    func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) -> EventLoopFuture<Void> {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedKeys = keys.chunked(by: maximumUpdatesPerExecuteStatement)
        let futures = chunkedKeys.map { chunk -> EventLoopFuture<Void> in
            return deleteChunkedItems(chunk)
        }
        
        return EventLoopFuture.andAllSucceed(futures, on: self.eventLoop)
    }
    
    func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType]) -> EventLoopFuture<Void> {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedItems = existingItems.chunked(by: maximumUpdatesPerExecuteStatement)
        let futures = chunkedItems.map { chunk -> EventLoopFuture<Void> in
            return deleteChunkedItems(chunk)
        }
        
        return EventLoopFuture.andAllSucceed(futures, on: self.eventLoop)
    }
    
#if (os(Linux) && compiler(>=5.5)) || (!os(Linux) && compiler(>=5.5.2)) && canImport(_Concurrency)
    private func deleteChunkedItems<AttributesType>(_ keys: [CompositePrimaryKey<AttributesType>]) async throws {
        // if there are no keys, there is nothing to update
        guard keys.count > 0 else {
            return
        }
        
        let statements: [BatchStatementRequest] = try keys.map { existingKey -> BatchStatementRequest in
            let statement = try getDeleteExpression(tableName: self.targetTableName,
                                                    existingKey: existingKey)
                
            return BatchStatementRequest(consistentRead: true, statement: statement)
        }
        
        let executeInput = BatchExecuteStatementInput(statements: statements)
        
        let response = try await self.dynamodb.batchExecuteStatement(input: executeInput)
        try throwOnBatchExecuteStatementErrors(response: response)
    }
    
    private func deleteChunkedItems<ItemType: DatabaseItem>(_ existingItems: [ItemType]) async throws {
        // if there are no items, there is nothing to update
        guard existingItems.count > 0 else {
            return
        }
        
        let statements: [BatchStatementRequest] = try existingItems.map { existingItem -> BatchStatementRequest in
            let statement = try getDeleteExpression(tableName: self.targetTableName,
                                                    existingItem: existingItem)
                
            return BatchStatementRequest(consistentRead: true, statement: statement)
        }
        
        let executeInput = BatchExecuteStatementInput(statements: statements)
        
        let response = try await self.dynamodb.batchExecuteStatement(input: executeInput)
        try throwOnBatchExecuteStatementErrors(response: response)
    }
    
    func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedKeys = keys.chunked(by: maximumUpdatesPerExecuteStatement)
        try await chunkedKeys.concurrentForEach { chunk in
            try await self.deleteChunkedItems(chunk)
        }
    }
    
    func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType]) async throws {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedItems = existingItems.chunked(by: maximumUpdatesPerExecuteStatement)
        try await chunkedItems.concurrentForEach { chunk in
            try await self.deleteChunkedItems(chunk)
        }
    }
#endif
}
