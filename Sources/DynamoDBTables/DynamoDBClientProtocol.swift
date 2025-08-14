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

import AWSDynamoDB
import Foundation
import Smockable

/// A protocol that abstracts DynamoDB client operations to enable testability and flexibility.
///
/// ## Usage
///
/// The protocol is designed to be implemented by DynamoDB clients that can perform the core operations
/// required for table management:
///
/// ```swift
/// // Production usage with AWS DynamoDB
/// let awsClient = AWSDynamoDB.DynamoDBClient(config: config)
/// let table = AWSDynamoDBCompositePrimaryKeyTable(tableName: "MyTable", client: awsClient)
///
/// // Testing usage with a mock implementation
/// let mockClient = MockDynamoDBClient()
/// let table = AWSDynamoDBCompositePrimaryKeyTable(tableName: "TestTable", client: mockClient)
/// ```
///
/// ## Error Handling
///
/// All methods can throw errors that conform to Swift's `Error` protocol. Implementations should
/// throw appropriate DynamoDB-specific errors that can be handled by the table layer.
///
/// ## Thread Safety
///
/// Implementations of this protocol should be thread-safe and support concurrent access,
/// as the table implementation may call these methods from multiple concurrent contexts.
@Smock
public protocol DynamoDBClientProtocol {
    
    // MARK: - Single Item Operations
    
    /// Puts an item into a DynamoDB table.
    ///
    /// This operation either creates a new item or replaces an existing item with the same key.
    /// Conditional expressions can be used to control when the operation succeeds.
    ///
    /// - Parameter input: The put item request containing the item data and optional conditions
    /// - Returns: The response from the put operation
    /// - Throws: DynamoDB errors such as conditional check failures, validation errors, or service errors
    ///
    /// ## Usage
    /// ```swift
    /// let putInput = PutItemInput(
    ///     item: itemAttributes,
    ///     tableName: "MyTable",
    ///     conditionExpression: "attribute_not_exists(pk)"
    /// )
    /// let response = try await client.putItem(input: putInput)
    /// ```
    func putItem(input: AWSDynamoDB.PutItemInput) async throws -> AWSDynamoDB.PutItemOutput
    
    /// Retrieves an item from a DynamoDB table by its primary key.
    ///
    /// This operation returns the item attributes for the specified key. If the item doesn't exist,
    /// the response will indicate no item was found.
    ///
    /// - Parameter input: The get item request containing the key and optional projection
    /// - Returns: The response containing the item data if found
    /// - Throws: DynamoDB errors such as validation errors or service errors
    ///
    /// ## Usage
    /// ```swift
    /// let getInput = GetItemInput(
    ///     key: keyAttributes,
    ///     tableName: "MyTable",
    ///     consistentRead: true
    /// )
    /// let response = try await client.getItem(input: getInput)
    /// ```
    func getItem(input: AWSDynamoDB.GetItemInput) async throws -> AWSDynamoDB.GetItemOutput
    
    /// Deletes an item from a DynamoDB table.
    ///
    /// This operation removes an item with the specified key. Conditional expressions can be used
    /// to control when the deletion occurs.
    ///
    /// - Parameter input: The delete item request containing the key and optional conditions
    /// - Returns: The response from the delete operation
    /// - Throws: DynamoDB errors such as conditional check failures, validation errors, or service errors
    ///
    /// ## Usage
    /// ```swift
    /// let deleteInput = DeleteItemInput(
    ///     key: keyAttributes,
    ///     tableName: "MyTable",
    ///     conditionExpression: "attribute_exists(pk)"
    /// )
    /// let response = try await client.deleteItem(input: deleteInput)
    /// ```
    func deleteItem(input: AWSDynamoDB.DeleteItemInput) async throws -> AWSDynamoDB.DeleteItemOutput
    
    // MARK: - Query Operations
    
    /// Queries items from a DynamoDB table using a partition key and optional sort key conditions.
    ///
    /// This operation efficiently retrieves items that share the same partition key value.
    /// Additional filtering can be applied using sort key conditions and filter expressions.
    ///
    /// - Parameter input: The query request containing partition key, conditions, and options
    /// - Returns: The response containing matching items and pagination information
    /// - Throws: DynamoDB errors such as validation errors or service errors
    ///
    /// ## Usage
    /// ```swift
    /// let queryInput = QueryInput(
    ///     tableName: "MyTable",
    ///     keyConditionExpression: "pk = :pk AND sk BEGINS_WITH :sk_prefix",
    ///     expressionAttributeValues: [":pk": .s("USER#123"), ":sk_prefix": .s("ORDER#")]
    /// )
    /// let response = try await client.query(input: queryInput)
    /// ```
    func query(input: AWSDynamoDB.QueryInput) async throws -> AWSDynamoDB.QueryOutput
    
    // MARK: - Batch Operations
    
    /// Retrieves multiple items from one or more DynamoDB tables in a single request.
    ///
    /// This operation allows efficient retrieval of up to 100 items across multiple tables.
    /// Unprocessed items are returned in the response and should be retried.
    ///
    /// - Parameter input: The batch get request containing keys for items to retrieve
    /// - Returns: The response containing retrieved items and any unprocessed keys
    /// - Throws: DynamoDB errors such as validation errors or service errors
    ///
    /// ## Usage
    /// ```swift
    /// let batchInput = BatchGetItemInput(
    ///     requestItems: [
    ///         "MyTable": KeysAndAttributes(keys: [keyAttributes1, keyAttributes2])
    ///     ]
    /// )
    /// let response = try await client.batchGetItem(input: batchInput)
    /// ```
    func batchGetItem(input: AWSDynamoDB.BatchGetItemInput) async throws -> AWSDynamoDB.BatchGetItemOutput
    
    /// Executes multiple PartiQL statements in a single batch request.
    ///
    /// This operation allows executing up to 25 PartiQL statements (SELECT, INSERT, UPDATE, DELETE)
    /// in a single request for improved performance.
    ///
    /// - Parameter input: The batch execute request containing multiple PartiQL statements
    /// - Returns: The response containing results for each statement
    /// - Throws: DynamoDB errors such as validation errors, statement errors, or service errors
    ///
    /// ## Usage
    /// ```swift
    /// let statements = [
    ///     BatchStatementRequest(statement: "DELETE FROM MyTable WHERE pk = ?", parameters: [.s("key1")]),
    ///     BatchStatementRequest(statement: "DELETE FROM MyTable WHERE pk = ?", parameters: [.s("key2")])
    /// ]
    /// let batchInput = BatchExecuteStatementInput(statements: statements)
    /// let response = try await client.batchExecuteStatement(input: batchInput)
    /// ```
    func batchExecuteStatement(input: AWSDynamoDB.BatchExecuteStatementInput) async throws -> AWSDynamoDB.BatchExecuteStatementOutput
    
    // MARK: - Advanced Operations
    
    /// Executes a single PartiQL statement against a DynamoDB table.
    ///
    /// PartiQL is a SQL-compatible query language for DynamoDB that supports SELECT, INSERT,
    /// UPDATE, and DELETE operations with familiar SQL syntax.
    ///
    /// - Parameter input: The execute statement request containing the PartiQL statement
    /// - Returns: The response containing query results or operation status
    /// - Throws: DynamoDB errors such as validation errors, statement errors, or service errors
    ///
    /// ## Usage
    /// ```swift
    /// let executeInput = ExecuteStatementInput(
    ///     statement: "SELECT * FROM MyTable WHERE pk = ? AND sk BEGINS_WITH ?",
    ///     parameters: [.s("USER#123"), .s("ORDER#")]
    /// )
    /// let response = try await client.executeStatement(input: executeInput)
    /// ```
    func executeStatement(input: AWSDynamoDB.ExecuteStatementInput) async throws -> AWSDynamoDB.ExecuteStatementOutput
    
    /// Executes multiple PartiQL statements as a single transaction.
    ///
    /// This operation ensures that all statements either succeed together or fail together,
    /// providing ACID transaction guarantees across multiple items and tables.
    ///
    /// - Parameter input: The transaction request containing multiple PartiQL statements
    /// - Returns: The response indicating transaction success or failure details
    /// - Throws: DynamoDB errors such as transaction conflicts, validation errors, or service errors
    ///
    /// ## Usage
    /// ```swift
    /// let statements = [
    ///     ParameterizedStatement(statement: "UPDATE MyTable SET balance = balance - ? WHERE pk = ?", parameters: [.n("100"), .s("ACCOUNT#1")]),
    ///     ParameterizedStatement(statement: "UPDATE MyTable SET balance = balance + ? WHERE pk = ?", parameters: [.n("100"), .s("ACCOUNT#2")])
    /// ]
    /// let transactionInput = ExecuteTransactionInput(transactStatements: statements)
    /// let response = try await client.executeTransaction(input: transactionInput)
    /// ```
    func executeTransaction(input: AWSDynamoDB.ExecuteTransactionInput) async throws -> AWSDynamoDB.ExecuteTransactionOutput
}

// MARK: - AWS DynamoDB Client Conformance

/// Retroactive conformance of AWS DynamoDB Client to the protocol.
extension AWSDynamoDB.DynamoDBClient: DynamoDBClientProtocol {
    // No implementation needed - the client already has all required methods
}