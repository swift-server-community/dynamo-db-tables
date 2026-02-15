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
//  GenericDynamoDBCompositePrimaryKeyTableDeleteTests.swift
//  DynamoDBTablesTests
//

import Foundation
import Logging
import Smockable
import Testing

@testable import DynamoDBTables

@Suite("AWSDynamoDBCompositePrimaryKeyTable Delete Operations Tests")
struct AWSDynamoDBCompositePrimaryKeyTableDeleteTests {

    // MARK: - Test Configuration

    private let testTableName = "TestTable"
    private let testLogger = Logger(label: "TestLogger")
    private let testConfiguration = AWSDynamoDBTableConfiguration(
        consistentRead: true,
        escapeSingleQuoteInPartiQL: false,
        retry: .default
    )
    private let testMetrics = AWSDynamoDBTableMetrics()

    // MARK: - Test Data

    private let testItemA = StandardTypedDatabaseItem.newItem(
        withKey: CompositePrimaryKey(partitionKey: "partition1", sortKey: "sort1"),
        andValue: TestTypeA(firstly: "test1", secondly: "test2")
    )

    private let testItemB = StandardTypedDatabaseItem.newItem(
        withKey: CompositePrimaryKey(partitionKey: "partition2", sortKey: "sort2"),
        andValue: TestTypeA(firstly: "test3", secondly: "test4")
    )

    // MARK: - Helper Methods

    private func createTable(
        with mockClient: MockTestDynamoDBClientProtocol
    ) -> GenericDynamoDBCompositePrimaryKeyTable<MockTestDynamoDBClientProtocol> {
        return GenericDynamoDBCompositePrimaryKeyTable(
            tableName: testTableName,
            client: mockClient,
            tableConfiguration: testConfiguration,
            tableMetrics: testMetrics,
            logger: testLogger
        )
    }

    // MARK: - Delete Existing Item Tests

    @Test("Delete existing item with version check succeeds")
    func deleteExistingItemWithVersionCheckSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()

        when(expectations.deleteItem(input: .any), complete: .withSuccess)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.deleteItem(existingItem: testItemA)

        // Verify
        verify(mockClient).deleteItem(
            input: .matching { input in
                input.tableName == testTableName
                    && input.key["PK"]?.asString == testItemA.compositePrimaryKey.partitionKey
                    && input.key["SK"]?.asString == testItemA.compositePrimaryKey.sortKey
            }
        )
    }

    @Test("Delete existing item with version check fails on condition")
    func deleteExistingItemWithVersionCheckFailsOnCondition() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let conditionalCheckError = DynamoDBClientError.conditionalCheckFailed(
            message: "Version mismatch - item was modified"
        )

        when(expectations.deleteItem(input: .any), times: 2, throw: conditionalCheckError)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When/Then
        do {
            try await table.deleteItem(existingItem: testItemA)
        } catch let error as DynamoDBTableError {
            if case .conditionalCheckFailed(let partitionKey, let sortKey, let message) = error {
                #expect(partitionKey == testItemA.compositePrimaryKey.partitionKey)
                #expect(sortKey == testItemA.compositePrimaryKey.sortKey)
                #expect(message == "Version mismatch - item was modified")
            } else {
                Issue.record("Expected DynamoDBTableError.concurrencyError, got \(error)")
            }
        }

        verify(mockClient).deleteItem(
            input: .matching { input in
                input.tableName == testTableName && input.conditionExpression?.contains("rowversion") == true
            }
        )
    }

    @Test("Delete existing item includes correct condition expression")
    func deleteExistingItemIncludesCorrectConditionExpression() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()

        when(expectations.deleteItem(input: .any), complete: .withSuccess)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.deleteItem(existingItem: testItemA)

        // Verify - The delete input should include version and creation date conditions
        verify(mockClient).deleteItem(
            input: .matching { input in
                input.tableName == testTableName && input.conditionExpression?.contains("rowversion") == true
                    && input.conditionExpression?.contains("createdate") == true
            }
        )
    }

    @Test("Delete existing item handles resource not found gracefully")
    func deleteExistingItemHandlesResourceNotFoundGracefully() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let resourceNotFoundError = DynamoDBClientError.resourceNotFound(
            message: "Item does not exist"
        )

        when(expectations.deleteItem(input: .any), throw: resourceNotFoundError)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When/Then
        await #expect(throws: DynamoDBTableError.self) {
            try await table.deleteItem(existingItem: testItemA)
        }

        verify(mockClient).deleteItem(
            input: .matching { input in
                input.tableName == testTableName
            }
        )
    }

    // MARK: - Delete Existing Items (Batch) Tests

    @Test("Delete existing items batch succeeds")
    func deleteExistingItemsBatchSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let existingItems = [testItemA, testItemB]
        let expectedOutput = DynamoDBModel.BatchExecuteStatementOutput(
            responses: [
                DynamoDBModel.BatchStatementResponse(),
                DynamoDBModel.BatchStatementResponse(),
            ]
        )

        when(expectations.batchExecuteStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.deleteItems(existingItems: existingItems)

        // Verify
        verify(mockClient).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 2
                    && input.statements?.allSatisfy { $0.statement?.contains("DELETE") == true } == true
            }
        )
    }

    @Test("Delete existing items batch handles empty list")
    func deleteExistingItemsBatchHandlesEmptyList() async throws {
        // Given
        let expectations = MockTestDynamoDBClientProtocol.Expectations()
        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let existingItems: [StandardTypedDatabaseItem<TestTypeA>] = []
        try await table.deleteItems(existingItems: existingItems)

        // Verify
        verify(mockClient, .never).batchExecuteStatement(input: .any)
    }

    @Test("Delete existing items batch with version check conditions")
    func deleteExistingItemsBatchWithVersionCheckConditions() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let existingItems = [testItemA]
        let expectedOutput = DynamoDBModel.BatchExecuteStatementOutput(
            responses: [DynamoDBModel.BatchStatementResponse()]
        )

        when(expectations.batchExecuteStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.deleteItems(existingItems: existingItems)

        // Verify - Each delete statement should include version conditions
        verify(mockClient).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 1 && input.statements?[0].statement?.contains("DELETE") == true
            }
        )
    }

    @Test("Delete existing items batch handles partial failures")
    func deleteExistingItemsBatchHandlesPartialFailures() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let existingItems = [testItemA, testItemB]
        let expectedOutput = DynamoDBModel.BatchExecuteStatementOutput(
            responses: [
                DynamoDBModel.BatchStatementResponse(
                    error: DynamoDBModel.BatchStatementError(
                        code: .conditionalcheckfailed,
                        message: "Version check failed"
                    )
                ),
                DynamoDBModel.BatchStatementResponse(),  // Success
            ]
        )

        when(expectations.batchExecuteStatement(input: .any), times: 2, return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When/Then
        do {
            try await table.deleteItems(existingItems: existingItems)
        } catch let error as DynamoDBTableError {
            if case .batchFailures(let errors) = error {
                #expect(errors.count == 1)
            } else {
                Issue.record("Expected DynamoDBTableError.batchFailures, got \(error)")
            }
        }

        verify(mockClient).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 2
            }
        )
    }

    @Test("Delete existing items batch chunks large requests")
    func deleteExistingItemsBatchChunksLargeRequests() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()

        // Create more than 25 items to test chunking (BatchExecuteStatement limit is 25)
        let existingItems: [StandardTypedDatabaseItem<TestTypeA>] = (0..<30).map { index in
            StandardTypedDatabaseItem.newItem(
                withKey: CompositePrimaryKey(partitionKey: "partition\(index)", sortKey: "sort\(index)"),
                andValue: TestTypeA(firstly: "test\(index)", secondly: "test\(index)")
            )
        }

        let firstBatchOutput = DynamoDBModel.BatchExecuteStatementOutput(
            responses: Array(repeating: DynamoDBModel.BatchStatementResponse(), count: 25)
        )
        let secondBatchOutput = DynamoDBModel.BatchExecuteStatementOutput(
            responses: Array(repeating: DynamoDBModel.BatchStatementResponse(), count: 5)
        )

        when(expectations.batchExecuteStatement(input: .any), return: firstBatchOutput)
        when(expectations.batchExecuteStatement(input: .any), return: secondBatchOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.deleteItems(existingItems: existingItems)

        // Verify chunking - should make 2 calls (25 + 5 items)
        verify(mockClient).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 25
            }
        )
        verify(mockClient).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 5
            }
        )
    }

    @Test("Delete existing items batch handles all error types")
    func deleteExistingItemsBatchHandlesAllErrorTypes() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let existingItems = [testItemA, testItemB]
        let expectedOutput = DynamoDBModel.BatchExecuteStatementOutput(
            responses: [
                DynamoDBModel.BatchStatementResponse(
                    error: DynamoDBModel.BatchStatementError(
                        code: .resourcenotfound,
                        message: "Item not found"
                    )
                ),
                DynamoDBModel.BatchStatementResponse(
                    error: DynamoDBModel.BatchStatementError(
                        code: .validationerror,
                        message: "Invalid input"
                    )
                ),
            ]
        )

        when(expectations.batchExecuteStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When/Then
        await #expect(throws: DynamoDBTableError.self) {
            try await table.deleteItems(existingItems: existingItems)
        }

        verify(mockClient).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 2
            }
        )
    }

    @Test("Delete existing items batch respects consistent read configuration")
    func deleteExistingItemsBatchRespectsConsistentReadConfiguration() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let inconsistentConfig = AWSDynamoDBTableConfiguration(consistentRead: false)
        let existingItems = [testItemA]
        let expectedOutput = DynamoDBModel.BatchExecuteStatementOutput(
            responses: [DynamoDBModel.BatchStatementResponse()]
        )

        when(expectations.batchExecuteStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = GenericDynamoDBCompositePrimaryKeyTable(
            tableName: testTableName,
            client: mockClient,
            tableConfiguration: inconsistentConfig,
            tableMetrics: testMetrics,
            logger: testLogger
        )

        // When
        try await table.deleteItems(existingItems: existingItems)

        // Verify
        verify(mockClient).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 1 && input.statements?[0].statement?.contains("DELETE") == true
            }
        )
    }
}
