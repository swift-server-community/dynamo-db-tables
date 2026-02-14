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
//  GenericDynamoDBCompositePrimaryKeyTableExecuteTests.swift
//  DynamoDBTablesTests
//

import AWSDynamoDB
import Foundation
import Logging
import Smockable
import Testing

@testable import DynamoDBTables

@Suite("AWSDynamoDBCompositePrimaryKeyTable Execute Operations Tests")
struct AWSDynamoDBCompositePrimaryKeyTableExecuteTests {

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

    // MARK: - Monomorphic Execute Tests

    @Test("Monomorphic execute basic version succeeds")
    func monomorphicExecuteBasicVersionSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKeys = ["partition1", "partition2"]
        let attributesFilter = ["firstly", "secondly"]
        let additionalWhereClause = "firstly = 'test1'"

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let itemBAttributes = try getAttributes(forItem: testItemB)
        let expectedOutput = AWSDynamoDB.ExecuteStatementOutput(
            items: [itemAAttributes, itemBAttributes]
        )

        when(expectations.executeStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [StandardTypedDatabaseItem<TestTypeA>] = try await table.execute(
            partitionKeys: partitionKeys,
            attributesFilter: attributesFilter,
            additionalWhereClause: additionalWhereClause
        )

        // Verify
        #expect(result.count == 2)
        #expect(result[0].rowValue.firstly == "test1")
        #expect(result[1].rowValue.firstly == "test3")
        verify(mockClient).executeStatement(
            input: .matching { input in
                input.statement?.contains("partition1") == true && input.statement?.contains("partition2") == true
                    && input.statement?.contains("firstly = 'test1'") == true
            }
        )
    }

    @Test("Monomorphic execute with pagination succeeds")
    func monomorphicExecuteWithPaginationSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKeys = ["partition1"]
        let attributesFilter: [String]? = nil
        let additionalWhereClause: String? = nil
        let nextToken = "nextToken123"

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let expectedOutput = AWSDynamoDB.ExecuteStatementOutput(
            items: [itemAAttributes],
            nextToken: "nextToken456"
        )

        when(expectations.executeStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: (items: [StandardTypedDatabaseItem<TestTypeA>], lastEvaluatedKey: String?) =
            try await table.execute(
                partitionKeys: partitionKeys,
                attributesFilter: attributesFilter,
                additionalWhereClause: additionalWhereClause,
                nextToken: nextToken
            )

        // Verify
        #expect(result.items.count == 1)
        #expect(result.lastEvaluatedKey == "nextToken456")
        verify(mockClient).executeStatement(
            input: .matching { input in
                input.nextToken == nextToken
            }
        )
    }

    @Test("Monomorphic execute with multiple partition keys succeeds")
    func monomorphicExecuteWithMultiplePartitionKeysSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKeys = ["partition1", "partition2", "partition3"]
        let attributesFilter: [String]? = nil
        let additionalWhereClause: String? = nil

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let itemBAttributes = try getAttributes(forItem: testItemB)
        let expectedOutput = AWSDynamoDB.ExecuteStatementOutput(
            items: [itemAAttributes, itemBAttributes]
        )

        when(expectations.executeStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [StandardTypedDatabaseItem<TestTypeA>] = try await table.execute(
            partitionKeys: partitionKeys,
            attributesFilter: attributesFilter,
            additionalWhereClause: additionalWhereClause
        )

        // Verify
        #expect(result.count == 2)
        verify(mockClient).executeStatement(
            input: .matching { input in
                input.statement?.contains("partition1") == true && input.statement?.contains("partition2") == true
                    && input.statement?.contains("partition3") == true
            }
        )
    }

    @Test("Monomorphic execute with specific attributes filter succeeds")
    func monomorphicExecuteWithSpecificAttributesFilterSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKeys = ["partition1"]
        let attributesFilter = ["firstly", "compositePrimaryKey"]
        let additionalWhereClause: String? = nil

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let expectedOutput = AWSDynamoDB.ExecuteStatementOutput(
            items: [itemAAttributes]
        )

        when(expectations.executeStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [StandardTypedDatabaseItem<TestTypeA>] = try await table.execute(
            partitionKeys: partitionKeys,
            attributesFilter: attributesFilter,
            additionalWhereClause: additionalWhereClause
        )

        // Verify
        #expect(result.count == 1)
        verify(mockClient).executeStatement(
            input: .matching { input in
                input.statement?.contains("firstly, compositePrimaryKey") == true
            }
        )
    }

    @Test("Monomorphic execute with complex where clause succeeds")
    func monomorphicExecuteWithComplexWhereClauseSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKeys = ["partition1", "partition2"]
        let attributesFilter: [String]? = nil
        let additionalWhereClause = "firstly = 'test1' AND secondly BEGINS_WITH 'test'"

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let expectedOutput = AWSDynamoDB.ExecuteStatementOutput(
            items: [itemAAttributes]
        )

        when(expectations.executeStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [StandardTypedDatabaseItem<TestTypeA>] = try await table.execute(
            partitionKeys: partitionKeys,
            attributesFilter: attributesFilter,
            additionalWhereClause: additionalWhereClause
        )

        // Verify
        #expect(result.count == 1)
        verify(mockClient).executeStatement(
            input: .matching { input in
                input.statement?.contains("firstly = 'test1' AND secondly BEGINS_WITH 'test'") == true
            }
        )
    }

    @Test("Monomorphic execute with empty results succeeds")
    func monomorphicExecuteWithEmptyResultsSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKeys = ["nonexistent"]
        let attributesFilter: [String]? = nil
        let additionalWhereClause: String? = nil

        let expectedOutput = AWSDynamoDB.ExecuteStatementOutput(items: [])

        when(expectations.executeStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [StandardTypedDatabaseItem<TestTypeA>] = try await table.execute(
            partitionKeys: partitionKeys,
            attributesFilter: attributesFilter,
            additionalWhereClause: additionalWhereClause
        )

        // Verify
        #expect(result.isEmpty)
        verify(mockClient).executeStatement(
            input: .matching { input in
                input.statement?.contains("nonexistent") == true
            }
        )
    }

    @Test("Monomorphic execute handles multiple pages automatically")
    func monomorphicExecuteHandlesMultiplePagesAutomatically() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKeys = ["partition1"]
        let attributesFilter: [String]? = nil
        let additionalWhereClause: String? = nil

        // First page with nextToken
        let itemAAttributes = try getAttributes(forItem: testItemA)
        let firstPageOutput = AWSDynamoDB.ExecuteStatementOutput(
            items: [itemAAttributes],
            nextToken: "token123"
        )

        // Second page without nextToken (end of results)
        let itemBAttributes = try getAttributes(forItem: testItemB)
        let secondPageOutput = AWSDynamoDB.ExecuteStatementOutput(
            items: [itemBAttributes]
        )

        when(expectations.executeStatement(input: .any), return: firstPageOutput)
        when(expectations.executeStatement(input: .any), return: secondPageOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [StandardTypedDatabaseItem<TestTypeA>] = try await table.execute(
            partitionKeys: partitionKeys,
            attributesFilter: attributesFilter,
            additionalWhereClause: additionalWhereClause
        )

        // Verify all results from both pages are returned
        #expect(result.count == 2)
        // First call should have no nextToken, second call should have the token from first response
        InOrder(strict: false, mockClient) { inOrder in
            inOrder.verify(mockClient).executeStatement(
                input: .matching { input in
                    input.nextToken == nil
                }
            )
            inOrder.verify(mockClient).executeStatement(
                input: .matching { input in
                    input.nextToken == "token123"
                }
            )
        }
    }

    @Test("Monomorphic execute with pagination handles single page")
    func monomorphicExecuteWithPaginationHandlesSinglePage() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKeys = ["partition1"]
        let attributesFilter: [String]? = nil
        let additionalWhereClause: String? = nil
        let nextToken = "initialToken"

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let expectedOutput = AWSDynamoDB.ExecuteStatementOutput(
            items: [itemAAttributes]
            // No nextToken means this is the last page
        )

        when(expectations.executeStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: (items: [StandardTypedDatabaseItem<TestTypeA>], lastEvaluatedKey: String?) =
            try await table.execute(
                partitionKeys: partitionKeys,
                attributesFilter: attributesFilter,
                additionalWhereClause: additionalWhereClause,
                nextToken: nextToken
            )

        // Verify
        #expect(result.items.count == 1)
        #expect(result.lastEvaluatedKey == nil)  // No more pages
        verify(mockClient).executeStatement(
            input: .matching { input in
                input.nextToken == "initialToken"
            }
        )
    }

    @Test("Monomorphic execute with single partition key succeeds")
    func monomorphicExecuteWithSinglePartitionKeySuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKeys = ["partition1"]
        let attributesFilter = ["*"]  // Select all attributes
        let additionalWhereClause: String? = nil

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let expectedOutput = AWSDynamoDB.ExecuteStatementOutput(
            items: [itemAAttributes]
        )

        when(expectations.executeStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [StandardTypedDatabaseItem<TestTypeA>] = try await table.execute(
            partitionKeys: partitionKeys,
            attributesFilter: attributesFilter,
            additionalWhereClause: additionalWhereClause
        )

        // Verify
        #expect(result.count == 1)
        verify(mockClient).executeStatement(
            input: .matching { input in
                input.statement?.contains("partition1") == true && input.consistentRead == true
            }
        )
    }

    @Test("Monomorphic execute respects consistent read configuration")
    func monomorphicExecuteRespectsConsistentReadConfiguration() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let inconsistentConfig = AWSDynamoDBTableConfiguration(consistentRead: false)
        let partitionKeys = ["partition1"]
        let attributesFilter: [String]? = nil
        let additionalWhereClause: String? = nil

        let expectedOutput = AWSDynamoDB.ExecuteStatementOutput(items: [])
        when(expectations.executeStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = GenericDynamoDBCompositePrimaryKeyTable(
            tableName: testTableName,
            client: mockClient,
            tableConfiguration: inconsistentConfig,
            tableMetrics: testMetrics,
            logger: testLogger
        )

        // When
        let _: [StandardTypedDatabaseItem<TestTypeA>] = try await table.execute(
            partitionKeys: partitionKeys,
            attributesFilter: attributesFilter,
            additionalWhereClause: additionalWhereClause
        )

        // Verify
        verify(mockClient).executeStatement(
            input: .matching { input in
                input.consistentRead == false
            }
        )
    }
}
