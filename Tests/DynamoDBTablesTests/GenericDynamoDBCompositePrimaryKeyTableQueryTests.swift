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
//  GenericDynamoDBCompositePrimaryKeyTableQueryTests.swift
//  DynamoDBTablesTests
//

import AWSDynamoDB
import Foundation
import Logging
import Smockable
import Testing

@testable import DynamoDBTables

@Suite("AWSDynamoDBCompositePrimaryKeyTable Query Operations Tests")
struct AWSDynamoDBCompositePrimaryKeyTableQueryTests {

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
        withKey: CompositePrimaryKey(partitionKey: "partition1", sortKey: "sort2"),
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

    // MARK: - Monomorphic Query Tests

    @Test("Monomorphic query basic version succeeds")
    func monomorphicQueryBasicVersionSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKey = "partition1"
        let sortKeyCondition = AttributeCondition.beginsWith("sort")

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let itemBAttributes = try getAttributes(forItem: testItemB)
        let expectedOutput = DynamoDBModel.QueryOutput(
            items: [itemAAttributes, itemBAttributes]
        )

        when(expectations.query(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [StandardTypedDatabaseItem<TestTypeA>] = try await table.query(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition
        )

        // Verify
        #expect(result.count == 2)
        #expect(result[0].rowValue.firstly == "test1")
        #expect(result[1].rowValue.firstly == "test3")
        verify(mockClient).query(
            input: .matching { input in
                input.tableName == testTableName && input.expressionAttributeValues?[":pk"]?.asString == partitionKey
                    && input.keyConditionExpression?.contains("begins_with") == true
            }
        )
    }

    private func getExclusiveStartKey(partitionKey: String, sortKey: String) throws -> String {
        let lastEvaluatedKey = [
            StandardPrimaryKeyAttributes.partitionKeyAttributeName: DynamoDBModel.AttributeValue.s(partitionKey),
            StandardPrimaryKeyAttributes.sortKeyAttributeName: DynamoDBModel.AttributeValue.s(sortKey),
        ]

        let data = try JSONEncoder().encode(lastEvaluatedKey)
        return String(data: data, encoding: .utf8)!
    }

    @Test("Monomorphic query with pagination and scan direction succeeds")
    func monomorphicQueryWithPaginationAndScanDirectionSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKey = "partition1"
        let sortKeyCondition = AttributeCondition.lessThanOrEqual("sort5")
        let limit = 10
        let scanIndexForward = false
        let exclusiveStartKey = try getExclusiveStartKey(partitionKey: partitionKey, sortKey: "firstKey")

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let lastEvaluatedKey = [
            StandardPrimaryKeyAttributes.partitionKeyAttributeName: DynamoDBModel.AttributeValue.s("partition1"),
            StandardPrimaryKeyAttributes.sortKeyAttributeName: DynamoDBModel.AttributeValue.s("sort1"),
        ]
        let expectedOutput = DynamoDBModel.QueryOutput(
            items: [itemAAttributes],
            lastEvaluatedKey: lastEvaluatedKey
        )

        when(expectations.query(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: (items: [StandardTypedDatabaseItem<TestTypeA>], lastEvaluatedKey: String?) =
            try await table.query(
                forPartitionKey: partitionKey,
                sortKeyCondition: sortKeyCondition,
                limit: limit,
                scanIndexForward: scanIndexForward,
                exclusiveStartKey: exclusiveStartKey
            )

        // Verify
        #expect(result.items.count == 1)
        #expect(result.lastEvaluatedKey != nil)
        verify(mockClient).query(
            input: .matching { input in
                input.limit == limit && input.scanIndexForward == false && input.exclusiveStartKey != nil
            }
        )
    }

    @Test("Monomorphic query with all sort key condition types")
    func monomorphicQueryWithAllSortKeyConditionTypes() async throws {
        let testCases: [(AttributeCondition, String)] = [
            (.equals("value"), "= :sortKey"),
            (.lessThan("value"), "< :sortKey"),
            (.lessThanOrEqual("value"), "<= :sortKey"),
            (.greaterThan("value"), "> :sortKey"),
            (.greaterThanOrEqual("value"), ">= :sortKey"),
            (.between("value1", "value2"), "BETWEEN :sortKey AND :sortKey2"),
            (.beginsWith("prefix"), "begins_with(#sortKey, :sortKey)"),
        ]

        for (condition, _) in testCases {
            // Given
            var expectations = MockTestDynamoDBClientProtocol.Expectations()
            let partitionKey = "partition1"

            let expectedOutput = DynamoDBModel.QueryOutput(items: [])
            when(expectations.query(input: .any), return: expectedOutput)

            let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
            let table = createTable(with: mockClient)

            // When
            let _: [StandardTypedDatabaseItem<TestTypeA>] = try await table.query(
                forPartitionKey: partitionKey,
                sortKeyCondition: condition
            )

            // Verify
            verify(mockClient).query(
                input: .matching { input in
                    input.tableName == testTableName
                }
            )
        }
    }

    @Test("Monomorphic query with no sort key condition succeeds")
    func monomorphicQueryNoSortKeyConditionSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKey = "partition1"

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let itemBAttributes = try getAttributes(forItem: testItemB)
        let expectedOutput = DynamoDBModel.QueryOutput(
            items: [itemAAttributes, itemBAttributes]
        )

        when(expectations.query(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [StandardTypedDatabaseItem<TestTypeA>] = try await table.query(
            forPartitionKey: partitionKey,
            sortKeyCondition: nil
        )

        // Verify
        #expect(result.count == 2)
        verify(mockClient).query(
            input: .matching { input in
                input.tableName == testTableName && input.expressionAttributeValues?[":pk"]?.asString == partitionKey
            }
        )
    }

    @Test("Monomorphic query with empty results succeeds")
    func monomorphicQueryWithEmptyResultsSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKey = "nonexistent"
        let sortKeyCondition = AttributeCondition.equals("sort1")

        let expectedOutput = DynamoDBModel.QueryOutput(items: [])

        when(expectations.query(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [StandardTypedDatabaseItem<TestTypeA>] = try await table.query(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition
        )

        // Verify
        #expect(result.isEmpty)
        verify(mockClient).query(
            input: .matching { input in
                input.tableName == testTableName && input.expressionAttributeValues?[":pk"]?.asString == partitionKey
            }
        )
    }

    @Test("Monomorphic query handles large result sets with multiple pages")
    func monomorphicQueryHandlesLargeResultSetsWithMultiplePages() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKey = "partition1"
        let sortKeyCondition = AttributeCondition.beginsWith("sort")

        // First page
        let itemAAttributes = try getAttributes(forItem: testItemA)
        let lastEvaluatedKey = [
            StandardPrimaryKeyAttributes.partitionKeyAttributeName: DynamoDBModel.AttributeValue.s("partition1"),
            StandardPrimaryKeyAttributes.sortKeyAttributeName: DynamoDBModel.AttributeValue.s("sort1"),
        ]
        let firstPageOutput = DynamoDBModel.QueryOutput(
            items: [itemAAttributes],
            lastEvaluatedKey: lastEvaluatedKey
        )

        // Second page
        let itemBAttributes = try getAttributes(forItem: testItemB)
        let secondPageOutput = DynamoDBModel.QueryOutput(
            items: [itemBAttributes]
        )

        when(expectations.query(input: .any), return: firstPageOutput)
        when(expectations.query(input: .any), return: secondPageOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [StandardTypedDatabaseItem<TestTypeA>] = try await table.query(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition
        )

        // Verify
        #expect(result.count == 2)
        // Verify two query calls were made in order
        InOrder(strict: false, mockClient) { inOrder in
            inOrder.verify(mockClient).query(
                input: .matching { input in
                    input.tableName == testTableName
                        && input.expressionAttributeValues?[":pk"]?.asString == partitionKey
                }
            )
            inOrder.verify(mockClient).query(
                input: .matching { input in
                    input.tableName == testTableName
                        && input.expressionAttributeValues?[":pk"]?.asString == partitionKey
                }
            )
        }
    }

    @Test("Monomorphic query with scan index forward false reverses order")
    func monomorphicQueryWithScanIndexForwardFalseReversesOrder() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKey = "partition1"
        let sortKeyCondition: AttributeCondition? = nil
        let limit = 10
        let scanIndexForward = false
        let exclusiveStartKey: String? = nil

        // Items returned in reverse order
        let itemBAttributes = try getAttributes(forItem: testItemB)
        let itemAAttributes = try getAttributes(forItem: testItemA)
        let expectedOutput = DynamoDBModel.QueryOutput(
            items: [itemBAttributes, itemAAttributes]  // Reversed order
        )

        when(expectations.query(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: (items: [StandardTypedDatabaseItem<TestTypeA>], lastEvaluatedKey: String?) =
            try await table.query(
                forPartitionKey: partitionKey,
                sortKeyCondition: sortKeyCondition,
                limit: limit,
                scanIndexForward: scanIndexForward,
                exclusiveStartKey: exclusiveStartKey
            )

        // Verify
        #expect(result.items.count == 2)
        #expect(result.items[0].compositePrimaryKey.sortKey == "sort2")  // testItemB first
        #expect(result.items[1].compositePrimaryKey.sortKey == "sort1")  // testItemA second
        verify(mockClient).query(
            input: .matching { input in
                input.scanIndexForward == false
            }
        )
    }
}
