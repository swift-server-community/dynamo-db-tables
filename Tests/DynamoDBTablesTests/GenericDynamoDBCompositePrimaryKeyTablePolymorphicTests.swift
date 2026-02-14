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
//  GenericDynamoDBCompositePrimaryKeyTablePolymorphicTests.swift
//  DynamoDBTablesTests
//

import AWSDynamoDB
import Foundation
import Logging
import Smockable
import Testing

@testable import DynamoDBTables

@Suite("AWSDynamoDBCompositePrimaryKeyTable Polymorphic Operations Tests")
struct AWSDynamoDBCompositePrimaryKeyTablePolymorphicTests {

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
        andValue: TestTypeB(thirdly: "test3", fourthly: "test4")
    )

    private let testKey1 = CompositePrimaryKey<StandardPrimaryKeyAttributes>(
        partitionKey: "partition1",
        sortKey: "sort1"
    )

    private let testKey2 = CompositePrimaryKey<StandardPrimaryKeyAttributes>(
        partitionKey: "partition2",
        sortKey: "sort2"
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

    // MARK: - Polymorphic Get Items Tests

    @Test("Polymorphic get items returns mixed types")
    func polymorphicGetItemsReturnsMixedTypes() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let keys = [testKey1, testKey2]

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let itemBAttributes = try getAttributes(forItem: testItemB)
        let expectedOutput = DynamoDBModel.BatchGetItemOutput(
            responses: [testTableName: [itemAAttributes, itemBAttributes]]
        )

        when(expectations.batchGetItem(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [CompositePrimaryKey<StandardPrimaryKeyAttributes>: TestQueryableTypes] =
            try await table.polymorphicGetItems(forKeys: keys)

        // Verify
        #expect(result.count == 2)
        #expect(result[testKey1] != nil)
        #expect(result[testKey2] != nil)
        verify(mockClient).batchGetItem(
            input: .matching { input in
                input.requestItems?[testTableName]?.keys?.count == 2
                    && input.requestItems?[testTableName]?.consistentRead == true
            }
        )
    }

    @Test("Polymorphic get items handles empty keys")
    func polymorphicGetItemsHandlesEmptyKeys() async throws {
        // Given
        let expectations = MockTestDynamoDBClientProtocol.Expectations()
        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [CompositePrimaryKey<StandardPrimaryKeyAttributes>: TestQueryableTypes] =
            try await table.polymorphicGetItems(forKeys: [])

        // Verify
        #expect(result.isEmpty)
        verify(mockClient, .never).batchGetItem(input: .any)
    }

    private func getAttributesForKey(
        key: CompositePrimaryKey<StandardPrimaryKeyAttributes>
    ) throws -> [String: DynamoDBClientTypes.AttributeValue] {
        let attributeValue = try DynamoDBEncoder().encode(key)

        if case let .m(keyAttributes) = attributeValue {
            return keyAttributes
        } else {
            throw DynamoDBTableError.unexpectedResponse(reason: "Expected a structure.")
        }
    }

    @Test("Polymorphic get items with unprocessed keys retries")
    func polymorphicGetItemsWithUnprocessedKeysRetries() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let keys = [testKey1, testKey2]
        let testKey1Attributes = try getAttributesForKey(key: testKey1)
        let testKey2Attributes = try getAttributesForKey(key: testKey2)

        // First response with unprocessed keys
        let itemBAttributes = try getAttributes(forItem: testItemB)
        let firstOutput = DynamoDBModel.BatchGetItemOutput(
            responses: [testTableName: [itemBAttributes]],
            unprocessedKeys: [testTableName: DynamoDBClientTypes.KeysAndAttributes(keys: [])]
        )

        // Second response with successful result
        let itemAAttributes = try getAttributes(forItem: testItemA)
        let secondOutput = DynamoDBModel.BatchGetItemOutput(
            responses: [testTableName: [itemAAttributes]]
        )

        when(expectations.batchGetItem(input: .any), return: firstOutput)
        when(expectations.batchGetItem(input: .any), return: secondOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [CompositePrimaryKey<StandardPrimaryKeyAttributes>: TestQueryableTypes] =
            try await table.polymorphicGetItems(forKeys: keys)

        // Verify
        #expect(result.count == 2)
        if let testKey1Result = result[testKey1], case .testTypeA(let row) = testKey1Result {
            #expect(row.rowValue == testItemA.rowValue)
        }
        if let testKey2Result = result[testKey2], case .testTypeB(let row) = testKey2Result {
            #expect(row.rowValue == testItemB.rowValue)
        }
        // First call with original request, second call with unprocessed keys
        InOrder(strict: false, mockClient) { inOrder in
            inOrder.verify(mockClient).batchGetItem(
                input: .matching { input in
                    input.requestItems?[testTableName]?.keys?.count == 2
                }
            )
            inOrder.verify(mockClient).batchGetItem(
                input: .matching { input in
                    input.requestItems?[testTableName]?.keys?.isEmpty == true
                }
            )
        }
    }

    // MARK: - Polymorphic Query Tests

    @Test("Polymorphic query basic version succeeds")
    func polymorphicQueryBasicVersionSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKey = "partition1"
        let sortKeyCondition = AttributeCondition.beginsWith("sort")

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let expectedOutput = DynamoDBModel.QueryOutput(
            items: [itemAAttributes]
        )

        when(expectations.query(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [TestQueryableTypes] = try await table.polymorphicQuery(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition
        )

        // Verify
        #expect(result.count == 1)
        verify(mockClient).query(
            input: .matching { input in
                input.tableName == testTableName && input.expressionAttributeValues?[":pk"]?.asString == partitionKey
                    && input.keyConditionExpression?.contains("begins_with") == true
            }
        )
    }

    private func getExclusiveStartKey(partitionKey: String, sortKey: String) throws -> String {
        let lastEvaluatedKey = [
            StandardPrimaryKeyAttributes.partitionKeyAttributeName: DynamoDBClientTypes.AttributeValue.s(partitionKey),
            StandardPrimaryKeyAttributes.sortKeyAttributeName: DynamoDBClientTypes.AttributeValue.s(sortKey),
        ]

        let data = try JSONEncoder().encode(lastEvaluatedKey)
        return String(data: data, encoding: .utf8)!
    }

    @Test("Polymorphic query with pagination succeeds")
    func polymorphicQueryWithPaginationSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKey = "partition1"
        let sortKeyCondition = AttributeCondition.equals("sort1")
        let limit = 10
        let exclusiveStartKey = try getExclusiveStartKey(partitionKey: partitionKey, sortKey: "firstKey")

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let lastEvaluatedKey = [
            StandardPrimaryKeyAttributes.partitionKeyAttributeName: DynamoDBClientTypes.AttributeValue.s(partitionKey),
            StandardPrimaryKeyAttributes.sortKeyAttributeName: DynamoDBClientTypes.AttributeValue.s("lastKey"),
        ]
        let expectedOutput = DynamoDBModel.QueryOutput(
            items: [itemAAttributes],
            lastEvaluatedKey: lastEvaluatedKey
        )

        when(expectations.query(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: (items: [TestQueryableTypes], lastEvaluatedKey: String?) = try await table.polymorphicQuery(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition,
            limit: limit,
            exclusiveStartKey: exclusiveStartKey
        )

        // Verify
        #expect(result.items.count == 1)
        #expect(result.lastEvaluatedKey != nil)
        verify(mockClient).query(
            input: .matching { input in
                input.tableName == testTableName && input.limit == limit && input.exclusiveStartKey != nil
            }
        )
    }

    @Test("Polymorphic query with scan direction succeeds")
    func polymorphicQueryWithScanDirectionSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKey = "partition1"
        let sortKeyCondition = AttributeCondition.lessThan("sort5")
        let limit = 5
        let scanIndexForward = false
        let exclusiveStartKey = try getExclusiveStartKey(partitionKey: partitionKey, sortKey: "firstKey")

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let expectedOutput = DynamoDBModel.QueryOutput(
            items: [itemAAttributes]
        )

        when(expectations.query(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: (items: [TestQueryableTypes], lastEvaluatedKey: String?) = try await table.polymorphicQuery(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition,
            limit: limit,
            scanIndexForward: scanIndexForward,
            exclusiveStartKey: exclusiveStartKey
        )

        // Verify
        #expect(result.items.count == 1)
        verify(mockClient).query(
            input: .matching { input in
                input.tableName == testTableName && input.scanIndexForward == false && input.limit == limit
            }
        )
    }

    @Test("Polymorphic query with no sort key condition succeeds")
    func polymorphicQueryNoSortKeyConditionSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKey = "partition1"

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let expectedOutput = DynamoDBModel.QueryOutput(
            items: [itemAAttributes]
        )

        when(expectations.query(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [TestQueryableTypes] = try await table.polymorphicQuery(
            forPartitionKey: partitionKey,
            sortKeyCondition: nil
        )

        // Verify
        #expect(result.count == 1)
        verify(mockClient).query(input: .any)
    }

    // MARK: - Polymorphic Execute Tests

    @Test("Polymorphic execute basic version succeeds")
    func polymorphicExecuteBasicVersionSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKeys = ["partition1", "partition2"]
        let attributesFilter = ["firstly", "secondly"]
        let additionalWhereClause = "firstly = 'test1'"

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let expectedOutput = DynamoDBModel.ExecuteStatementOutput(
            items: [itemAAttributes]
        )

        when(expectations.executeStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [TestQueryableTypes] = try await table.polymorphicExecute(
            partitionKeys: partitionKeys,
            attributesFilter: attributesFilter,
            additionalWhereClause: additionalWhereClause
        )

        // Verify
        #expect(result.count == 1)
        verify(mockClient).executeStatement(
            input: .matching { input in
                input.statement.contains("SELECT") == true && input.statement.contains("firstly, secondly") == true
                    && input.statement.contains("firstly = 'test1'") == true && input.consistentRead == true
            }
        )
    }

    @Test("Polymorphic execute with pagination succeeds")
    func polymorphicExecuteWithPaginationSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKeys = ["partition1"]
        let attributesFilter: [String]? = nil
        let additionalWhereClause: String? = nil
        let nextToken = "nextToken123"

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let expectedOutput = DynamoDBModel.ExecuteStatementOutput(
            items: [itemAAttributes],
            nextToken: "nextToken456"
        )

        when(expectations.executeStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: (items: [TestQueryableTypes], lastEvaluatedKey: String?) = try await table.polymorphicExecute(
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
                input.nextToken == nextToken && input.statement.contains("SELECT") == true
            }
        )
    }

    @Test("Polymorphic execute with multiple partition keys succeeds")
    func polymorphicExecuteWithMultiplePartitionKeysSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKeys = ["partition1", "partition2", "partition3"]
        let attributesFilter = ["*"]
        let additionalWhereClause: String? = nil

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let itemBAttributes = try getAttributes(forItem: testItemB)
        let expectedOutput = DynamoDBModel.ExecuteStatementOutput(
            items: [itemAAttributes, itemBAttributes]
        )

        when(expectations.executeStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [TestQueryableTypes] = try await table.polymorphicExecute(
            partitionKeys: partitionKeys,
            attributesFilter: attributesFilter,
            additionalWhereClause: additionalWhereClause
        )

        // Verify
        #expect(result.count == 2)
        verify(mockClient).executeStatement(
            input: .matching { input in
                input.statement.contains("SELECT") == true && input.statement.contains("partition1") == true
                    && input.statement.contains("partition2") == true
                    && input.statement.contains("partition3") == true
            }
        )
    }

    @Test("Polymorphic execute with empty results succeeds")
    func polymorphicExecuteWithEmptyResultsSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let partitionKeys = ["nonexistent"]
        let attributesFilter: [String]? = nil
        let additionalWhereClause: String? = nil

        let expectedOutput = DynamoDBModel.ExecuteStatementOutput(items: [])

        when(expectations.executeStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [TestQueryableTypes] = try await table.polymorphicExecute(
            partitionKeys: partitionKeys,
            attributesFilter: attributesFilter,
            additionalWhereClause: additionalWhereClause
        )

        // Verify
        #expect(result.isEmpty)
        verify(mockClient).executeStatement(
            input: .matching { input in
                input.statement.contains("SELECT") == true && input.statement.contains("nonexistent") == true
            }
        )
    }
}
