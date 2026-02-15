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
//  GenericDynamoDBCompositePrimaryKeysProjectionTests.swift
//  DynamoDBTablesTests
//

import Foundation
import Logging
import Smockable
import Testing

@testable import DynamoDBTables

struct GenericDynamoDBCompositePrimaryKeysProjectionTests {

    // MARK: - Test Configuration

    private let testTableName = "TestTable"
    private let testLogger = Logger(label: "TestLogger")
    private let testConfiguration = AWSDynamoDBTableConfiguration(
        consistentRead: true,
        escapeSingleQuoteInPartiQL: false,
        retry: .default
    )

    // MARK: - Helper Methods

    private func createProjection(
        with mockClient: MockTestDynamoDBClientProtocol,
        tableConfiguration: AWSDynamoDBTableConfiguration? = nil
    ) -> GenericDynamoDBCompositePrimaryKeysProjection<MockTestDynamoDBClientProtocol> {
        GenericDynamoDBCompositePrimaryKeysProjection(
            tableName: testTableName,
            client: mockClient,
            tableConfiguration: tableConfiguration ?? testConfiguration,
            logger: testLogger
        )
    }

    private func getAttributesForKey(
        key: CompositePrimaryKey<StandardPrimaryKeyAttributes>
    ) throws -> [String: DynamoDBModel.AttributeValue] {
        let attributeValue = try DynamoDBEncoder().encode(key)

        if case let .m(keyAttributes) = attributeValue {
            return keyAttributes
        } else {
            throw DynamoDBTableError.unexpectedResponse(reason: "Expected a structure.")
        }
    }

    // MARK: - Query Tests

    @Test("Query returns keys successfully")
    func queryReturnsKeysSuccessfully() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let key = CompositePrimaryKey<StandardPrimaryKeyAttributes>(
            partitionKey: "partition1",
            sortKey: "sort1"
        )
        let keyAttributes = try getAttributesForKey(key: key)
        let expectedOutput = DynamoDBModel.QueryOutput(
            items: [keyAttributes]
        )

        when(expectations.query(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let projection = createProjection(with: mockClient)

        // When
        let result: [CompositePrimaryKey<StandardPrimaryKeyAttributes>] = try await projection.query(
            forPartitionKey: "partition1",
            sortKeyCondition: .beginsWith("sort")
        )

        // Verify
        #expect(result.count == 1)
        #expect(result[0].partitionKey == "partition1")
        #expect(result[0].sortKey == "sort1")
        verify(mockClient).query(
            input: .matching { input in
                input.tableName == testTableName
                    && input.expressionAttributeValues?[":pk"]?.asString == "partition1"
                    && input.keyConditionExpression?.contains("begins_with") == true
            }
        )
    }

    @Test("Query with no sort key condition")
    func queryWithNoSortKeyCondition() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let expectedOutput = DynamoDBModel.QueryOutput(items: [])

        when(expectations.query(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let projection = createProjection(with: mockClient)

        // When
        let _: [CompositePrimaryKey<StandardPrimaryKeyAttributes>] = try await projection.query(
            forPartitionKey: "partition1",
            sortKeyCondition: nil
        )

        // Verify
        verify(mockClient).query(
            input: .matching { input in
                input.keyConditionExpression == "#pk= :pk"
                    && input.expressionAttributeNames?["#sk"] == nil
            }
        )
    }

    @Test("Query returns empty results")
    func queryReturnsEmptyResults() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let expectedOutput = DynamoDBModel.QueryOutput(items: [])

        when(expectations.query(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let projection = createProjection(with: mockClient)

        // When
        let result: [CompositePrimaryKey<StandardPrimaryKeyAttributes>] = try await projection.query(
            forPartitionKey: "partition1",
            sortKeyCondition: .beginsWith("sort")
        )

        // Verify
        #expect(result.isEmpty)
    }

    @Test("Query with pagination auto-follows lastEvaluatedKey")
    func queryWithPaginationAutoFollows() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()

        let key1 = CompositePrimaryKey<StandardPrimaryKeyAttributes>(
            partitionKey: "partition1",
            sortKey: "sort1"
        )
        let key2 = CompositePrimaryKey<StandardPrimaryKeyAttributes>(
            partitionKey: "partition1",
            sortKey: "sort2"
        )
        let key1Attributes = try getAttributesForKey(key: key1)
        let key2Attributes = try getAttributesForKey(key: key2)

        let lastEvaluatedKey: [String: DynamoDBModel.AttributeValue] = [
            "PK": .s("partition1"),
            "SK": .s("sort1"),
        ]

        let firstOutput = DynamoDBModel.QueryOutput(
            items: [key1Attributes],
            lastEvaluatedKey: lastEvaluatedKey
        )
        let secondOutput = DynamoDBModel.QueryOutput(
            items: [key2Attributes]
        )

        when(expectations.query(input: .any), return: firstOutput)
        when(expectations.query(input: .any), return: secondOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let projection = createProjection(with: mockClient)

        // When
        let result: [CompositePrimaryKey<StandardPrimaryKeyAttributes>] = try await projection.query(
            forPartitionKey: "partition1",
            sortKeyCondition: .beginsWith("sort")
        )

        // Verify
        #expect(result.count == 2)
        #expect(result[0].sortKey == "sort1")
        #expect(result[1].sortKey == "sort2")
        InOrder(strict: false, mockClient) { inOrder in
            inOrder.verify(mockClient).query(
                input: .matching { input in
                    input.exclusiveStartKey == nil
                }
            )
            inOrder.verify(mockClient).query(
                input: .matching { input in
                    input.exclusiveStartKey != nil
                }
            )
        }
    }

    @Test("Paginated query returns lastEvaluatedKey")
    func paginatedQueryReturnsLastEvaluatedKey() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()

        let key = CompositePrimaryKey<StandardPrimaryKeyAttributes>(
            partitionKey: "partition1",
            sortKey: "sort1"
        )
        let keyAttributes = try getAttributesForKey(key: key)

        let lastEvaluatedKey: [String: DynamoDBModel.AttributeValue] = [
            "PK": .s("partition1"),
            "SK": .s("sort1"),
        ]

        let expectedOutput = DynamoDBModel.QueryOutput(
            items: [keyAttributes],
            lastEvaluatedKey: lastEvaluatedKey
        )

        when(expectations.query(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let projection = createProjection(with: mockClient)

        // When
        let result: (keys: [CompositePrimaryKey<StandardPrimaryKeyAttributes>], lastEvaluatedKey: String?) =
            try await projection.query(
                forPartitionKey: "partition1",
                sortKeyCondition: .beginsWith("sort"),
                limit: 1,
                exclusiveStartKey: nil
            )

        // Verify
        #expect(result.keys.count == 1)
        #expect(result.keys[0].partitionKey == "partition1")
        #expect(result.lastEvaluatedKey != nil)
    }

    @Test("Paginated query with scanIndexForward false")
    func paginatedQueryWithScanIndexForwardFalse() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let expectedOutput = DynamoDBModel.QueryOutput(items: [])

        when(expectations.query(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let projection = createProjection(with: mockClient)

        // When
        let _: (keys: [CompositePrimaryKey<StandardPrimaryKeyAttributes>], lastEvaluatedKey: String?) =
            try await projection.query(
                forPartitionKey: "partition1",
                sortKeyCondition: .beginsWith("sort"),
                limit: 10,
                scanIndexForward: false,
                exclusiveStartKey: nil
            )

        // Verify
        verify(mockClient).query(
            input: .matching { input in
                input.scanIndexForward == false
            }
        )
    }

    @Test("Query respects consistentRead configuration")
    func queryRespectsConsistentReadConfiguration() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let inconsistentConfig = AWSDynamoDBTableConfiguration(consistentRead: false)
        let expectedOutput = DynamoDBModel.QueryOutput(items: [])

        when(expectations.query(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let projection = createProjection(with: mockClient, tableConfiguration: inconsistentConfig)

        // When
        let _: [CompositePrimaryKey<StandardPrimaryKeyAttributes>] = try await projection.query(
            forPartitionKey: "partition1",
            sortKeyCondition: .beginsWith("sort")
        )

        // Verify
        verify(mockClient).query(
            input: .matching { input in
                input.consistentRead == false
            }
        )
    }

    @Test("Query with nil items in output returns empty array")
    func queryWithNilItemsReturnsEmptyArray() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let expectedOutput = DynamoDBModel.QueryOutput(items: nil)

        when(expectations.query(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let projection = createProjection(with: mockClient)

        // When
        let result: (keys: [CompositePrimaryKey<StandardPrimaryKeyAttributes>], lastEvaluatedKey: String?) =
            try await projection.query(
                forPartitionKey: "partition1",
                sortKeyCondition: nil,
                limit: 10,
                exclusiveStartKey: nil
            )

        // Verify
        #expect(result.keys.isEmpty)
        #expect(result.lastEvaluatedKey == nil)
    }
}
