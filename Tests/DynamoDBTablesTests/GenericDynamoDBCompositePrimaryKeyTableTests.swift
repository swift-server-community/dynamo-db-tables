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
//  GenericDynamoDBCompositePrimaryKeyTableTests.swift
//  DynamoDBTablesTests
//

import AWSDynamoDB
import Foundation
import Logging
import Smockable
import Testing

@testable import DynamoDBTables

struct AWSDynamoDBCompositePrimaryKeyTableTests {

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

    private let testKey1 = CompositePrimaryKey<StandardPrimaryKeyAttributes>(
        partitionKey: "partition1",
        sortKey: "sort1"
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

    private func createExpectedPutItemInput(
        for item: StandardTypedDatabaseItem<TestTypeA>,
        isInsert: Bool = false
    ) throws -> DynamoDBModel.PutItemInput {
        let attributes = try getAttributes(forItem: item)

        if isInsert {
            let expressionAttributeNames = [
                "#pk": StandardPrimaryKeyAttributes.partitionKeyAttributeName,
                "#sk": StandardPrimaryKeyAttributes.sortKeyAttributeName,
            ]
            let conditionExpression = "attribute_not_exists (#pk) AND attribute_not_exists (#sk)"

            return DynamoDBModel.PutItemInput(
                conditionExpression: conditionExpression,
                expressionAttributeNames: expressionAttributeNames,
                item: attributes,
                tableName: testTableName
            )
        } else {
            return DynamoDBModel.PutItemInput(
                item: attributes,
                tableName: testTableName
            )
        }
    }

    // MARK: - Insert Item Tests

    @Test("Insert item succeeds with correct condition expression")
    func insertItemSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let expectedInput = try createExpectedPutItemInput(for: testItemA, isInsert: true)
        when(expectations.putItem(input: .any), complete: .withSuccess)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.insertItem(testItemA)

        // Verify
        verify(mockClient).putItem(
            input: .matching { input in
                input.tableName == testTableName && input.conditionExpression?.contains("attribute_not_exists") == true
                    && input.item["PK"]?.asString == testItemA.compositePrimaryKey.partitionKey
                    && input.item["SK"]?.asString == testItemA.compositePrimaryKey.sortKey
            }
        )
    }

    @Test("Insert item fails with conditional check failed error")
    func insertItemConditionalCheckFailed() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let conditionalCheckError = AWSDynamoDB.ConditionalCheckFailedException(message: "Item already exists")

        when(expectations.putItem(input: .any), throw: conditionalCheckError)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // Then
        do {
            try await table.insertItem(testItemA)
        } catch let error as DynamoDBTableError {
            if case .conditionalCheckFailed(let partitionKey, let sortKey, let message) = error {
                #expect(partitionKey == testItemA.compositePrimaryKey.partitionKey)
                #expect(sortKey == testItemA.compositePrimaryKey.sortKey)
                #expect(message == "Item already exists")
            } else {
                Issue.record("Expected DynamoDBTableError.duplicateItem, got \(error)")
            }
        }

        verify(mockClient).putItem(
            input: .matching { input in
                input.tableName == testTableName && input.conditionExpression?.contains("attribute_not_exists") == true
            }
        )
    }

    @Test("Clobber item succeeds without condition expressions")
    func clobberItemSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        when(expectations.putItem(input: .any), complete: .withSuccess)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.clobberItem(testItemA)

        // Verify
        verify(mockClient).putItem(
            input: .matching { input in
                input.tableName == testTableName && input.conditionExpression == nil
                    && input.item["PK"]?.asString == testItemA.compositePrimaryKey.partitionKey
                    && input.item["SK"]?.asString == testItemA.compositePrimaryKey.sortKey
            }
        )
    }

    @Test("Get item returns item when found")
    func getItemSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let itemAttributes = try getAttributes(forItem: testItemA)
        let expectedOutput = DynamoDBModel.GetItemOutput(item: itemAttributes)

        when(expectations.getItem(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: testKey1)

        // Verify
        #expect(result != nil)
        #expect(result?.compositePrimaryKey == testItemA.compositePrimaryKey)
        #expect(result?.rowValue.firstly == testItemA.rowValue.firstly)
        #expect(result?.rowValue.secondly == testItemA.rowValue.secondly)
        verify(mockClient).getItem(
            input: .matching { input in
                input.tableName == testTableName && input.key["PK"]?.asString == testKey1.partitionKey
                    && input.key["SK"]?.asString == testKey1.sortKey && input.consistentRead == true
            }
        )
    }

    @Test("Get item returns nil when not found")
    func getItemNotFound() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let expectedOutput = DynamoDBModel.GetItemOutput(item: nil)

        when(expectations.getItem(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: testKey1)

        // Verify
        #expect(result == nil)
        verify(mockClient).getItem(
            input: .matching { input in
                input.tableName == testTableName && input.key["PK"]?.asString == testKey1.partitionKey
                    && input.key["SK"]?.asString == testKey1.sortKey
            }
        )
    }

    @Test("Delete item by key succeeds")
    func deleteItemByKeySuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        when(expectations.deleteItem(input: .any), complete: .withSuccess)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.deleteItem(forKey: testKey1)

        // Verify
        verify(mockClient).deleteItem(
            input: .matching { input in
                input.tableName == testTableName && input.key["PK"]?.asString == testKey1.partitionKey
                    && input.key["SK"]?.asString == testKey1.sortKey
            }
        )
    }

    @Test("Update item succeeds with version check")
    func updateItemSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let existingItem = testItemA
        let updatedItem = StandardTypedDatabaseItem(
            compositePrimaryKey: existingItem.compositePrimaryKey,
            createDate: existingItem.createDate,
            rowStatus: RowStatus(rowVersion: existingItem.rowStatus.rowVersion + 1, lastUpdatedDate: Date()),
            rowValue: TestTypeA(firstly: "updated1", secondly: "updated2"),
            timeToLive: existingItem.timeToLive
        )

        when(expectations.putItem(input: .any), complete: .withSuccess)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.updateItem(newItem: updatedItem, existingItem: existingItem)

        // Verify
        verify(mockClient).putItem(
            input: .matching { input in
                input.tableName == testTableName && input.conditionExpression?.contains("rowversion") == true
                    && input.conditionExpression?.contains("createdate") == true
                    && input.item["PK"]?.asString == existingItem.compositePrimaryKey.partitionKey
                    && input.item["SK"]?.asString == existingItem.compositePrimaryKey.sortKey
            }
        )
    }

    @Test("Update item fails with conditional check failed error")
    func updateItemConditionalCheckFailed() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let existingItem = testItemA
        let updatedItem = StandardTypedDatabaseItem(
            compositePrimaryKey: existingItem.compositePrimaryKey,
            createDate: existingItem.createDate,
            rowStatus: RowStatus(rowVersion: existingItem.rowStatus.rowVersion + 1, lastUpdatedDate: Date()),
            rowValue: TestTypeA(firstly: "updated1", secondly: "updated2"),
            timeToLive: existingItem.timeToLive
        )

        let conditionalCheckError = AWSDynamoDB.ConditionalCheckFailedException(message: "Version mismatch")

        when(expectations.putItem(input: .any), throw: conditionalCheckError)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // Then
        do {
            try await table.updateItem(newItem: updatedItem, existingItem: existingItem)
        } catch let error as DynamoDBTableError {
            if case .conditionalCheckFailed(let partitionKey, let sortKey, let message) = error {
                #expect(partitionKey == existingItem.compositePrimaryKey.partitionKey)
                #expect(sortKey == existingItem.compositePrimaryKey.sortKey)
                #expect(message == "Version mismatch")
            } else {
                Issue.record("Expected DynamoDBTableError.conditionalCheckFailed, got \(error)")
            }
        }

        verify(mockClient).putItem(
            input: .matching { input in
                input.conditionExpression?.contains("rowversion") == true
                    && input.conditionExpression?.contains("createdate") == true
            }
        )
    }

    @Test("Query with sort key condition succeeds")
    func queryWithSortKeyConditionSuccess() async throws {
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
        let result: [StandardTypedDatabaseItem<TestTypeA>] = try await table.query(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition
        )

        // Verify
        #expect(result.count == 1)
        #expect(result[0].rowValue.firstly == testItemA.rowValue.firstly)
        verify(mockClient).query(
            input: .matching { input in
                input.tableName == testTableName && input.expressionAttributeValues?[":pk"]?.asString == partitionKey
                    && input.keyConditionExpression?.contains("begins_with") == true
            }
        )
    }

    @Test("Batch get items succeeds")
    func batchGetItemsSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let keys = [testKey1]
        let itemAAttributes = try getAttributes(forItem: testItemA)
        let expectedOutput = DynamoDBModel.BatchGetItemOutput(
            responses: [testTableName: [itemAAttributes]]
        )

        when(expectations.batchGetItem(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [CompositePrimaryKey<StandardPrimaryKeyAttributes>: StandardTypedDatabaseItem<TestTypeA>] =
            try await table.getItems(forKeys: keys)

        // Verify
        #expect(result.count == 1)
        #expect(result[testKey1] != nil)
        #expect(result[testKey1]?.rowValue.firstly == testItemA.rowValue.firstly)
        verify(mockClient).batchGetItem(
            input: .matching { input in
                input.requestItems?[testTableName]?.keys?.count == 1
                    && input.requestItems?[testTableName]?.consistentRead == true
            }
        )
    }

    @Test("Batch get items with unprocessed keys retries")
    func batchGetItemsWithUnprocessedKeysRetries() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let keys = [testKey1]

        // First response with unprocessed keys
        let firstOutput = DynamoDBModel.BatchGetItemOutput(
            responses: [:],
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
        let result: [CompositePrimaryKey<StandardPrimaryKeyAttributes>: StandardTypedDatabaseItem<TestTypeA>] =
            try await table.getItems(forKeys: keys)

        // Verify
        #expect(result.count == 1)
        #expect(result[testKey1] != nil)
        // First call with original request, second call with unprocessed keys
        InOrder(strict: false, mockClient) { inOrder in
            inOrder.verify(mockClient).batchGetItem(
                input: .matching { input in
                    input.requestItems?[testTableName]?.keys?.count == 1
                }
            )
            inOrder.verify(mockClient).batchGetItem(
                input: .matching { input in
                    input.requestItems?[testTableName] != nil
                }
            )
        }
    }

    @Test("Delete items batch succeeds")
    func deleteItemsBatchSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let keys = [testKey1]
        let expectedOutput = DynamoDBModel.BatchExecuteStatementOutput(
            responses: [DynamoDBClientTypes.BatchStatementResponse()]
        )

        when(expectations.batchExecuteStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.deleteItems(forKeys: keys)

        // Verify
        verify(mockClient).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 1 && input.statements?[0].statement?.contains("DELETE") == true
            }
        )
    }

    @Test("Execute PartiQL statement succeeds")
    func executePartiQLStatementSuccess() async throws {
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
        let result: [StandardTypedDatabaseItem<TestTypeA>] = try await table.execute(
            partitionKeys: partitionKeys,
            attributesFilter: attributesFilter,
            additionalWhereClause: additionalWhereClause
        )

        // Verify
        #expect(result.count == 1)
        #expect(result[0].rowValue.firstly == testItemA.rowValue.firstly)
        verify(mockClient).executeStatement(
            input: .matching { input in
                input.statement.contains("SELECT") == true && input.statement.contains("firstly, secondly") == true
                    && input.statement.contains("firstly = 'test1'") == true && input.consistentRead == true
            }
        )
    }

    @Test("Configuration consistent read setting is respected")
    func configurationConsistentReadSetting() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let inconsistentConfig = AWSDynamoDBTableConfiguration(consistentRead: false)
        let expectedOutput = DynamoDBModel.GetItemOutput(item: nil)

        when(expectations.getItem(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = GenericDynamoDBCompositePrimaryKeyTable(
            tableName: testTableName,
            client: mockClient,
            tableConfiguration: inconsistentConfig,
            tableMetrics: testMetrics,
            logger: testLogger
        )

        // When
        let _: StandardTypedDatabaseItem<TestTypeA>? = try await table.getItem(forKey: testKey1)

        // Verify
        verify(mockClient).getItem(
            input: .matching { input in
                input.tableName == testTableName && input.consistentRead == false  // Configuration setting
            }
        )
    }

    @Test("Empty key list returns empty result for getItems")
    func emptyKeyListReturnsEmptyResultForGetItems() async throws {
        // Given
        let expectations = MockTestDynamoDBClientProtocol.Expectations()
        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let result: [CompositePrimaryKey<StandardPrimaryKeyAttributes>: StandardTypedDatabaseItem<TestTypeA>] =
            try await table.getItems(forKeys: [])

        // Verify
        #expect(result.isEmpty)
        verify(mockClient, .never).batchGetItem(input: .any)  // No calls should be made
    }

    @Test("Empty key list does nothing for deleteItems")
    func emptyKeyListDoesNothingForDeleteItems() async throws {
        // Given
        let expectations = MockTestDynamoDBClientProtocol.Expectations()
        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        let items: [StandardCompositePrimaryKey] = []
        try await table.deleteItems(forKeys: items)

        // Verify
        verify(mockClient, .never).batchExecuteStatement(input: .any)  // No calls should be made
    }
}
