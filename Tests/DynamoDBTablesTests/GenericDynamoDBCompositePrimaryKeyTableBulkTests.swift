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
//  GenericDynamoDBCompositePrimaryKeyTableBulkTests.swift
//  DynamoDBTablesTests
//

import Foundation
import Logging
import Smockable
import Testing

@testable import DynamoDBTables

@Suite("AWSDynamoDBCompositePrimaryKeyTable Bulk Operations Tests")
struct AWSDynamoDBCompositePrimaryKeyTableBulkTests {

    // MARK: - Test Configuration

    private let testTableName = "TestTable"
    private let testLogger = Logger(label: "TestLogger")
    private let testConfiguration = DynamoDBTableConfiguration(
        consistentRead: true,
        escapeSingleQuoteInPartiQL: false,
        retry: .default
    )
    private let testMetrics = DynamoDBTableMetrics()

    // MARK: - Test Data

    private let testItemA = StandardTypedDatabaseItem.newItem(
        withKey: CompositePrimaryKey(partitionKey: "partition1", sortKey: "sort1"),
        andValue: TestTypeA(firstly: "test1", secondly: "test2")
    )

    private let testItemB = StandardTypedDatabaseItem.newItem(
        withKey: CompositePrimaryKey(partitionKey: "partition2", sortKey: "sort2"),
        andValue: TestTypeB(thirdly: "test3", fourthly: "test4")
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

    // MARK: - Bulk Write Tests

    @Test("Bulk write with small batch")
    func bulkWriteSmallBatch() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let writeEntries: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: testItemA),
            .deleteAtKey(key: testKey2),
        ]

        let expectedOutput = DynamoDBModel.BatchExecuteStatementOutput()
        when(expectations.batchExecuteStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.bulkWrite(writeEntries)

        // Verify transaction is used for small batches
        verify(mockClient).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 2
            }
        )
        verify(mockClient, .never).putItem(input: .any)
        verify(mockClient, .never).deleteItem(input: .any)
    }

    @Test("Bulk write with large batch")
    func bulkWriteLargeBatch() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()

        let writeEntries: [StandardWriteEntry<TestTypeA>] = (0..<101).map { index in
            let item = StandardTypedDatabaseItem.newItem(
                withKey: CompositePrimaryKey(partitionKey: "partition\(index)", sortKey: "sort\(index)"),
                andValue: TestTypeA(firstly: "test\(index)", secondly: "test\(index)")
            )
            return .insert(new: item)
        }

        let expectedOutput = DynamoDBModel.BatchExecuteStatementOutput()
        when(expectations.batchExecuteStatement(input: .any), times: .unbounded, return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.bulkWrite(writeEntries)

        // Entries will get chunked into statements of 25
        verify(mockClient, times: 4).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 25
            }
        )
        verify(mockClient).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 1
            }
        )
        verify(mockClient, .never).putItem(input: .any)
    }

    private func getWriteEntriesWithSomeLargeSortKeys() -> [StandardWriteEntry<TestTypeA>] {
        return (0..<101).map { index in
            let payload = TestTypeA(firstly: "test\(index)", secondly: "test\(index)")

            if index % 10 == 0 {
                let item = StandardTypedDatabaseItem.newItem(
                    withKey: CompositePrimaryKey(
                        partitionKey: "partition\(index)",
                        sortKey:
                            "sort\(index)firstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstly",
                    ),
                    andValue: payload
                )

                let updatedPayload = TestTypeA(firstly: "test\(index)!!", secondly: "test\(index)!!")
                let updated = item.createUpdatedItem(withValue: updatedPayload)

                if index % 40 == 0 {
                    return .update(
                        new: updated,
                        existing: item
                    )
                } else if (index + 10) % 40 == 0 {
                    return .deleteItem(existing: updated)
                } else if (index + 20) % 40 == 0 {
                    return .deleteAtKey(key: item.compositePrimaryKey)
                }

                return .insert(new: updated)
            }

            let item = StandardTypedDatabaseItem.newItem(
                withKey: CompositePrimaryKey(partitionKey: "partition\(index)", sortKey: "sort\(index)"),
                andValue: payload
            )

            return .insert(new: item)
        }
    }

    @Test("Bulk write with insert with fallback")
    func bulkWriteWithFallbackHasFallsBack() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let writeEntries = getWriteEntriesWithSomeLargeSortKeys()

        // First call to transaction fails
        when(
            expectations.batchExecuteStatement(input: .any),
            times: .unbounded,
            return: DynamoDBModel.BatchExecuteStatementOutput()
        )
        when(expectations.putItem(input: .any), times: .unbounded, complete: .withSuccess)
        when(expectations.deleteItem(input: .any), times: .unbounded, complete: .withSuccess)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.bulkWriteWithFallback(writeEntries)

        // Verify transaction is tried first, then individual operations
        verify(mockClient, times: 3).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 25
            }
        )
        verify(mockClient).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 15
            }
        )
        // The items that exceed the statement length - both update and insert call putItem
        verify(mockClient, times: 6).putItem(
            input: .matching { input in
                input.tableName == testTableName
            }
        )
        // The items that exceed the statement length - both deleteItem and deleteAtKey call deleteItem
        verify(mockClient, times: 5).deleteItem(
            input: .matching { input in
                input.tableName == testTableName
            }
        )
    }

    @Test("Bulk write with fallback succeeds with just batch execute statement")
    func bulkWriteWithFallbackSucceedsWithJustBatchExecuteStatement() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let writeEntries: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: testItemA)
        ]

        let expectedOutput = DynamoDBModel.BatchExecuteStatementOutput()
        when(expectations.batchExecuteStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.bulkWriteWithFallback(writeEntries)

        // Verify only transaction is used when it succeeds
        verify(mockClient).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 1
            }
        )
        verify(mockClient, .never).putItem(input: .any)
        verify(mockClient, .never).deleteItem(input: .any)
    }

    @Test("Polymorphic bulk write succeeds")
    func polymorphicBulkWriteSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let writeEntries: [TestPolymorphicWriteEntry] = [
            .testTypeA(.insert(new: testItemA)),
            .testTypeB(.insert(new: testItemB)),
        ]

        let expectedOutput = DynamoDBModel.BatchExecuteStatementOutput()
        when(expectations.batchExecuteStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.polymorphicBulkWrite(writeEntries)

        // Verify
        verify(mockClient).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 2
            }
        )
    }

    @Test("Bulk write with empty entries succeeds")
    func bulkWriteEmptyEntriesSuccess() async throws {
        // Given
        let expectations = MockTestDynamoDBClientProtocol.Expectations()
        let writeEntries: [StandardWriteEntry<TestTypeA>] = []

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.bulkWrite(writeEntries)

        // Verify no calls are made for empty entries
        verify(mockClient, .never).executeTransaction(input: .any)
        verify(mockClient, .never).putItem(input: .any)
        verify(mockClient, .never).deleteItem(input: .any)
    }

    @Test("Bulk write handles mixed operation types")
    func bulkWriteHandlesMixedOperationTypes() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let updatedItem = StandardTypedDatabaseItem(
            compositePrimaryKey: testItemA.compositePrimaryKey,
            createDate: testItemA.createDate,
            rowStatus: RowStatus(rowVersion: testItemA.rowStatus.rowVersion + 1, lastUpdatedDate: Date()),
            rowValue: TestTypeA(firstly: "updated1", secondly: "updated2"),
            timeToLive: testItemA.timeToLive
        )

        let writeEntries: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: testItemA),
            .update(new: updatedItem, existing: testItemA),
            .deleteAtKey(key: testKey2),
            .deleteItem(existing: testItemA),
        ]

        let expectedOutput = DynamoDBModel.BatchExecuteStatementOutput()
        when(expectations.batchExecuteStatement(input: .any), return: expectedOutput)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.bulkWrite(writeEntries)

        // Verify transaction handles all operation types
        verify(mockClient).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 4
            }
        )
    }

    @Test("Bulk write with fallback handles failures in individual operations")
    func bulkWriteWithFallbackHandlesIndividualFailures() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let writeEntries = getWriteEntriesWithSomeLargeSortKeys()

        // Transaction fails, then individual operations have mixed results
        let error = DynamoDBClientError.resourceNotFound(message: "Item not found")

        when(expectations.batchExecuteStatement(input: .any), times: .unbounded, return: .init())
        when(expectations.putItem(input: .any), times: .unbounded, throw: error)
        when(expectations.deleteItem(input: .any), times: .unbounded, throw: error)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When/Then - Should handle the delete error appropriately
        do {
            try await table.bulkWriteWithFallback(writeEntries)

            Issue.record("Expected error not thrown")
        } catch let DynamoDBTableError.batchFailures(errors) {
            #expect(errors.count == 11)

            errors.forEach { error in
                if case .resourceNotFound = error {
                    // expected error
                } else {
                    Issue.record("Unexpected error")
                }
            }
        }

        // Verify transaction was tried and individual operations were called
        verify(mockClient, times: 3).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 25
            }
        )
        verify(mockClient).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 15
            }
        )
        verify(mockClient, times: 6).putItem(
            input: .matching { input in
                input.tableName == testTableName
            }
        )
        verify(mockClient, times: 5).deleteItem(
            input: .matching { input in
                input.tableName == testTableName
            }
        )
    }

    @Test("Bulk write with fallback handles partial failures in individual operations")
    func bulkWriteWithFallbackHandlesPartialFailures() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let writeEntries = getWriteEntriesWithSomeLargeSortKeys()

        // Transaction fails, then individual operations have mixed results
        let deleteError = DynamoDBClientError.resourceNotFound(message: "Item not found")

        let response = DynamoDBModel.BatchStatementResponse(
            error: .init(code: .resourcenotfound, message: "Item not found")
        )
        let batchOutput = DynamoDBModel.BatchExecuteStatementOutput(responses: [response])

        when(expectations.batchExecuteStatement(input: .any), times: .unbounded, return: batchOutput)
        when(expectations.putItem(input: .any), times: .unbounded, complete: .withSuccess)
        when(expectations.deleteItem(input: .any), times: .unbounded, throw: deleteError)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When/Then - Should handle the delete error appropriately
        do {
            try await table.bulkWriteWithFallback(writeEntries)

            Issue.record("Expected error not thrown")
        } catch let DynamoDBTableError.batchFailures(errors) {
            #expect(errors.count == 9)

            errors.forEach { error in
                if case .resourceNotFound = error {
                    // expected error
                } else {
                    Issue.record("Unexpected error")
                }
            }
        }

        // Verify transaction was tried and individual operations were called
        verify(mockClient, times: 3).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 25
            }
        )
        verify(mockClient).batchExecuteStatement(
            input: .matching { input in
                input.statements?.count == 15
            }
        )
        verify(mockClient, times: 6).putItem(
            input: .matching { input in
                input.tableName == testTableName
            }
        )
        verify(mockClient, times: 5).deleteItem(
            input: .matching { input in
                input.tableName == testTableName
            }
        )
    }
}
