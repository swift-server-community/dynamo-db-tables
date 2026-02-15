//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// Copyright (c) 2026 the DynamoDBTables authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of DynamoDBTables authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

//
//  GenericDynamoDBCompositePrimaryKeyTableTransactionTests.swift
//  DynamoDBTablesTests
//

import Foundation
import Logging
import Smockable
import Testing

@testable import DynamoDBTables

@Suite("AWSDynamoDBCompositePrimaryKeyTable Transaction Tests")
struct AWSDynamoDBCompositePrimaryKeyTableTransactionTests {

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

    // MARK: - Transaction Write Tests

    @Test("Transaction write with mixed operations succeeds")
    func transactWriteMixedOperationsSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let writeEntries: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: testItemA),
            .update(new: testItemA, existing: testItemA),
            .deleteAtKey(key: testKey2),
        ]

        when(expectations.executeTransaction(input: .any), complete: .withSuccess)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.transactWrite(writeEntries)

        // Verify
        verify(mockClient).executeTransaction(
            input: .matching { input in
                input.transactStatements?.count == 3
            }
        )
    }

    @Test("Transaction write with constraints succeeds")
    func transactWriteWithConstraintsSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let writeEntries: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: testItemA)
        ]
        let constraints: [StandardTransactionConstraintEntry<TestTypeA>] = [
            .required(existing: testItemA)
        ]

        when(expectations.executeTransaction(input: .any), complete: .withSuccess)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.transactWrite(writeEntries, constraints: constraints)

        // Verify
        verify(mockClient).executeTransaction(
            input: .matching { input in
                input.transactStatements?.count == 2  // 1 write + 1 constraint
            }
        )
    }

    @Test("Transaction write fails with transaction conflict error")
    func transactWriteTransactionConflictError() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let writeEntries: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: testItemA),
            .deleteAtKey(key: testKey2),
        ]

        let transactionConflictError = DynamoDBClientError.transactionConflict(
            message: "Transaction conflict occurred"
        )
        when(expectations.executeTransaction(input: .any), times: .unbounded, throw: transactionConflictError)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When/Then
        do {
            try await table.transactWrite(writeEntries)
        } catch let error as DynamoDBTableError {
            if case .transactionCanceled(let reasons) = error, case .transactionConflict(let message) = reasons.first {
                #expect(message == "Transaction conflict occurred")
            } else {
                Issue.record("Expected DynamoDBTableError.transactionConflict, got \(error)")
            }
        }

        // first attempt + 5 retries
        verify(mockClient, times: 6).executeTransaction(
            input: .matching { input in
                input.transactStatements?.count == 2
            }
        )
    }

    @Test("Transaction write fails with transaction canceled error")
    func transactWriteTransactionCanceledError() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let writeEntries: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: testItemA)
        ]

        let transactionCanceledError = DynamoDBClientError.transactionCanceled(
            reasons: [
                DynamoDBModel.CancellationReason(code: "conditionalCheckFailed", message: "Condition failed")
            ],
            message: "Transaction was canceled"
        )
        when(expectations.executeTransaction(input: .any), throw: transactionCanceledError)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When/Then
        await #expect(throws: DynamoDBTableError.self) {
            try await table.transactWrite(writeEntries)
        }

        verify(mockClient).executeTransaction(
            input: .matching { input in
                input.transactStatements?.count == 1
            }
        )
    }

    @Test("Polymorphic transaction write succeeds")
    func polymorphicTransactWriteSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let writeEntries: [TestPolymorphicWriteEntry] = [
            .testTypeA(.insert(new: testItemA)),
            .testTypeB(.insert(new: testItemB)),
        ]

        when(expectations.executeTransaction(input: .any), complete: .withSuccess)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.polymorphicTransactWrite(writeEntries)

        // Verify
        verify(mockClient).executeTransaction(
            input: .matching { input in
                input.transactStatements?.count == 2
            }
        )
    }

    @Test("Polymorphic transaction write with constraints succeeds")
    func polymorphicTransactWriteWithConstraintsSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let writeEntries: [TestPolymorphicWriteEntry] = [
            .testTypeA(.insert(new: testItemA))
        ]
        let constraints: [TestPolymorphicTransactionConstraintEntry] = [
            .testTypeA(.required(existing: testItemA))
        ]

        when(expectations.executeTransaction(input: .any), complete: .withSuccess)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When
        try await table.polymorphicTransactWrite(writeEntries, constraints: constraints)

        // Verify
        verify(mockClient).executeTransaction(
            input: .matching { input in
                input.transactStatements?.count == 2
            }
        )
    }

    @Test("Transaction write with too many entries fails")
    func transactWriteTooManyEntriesError() async throws {
        // Given
        // Create more than 100 entries to exceed transaction limit
        let writeEntries: [StandardWriteEntry<TestTypeA>] = (0..<101).map { index in
            let key = CompositePrimaryKey<StandardPrimaryKeyAttributes>(
                partitionKey: "partition\(index)",
                sortKey: "sort\(index)"
            )
            return .deleteAtKey(key: key)
        }

        let mockClient = MockTestDynamoDBClientProtocol(expectations: .init())
        let table = createTable(with: mockClient)

        // When/Then
        do {
            try await table.transactWrite(writeEntries)
        } catch let error as DynamoDBTableError {
            if case .itemCollectionSizeLimitExceeded(let attemptedSize, let maximumSize) = error {
                #expect(attemptedSize == writeEntries.count)
                #expect(maximumSize == 100)
            } else {
                Issue.record("Expected DynamoDBTableError.itemCollectionSizeLimitExceeded, got \(error)")
            }
        }

        // Verify
        verifyNoInteractions(mockClient)
    }

    @Test("Transaction write with empty entries succeeds")
    func transactWriteEmptyEntriesSuccess() async throws {
        // Given
        let writeEntries: [StandardWriteEntry<TestTypeA>] = []

        let mockClient = MockTestDynamoDBClientProtocol(expectations: .init())
        let table = createTable(with: mockClient)

        // When
        try await table.transactWrite(writeEntries)

        // Verify
        verifyNoInteractions(mockClient)
    }

    // MARK: - Conditional Transaction Write Retry Tests

    private let constraintKey = CompositePrimaryKey<StandardPrimaryKeyAttributes>(
        partitionKey: "constraint_pk",
        sortKey: "constraint_sk"
    )

    private var constraintItem: StandardTypedDatabaseItem<TestTypeA> {
        StandardTypedDatabaseItem.newItem(
            withKey: constraintKey,
            andValue: TestTypeA(firstly: "c1", secondly: "c2")
        )
    }

    @Test("Constraint failure short-circuits retry in transactWrite(forKeys:)")
    func constraintFailureShortCircuitsRetry() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let batchGetOutput = DynamoDBModel.BatchGetItemOutput(
            responses: [testTableName: [itemAAttributes]]
        )
        when(expectations.batchGetItem(input: .any), return: batchGetOutput)

        // ConditionalCheckFailed at the constraint's position (index 1), None at write entry (index 0)
        let canceledError = DynamoDBClientError.transactionCanceled(
            reasons: [
                DynamoDBModel.CancellationReason(code: "None"),
                DynamoDBModel.CancellationReason(
                    code: "ConditionalCheckFailed",
                    message: "Constraint not met"
                ),
            ],
            message: "Transaction canceled"
        )
        when(expectations.executeTransaction(input: .any), throw: canceledError)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When/Then
        do {
            try await table.retryingTransactWrite(
                forKeys: [testKey1],
                withRetries: 5,
                constraints: [.required(existing: constraintItem)]
            ) { key, _ -> StandardWriteEntry<TestTypeA>? in
                .insert(
                    new: StandardTypedDatabaseItem.newItem(
                        withKey: key,
                        andValue: TestTypeA(firstly: "t1", secondly: "t2")
                    )
                )
            }
            Issue.record("Expected constraintFailure error")
        } catch let error as DynamoDBTableError {
            guard case .constraintFailure = error else {
                Issue.record("Expected constraintFailure, got \(error)")
                return
            }
        }

        // Short-circuited: executeTransaction called only once
        verify(mockClient, times: 1).executeTransaction(input: .any)
        verify(mockClient, times: 1).batchGetItem(input: .any)
    }

    @Test("Write entry failure retries normally in transactWrite(forKeys:)")
    func writeEntryFailureRetriesNormally() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let batchGetOutput = DynamoDBModel.BatchGetItemOutput(
            responses: [testTableName: [itemAAttributes]]
        )
        when(expectations.batchGetItem(input: .any), times: .unbounded, return: batchGetOutput)

        // ConditionalCheckFailed at the write entry's position (index 0), None at constraint (index 1)
        let canceledError = DynamoDBClientError.transactionCanceled(
            reasons: [
                DynamoDBModel.CancellationReason(
                    code: "ConditionalCheckFailed",
                    message: "Write conflict"
                ),
                DynamoDBModel.CancellationReason(code: "None"),
            ],
            message: "Transaction canceled"
        )
        when(expectations.executeTransaction(input: .any), times: .unbounded, throw: canceledError)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When/Then - retries exhausted, should get concurrencyError
        do {
            try await table.retryingTransactWrite(
                forKeys: [testKey1],
                withRetries: 2,
                constraints: [.required(existing: constraintItem)]
            ) { key, _ -> StandardWriteEntry<TestTypeA>? in
                .insert(
                    new: StandardTypedDatabaseItem.newItem(
                        withKey: key,
                        andValue: TestTypeA(firstly: "t1", secondly: "t2")
                    )
                )
            }
            Issue.record("Expected concurrencyError")
        } catch let error as DynamoDBTableError {
            guard case .concurrencyError = error else {
                Issue.record("Expected concurrencyError, got \(error)")
                return
            }
        }

        // Retried: executeTransaction called twice (retries=2 → retries=1 → retries=0 throws)
        verify(mockClient, times: 2).executeTransaction(input: .any)
        verify(mockClient, times: 2).batchGetItem(input: .any)
    }

    @Test("No constraints retries as before in transactWrite(forKeys:)")
    func noConstraintsRetriesAsBefore() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()

        let itemAAttributes = try getAttributes(forItem: testItemA)
        let batchGetOutput = DynamoDBModel.BatchGetItemOutput(
            responses: [testTableName: [itemAAttributes]]
        )
        when(expectations.batchGetItem(input: .any), times: .unbounded, return: batchGetOutput)

        // ConditionalCheckFailed at the write entry's position
        let canceledError = DynamoDBClientError.transactionCanceled(
            reasons: [
                DynamoDBModel.CancellationReason(
                    code: "ConditionalCheckFailed",
                    message: "Write conflict"
                )
            ],
            message: "Transaction canceled"
        )
        when(expectations.executeTransaction(input: .any), times: .unbounded, throw: canceledError)

        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)

        // When/Then - no constraints, should retry until exhaustion
        do {
            try await table.retryingTransactWrite(
                forKeys: [testKey1],
                withRetries: 2
            ) { key, _ -> StandardWriteEntry<TestTypeA>? in
                .insert(
                    new: StandardTypedDatabaseItem.newItem(
                        withKey: key,
                        andValue: TestTypeA(firstly: "t1", secondly: "t2")
                    )
                )
            }
            Issue.record("Expected concurrencyError")
        } catch let error as DynamoDBTableError {
            guard case .concurrencyError = error else {
                Issue.record("Expected concurrencyError, got \(error)")
                return
            }
        }

        // Retried until exhaustion
        verify(mockClient, times: 2).executeTransaction(input: .any)
        verify(mockClient, times: 2).batchGetItem(input: .any)
    }
}
