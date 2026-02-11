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
//  AWSDynamoDBCompositePrimaryKeyTableTransactionTests.swift
//  DynamoDBTablesTests
//

import AWSDynamoDB
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
    ) -> GenericAWSDynamoDBCompositePrimaryKeyTable<MockTestDynamoDBClientProtocol> {
        return GenericAWSDynamoDBCompositePrimaryKeyTable(
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
            .deleteAtKey(key: testKey2)
        ]
        
        let expectedOutput = AWSDynamoDB.ExecuteTransactionOutput()
        when(expectations.executeTransaction(input: .any), return: expectedOutput)
        
        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)
        
        // When
        try await table.transactWrite(writeEntries)
        
        // Verify
        verify(mockClient).executeTransaction(input: .matching { input in
            input.transactStatements?.count == 3
        })
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
        
        let expectedOutput = AWSDynamoDB.ExecuteTransactionOutput()
        when(expectations.executeTransaction(input: .any), return: expectedOutput)
        
        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)
        
        // When
        try await table.transactWrite(writeEntries, constraints: constraints)
        
        // Verify
        verify(mockClient).executeTransaction(input: .matching { input in
            input.transactStatements?.count == 2 // 1 write + 1 constraint
        })
    }
    
    @Test("Transaction write fails with transaction conflict error")
    func transactWriteTransactionConflictError() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let writeEntries: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: testItemA),
            .deleteAtKey(key: testKey2)
        ]
        
        let transactionConflictError = AWSDynamoDB.TransactionConflictException(message: "Transaction conflict occurred")
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
        verify(mockClient, times: 6).executeTransaction(input: .matching { input in
            input.transactStatements?.count == 2
        })
    }
    
    @Test("Transaction write fails with transaction canceled error")
    func transactWriteTransactionCanceledError() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let writeEntries: [StandardWriteEntry<TestTypeA>] = [
            .insert(new: testItemA)
        ]
        
        let transactionCanceledError = AWSDynamoDB.TransactionCanceledException(
            cancellationReasons: [
                DynamoDBClientTypes.CancellationReason(code: "conditionalCheckFailed", message: "Condition failed")
            ], message: "Transaction was canceled"
        )
        when(expectations.executeTransaction(input: .any), throw: transactionCanceledError)
        
        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)
        
        // When/Then
        await #expect(throws: DynamoDBTableError.self) {
            try await table.transactWrite(writeEntries)
        }
        
        verify(mockClient).executeTransaction(input: .matching { input in
            input.transactStatements?.count == 1
        })
    }

    @Test("Polymorphic transaction write succeeds")
    func polymorphicTransactWriteSuccess() async throws {
        // Given
        var expectations = MockTestDynamoDBClientProtocol.Expectations()
        let writeEntries: [TestPolymorphicWriteEntry] = [
            .testTypeA(.insert(new: testItemA)),
            .testTypeB(.insert(new: testItemB))
        ]
        
        let expectedOutput = AWSDynamoDB.ExecuteTransactionOutput()
        when(expectations.executeTransaction(input: .any), return: expectedOutput)
        
        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)
        
        // When
        try await table.polymorphicTransactWrite(writeEntries)
        
        // Verify
        verify(mockClient).executeTransaction(input: .matching { input in
            input.transactStatements?.count == 2
        })
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
        
        let expectedOutput = AWSDynamoDB.ExecuteTransactionOutput()
        when(expectations.executeTransaction(input: .any), return: expectedOutput)
        
        let mockClient = MockTestDynamoDBClientProtocol(expectations: expectations)
        let table = createTable(with: mockClient)
        
        // When
        try await table.polymorphicTransactWrite(writeEntries, constraints: constraints)
        
        // Verify
        verify(mockClient).executeTransaction(input: .matching { input in
            input.transactStatements?.count == 2
        })
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
}