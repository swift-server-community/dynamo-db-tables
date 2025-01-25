// swiftlint:disable cyclomatic_complexity
//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/main/Sources/SmokeDynamoDB/InMemoryDynamoDBCompositePrimaryKeyTableStore.swift
// Copyright 2018-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// Licensed under Apache License v2.0
//
// Changes specified by
// https://github.com/swift-server-community/dynamo-db-tables/compare/9ab0e7a..main
// Copyright (c) 2024 the DynamoDBTables authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of DynamoDBTables authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

//
//  InMemoryDynamoDBCompositePrimaryKeyTableStore.swift
//  DynamoDBTables
//

@preconcurrency import AWSDynamoDB
import Foundation

// MARK: - Store implementation

actor InMemoryDynamoDBCompositePrimaryKeyTableStore {
    typealias StoreType = [String: [String: InMemoryDatabaseItem]]

    var store: StoreType = [:]

    init() {}

    func execute<Result>(_ operation: (inout StoreType) throws -> Result) rethrows -> Result {
        try operation(&self.store)
    }
    /*
     nonisolated func validateEntry(entry: WriteEntry<some Any, some Any, some Any>) throws {
         let entryString = "\(entry)"
         if entryString.count > AWSDynamoDBLimits.maxStatementLength {
             throw DynamoDBTableError.statementLengthExceeded(
                 reason: "failed to satisfy constraint: Member must have length less than or equal to "
                     + "\(AWSDynamoDBLimits.maxStatementLength). Actual length \(entryString.count)")
         }
     }

     func polymorphicBulkWrite(
         _ entries: [some PolymorphicWriteEntry], constraints: [some PolymorphicTransactionConstraintEntry],
         isTransaction: Bool) throws
     {
         let entryCount = entries.count + constraints.count
         let context = StandardPolymorphicWriteEntryContext<InMemoryPolymorphicWriteEntryTransform,
             InMemoryPolymorphicTransactionConstraintTransform>(table: self)

         if isTransaction, entryCount > AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement {
             throw DynamoDBTableError.transactionSizeExceeded(attemptedSize: entryCount,
                                                              maximumSize: AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement)
         }

         let store = self.store

         if let error = self.handleConstraints(constraints: constraints, isTransaction: isTransaction, context: context) {
             throw error
         }

         if let error = self.handlePolymorphicEntries(entries: entries, isTransaction: isTransaction, context: context) {
             if isTransaction {
                 // restore the state prior to the transaction
                 self.store = store
             }

             throw error
         }
     }

     func deleteItems(forKeys keys: [CompositePrimaryKey<some Any>]) throws {
         try keys.forEach { key in
             try self.deleteItem(forKey: key)
         }
     }

     func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType]) throws {
         try existingItems.forEach { (existingItem: ItemType) in
             try self.deleteItem(existingItem: existingItem)
         }
     }*/
}
