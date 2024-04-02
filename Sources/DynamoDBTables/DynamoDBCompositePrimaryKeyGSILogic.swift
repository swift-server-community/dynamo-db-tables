//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/DynamoDBCompositePrimaryKeyGSILogic.swift
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
//  DynamoDBCompositePrimaryKeyGSILogic.swift
//  DynamoDBTables
//

import Foundation
import AWSDynamoDB

// Provide a default `PolymorphicWriteEntry` for the `DynamoDBCompositePrimaryKeyGSILogic` for backwards compatibility
public struct NoOpPolymorphicWriteEntry: PolymorphicWriteEntry {
    public func handle<Context>(context: Context) throws -> Context.WriteEntryTransformType where Context : PolymorphicWriteEntryContext {
        fatalError("Unimplemented")
    }
}

/**
  A protocol that simulates the logic of a GSI reacting to events on the main table.
 */
public protocol DynamoDBCompositePrimaryKeyGSILogic {
    associatedtype GSIAttributesType: PrimaryKeyAttributes
    associatedtype WriteEntryType: PolymorphicWriteEntry = NoOpPolymorphicWriteEntry

    /**
     * Called when an item is inserted on the main table. Can be used to transform the provided item to the item that would be made available on the GSI.
     */
    func onInsertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) async throws

    /**
     * Called when an item is clobbered on the main table. Can be used to transform the provided item to the item that would be made available on the GSI.
     */
    func onClobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                 gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) async throws

    /**
     * Called when an item is updated on the main table. Can be used to transform the provided item to the item that would be made available on the GSI.
     */
    func onUpdateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) async throws
 
    /**
     * Called when an item is delete on the main table. Can be used to also delete the corresponding item on the GSI.

     */
    func onDeleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>,
                                      gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) async throws

    /**
     * Called when an transact write in the main table. Can be used to also transact write the corresponding item on the GSI.

     */
    func onTransactWrite(_ entries: [WriteEntryType],
                         gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) async throws
}
