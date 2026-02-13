//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
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
//  InMemoryDataRepresentations.swift
//  DynamoDBTables
//

// MARK: - InMemory Data Representations

@preconcurrency import AWSDynamoDB
import Foundation

/**
 In-memory Sendable representations of database rows and their metadata, allowing rows to be stored `InMemoryDynamoDBCompositePrimaryKeyTableStore`
 without storing the potentially non-Sendable row types.
 */

/// Representation of a `DatabaseItem` when stored in `InMemoryDynamoDBCompositePrimaryKeyTableStore`.
/// This type stores the serialised `DynamoDBClientTypes.AttributeValue` map
public struct InMemoryDatabaseItem: Sendable {
    public var item: [Swift.String: DynamoDBClientTypes.AttributeValue]
    var metadata: DatabaseItemMetadata

    var createDate: Date {
        self.metadata.createDate
    }

    var rowStatus: RowStatus {
        self.metadata.rowStatus
    }

    /**
     De-serialises the `DynamoDBClientTypes.AttributeValue` map into an appropriate `TypedTTLDatabaseItem`.
     */
    public func getItem<AttributesType, ItemType, TimeToLiveAttributesType>() throws
        -> TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>?
    {
        do {
            return try DynamoDBDecoder().decode(DynamoDBClientTypes.AttributeValue.m(self.item))
        } catch {
            throw error.asUnrecognizedDynamoDBTableError()
        }
    }
}

public struct TypeErasedCompositePrimaryKey: Sendable {
    public let partitionKey: String
    public let sortKey: String

    public init(partitionKey: String, sortKey: String) {
        self.partitionKey = partitionKey
        self.sortKey = sortKey
    }
}

// MARK: - Internal Storage Helpers

extension TypedTTLDatabaseItem {
    func inMemoryForm() throws
        -> InMemoryDatabaseItem
    {
        let attributes = try getAttributes(forItem: self)
        return .init(item: attributes, metadata: self.asMetadata())
    }
}

struct DatabaseItemMetadata: Sendable {
    var createDate: Date
    var rowStatus: RowStatus
}

struct DatabaseItemMetadataWithKey<AttributesType: PrimaryKeyAttributes>: Sendable {
    var compositePrimaryKey: CompositePrimaryKey<AttributesType>
    var metadata: DatabaseItemMetadata

    var createDate: Date {
        self.metadata.createDate
    }

    var rowStatus: RowStatus {
        self.metadata.rowStatus
    }
}

extension TypedTTLDatabaseItem {
    func asMetadata()
        -> DatabaseItemMetadata
    {
        .init(createDate: self.createDate, rowStatus: self.rowStatus)
    }

    func asMetadataWithKey()
        -> DatabaseItemMetadataWithKey<AttributesType>
    {
        .init(compositePrimaryKey: self.compositePrimaryKey, metadata: self.asMetadata())
    }
}

struct InMemoryDatabaseItemWithKey<AttributesType: PrimaryKeyAttributes>: Sendable {
    var compositePrimaryKey: CompositePrimaryKey<AttributesType>
    var inMemoryDatabaseItem: InMemoryDatabaseItem

    var rowStatus: RowStatus {
        self.inMemoryDatabaseItem.metadata.rowStatus
    }

    func asMetadataWithKey()
        -> DatabaseItemMetadataWithKey<AttributesType>
    {
        .init(compositePrimaryKey: self.compositePrimaryKey, metadata: self.inMemoryDatabaseItem.metadata)
    }
}

extension TypedTTLDatabaseItem {
    func inMemoryFormWithKey() throws
        -> InMemoryDatabaseItemWithKey<AttributesType>
    {
        try InMemoryDatabaseItemWithKey(
            compositePrimaryKey: self.compositePrimaryKey,
            inMemoryDatabaseItem: self.inMemoryForm()
        )
    }
}

enum InMemoryWriteEntry<AttributesType: PrimaryKeyAttributes>: Sendable {
    case update(new: InMemoryDatabaseItemWithKey<AttributesType>, existing: InMemoryDatabaseItemWithKey<AttributesType>)
    case insert(new: InMemoryDatabaseItemWithKey<AttributesType>)
    case deleteAtKey(key: CompositePrimaryKey<AttributesType>)
    case deleteItem(existing: InMemoryDatabaseItemWithKey<AttributesType>)
}

extension WriteEntry {
    func inMemoryForm() throws -> InMemoryWriteEntry<AttributesType> {
        switch self {
        case let .update(new: new, existing: existing):
            try .update(new: new.inMemoryFormWithKey(), existing: existing.inMemoryFormWithKey())
        case let .insert(new: new):
            try .insert(new: new.inMemoryFormWithKey())
        case let .deleteAtKey(key: key):
            .deleteAtKey(key: key)
        case let .deleteItem(existing: existing):
            try .deleteItem(existing: existing.inMemoryFormWithKey())
        }
    }
}

enum InMemoryTransactionConstraintEntry<AttributesType: PrimaryKeyAttributes>: Sendable {
    case required(existing: InMemoryDatabaseItemWithKey<AttributesType>)
}

extension TransactionConstraintEntry {
    func inMemoryForm() throws -> InMemoryTransactionConstraintEntry<AttributesType> {
        switch self {
        case let .required(existing: existing):
            try .required(existing: existing.inMemoryFormWithKey())
        }
    }
}
