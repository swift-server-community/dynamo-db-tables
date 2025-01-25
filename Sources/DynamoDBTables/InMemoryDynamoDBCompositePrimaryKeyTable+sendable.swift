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
//  InMemoryDynamoDBCompositePrimaryKeyTableStore+sendable.swift
//  DynamoDBTables
//

// MARK: - Sendable InMemory Storage Helpers

@preconcurrency import AWSDynamoDB
import Foundation

public struct DatabaseItemMetadata: Sendable {
    public var createDate: Date
    public var rowStatus: RowStatus
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

public struct InMemoryDatabaseItem: Sendable {
    public var item: [Swift.String: DynamoDBClientTypes.AttributeValue]
    var metadata: DatabaseItemMetadata

    var createDate: Date {
        self.metadata.createDate
    }

    var rowStatus: RowStatus {
        self.metadata.rowStatus
    }

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

extension TypedTTLDatabaseItem {
    func inMemoryForm() throws
        -> InMemoryDatabaseItem
    {
        let attributes = try getAttributes(forItem: self)
        return .init(item: attributes, metadata: self.asMetadata())
    }
}

struct InMemoryDatabaseItemWithKey<AttributesType: PrimaryKeyAttributes>: Sendable {
    var compositePrimaryKey: CompositePrimaryKey<AttributesType>
    var inMemoryDatabaseItem: InMemoryDatabaseItem

    var createDate: Date {
        self.inMemoryDatabaseItem.metadata.createDate
    }

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
        try InMemoryDatabaseItemWithKey(compositePrimaryKey: self.compositePrimaryKey,
                                        inMemoryDatabaseItem: self.inMemoryForm())
    }
}

enum InMemoryWriteEntry<AttributesType: PrimaryKeyAttributes>: Sendable {
    case update(new: InMemoryDatabaseItemWithKey<AttributesType>, existing: InMemoryDatabaseItemWithKey<AttributesType>)
    case insert(new: InMemoryDatabaseItemWithKey<AttributesType>)
    case deleteAtKey(key: CompositePrimaryKey<AttributesType>)
    case deleteItem(existing: InMemoryDatabaseItemWithKey<AttributesType>)

    var compositePrimaryKey: CompositePrimaryKey<AttributesType> {
        switch self {
        case .update(new: let new, existing: _):
            return new.compositePrimaryKey
        case let .insert(new: new):
            return new.compositePrimaryKey
        case let .deleteAtKey(key: key):
            return key
        case let .deleteItem(existing: existing):
            return existing.compositePrimaryKey
        }
    }
}

extension WriteEntry {
    func inMemoryForm() throws -> InMemoryWriteEntry<AttributesType> {
        return switch self {
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

    var compositePrimaryKey: CompositePrimaryKey<AttributesType> {
        switch self {
        case let .required(existing: existing):
            return existing.compositePrimaryKey
        }
    }
}

extension TransactionConstraintEntry {
    func inMemoryForm() throws -> InMemoryTransactionConstraintEntry<AttributesType> {
        return switch self {
        case let .required(existing: existing):
            try .required(existing: existing.inMemoryFormWithKey())
        }
    }
}
