//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/PolymorphicWriteEntry.swift
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
//  PolymorphicWriteEntry.swift
//  DynamoDBTables
//

import AWSDynamoDB

// Conforming types are provided by the Table implementation to convert a `WriteEntry` into
// something the table can use to perform the write.
public protocol PolymorphicWriteEntryTransform {
    associatedtype TableType

    init<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: WriteEntry<AttributesType, ItemType>, table: TableType) throws
}

// Conforming types are provided by the Table implementation to convert a `WriteEntry` into
// something the table can use to achieve the constraint.
public protocol PolymorphicTransactionConstraintTransform {
    associatedtype TableType

    init<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: TransactionConstraintEntry<AttributesType, ItemType>, table: TableType) throws
}

// Conforming types are provided by the application to express the different possible write entries
// and how they can be converted to the table-provided transform type.
public protocol PolymorphicWriteEntry {
    func handle<Context: PolymorphicWriteEntryContext>(context: Context) throws -> Context.WriteEntryTransformType

    var compositePrimaryKey: StandardCompositePrimaryKey? { get }
}

public extension PolymorphicWriteEntry {
    var compositePrimaryKey: StandardCompositePrimaryKey? {
        nil
    }
}

public typealias StandardTransactionConstraintEntry<ItemType: Codable> = TransactionConstraintEntry<StandardPrimaryKeyAttributes, ItemType>

public enum TransactionConstraintEntry<AttributesType: PrimaryKeyAttributes, ItemType: Sendable & Codable>: Sendable {
    case required(existing: TypedDatabaseItem<AttributesType, ItemType>)

    public var compositePrimaryKey: CompositePrimaryKey<AttributesType> {
        switch self {
        case let .required(existing: existing):
            return existing.compositePrimaryKey
        }
    }
}

// Conforming types are provided by the application to express the different possible constraint entries
// and how they can be converted to the table-provided transform type.
public protocol PolymorphicTransactionConstraintEntry: Sendable {
    func handle<Context: PolymorphicWriteEntryContext>(context: Context) throws -> Context.WriteTransactionConstraintType

    var compositePrimaryKey: StandardCompositePrimaryKey? { get }
}

public extension PolymorphicTransactionConstraintEntry {
    var compositePrimaryKey: StandardCompositePrimaryKey? {
        nil
    }
}

public struct EmptyPolymorphicTransactionConstraintEntry: PolymorphicTransactionConstraintEntry {
    public func handle<Context: PolymorphicWriteEntryContext>(context _: Context) throws -> Context.WriteTransactionConstraintType {
        fatalError("There are no items to transform")
    }
}

// Helper Context type that enables transforming Write Entries into the table-provided transform type.
public protocol PolymorphicWriteEntryContext {
    associatedtype WriteEntryTransformType: PolymorphicWriteEntryTransform
    associatedtype WriteTransactionConstraintType: PolymorphicTransactionConstraintTransform

    func transform<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: WriteEntry<AttributesType, ItemType>) throws
        -> WriteEntryTransformType

    func transform<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: TransactionConstraintEntry<AttributesType, ItemType>) throws
        -> WriteTransactionConstraintType
}

public struct StandardPolymorphicWriteEntryContext<WriteEntryTransformType: PolymorphicWriteEntryTransform,
    WriteTransactionConstraintType: PolymorphicTransactionConstraintTransform>: PolymorphicWriteEntryContext
    where WriteEntryTransformType.TableType == WriteTransactionConstraintType.TableType
{
    public typealias TableType = WriteEntryTransformType.TableType

    private let table: TableType

    public init(table: TableType) {
        self.table = table
    }

    public func transform(_ entry: WriteEntry<some PrimaryKeyAttributes, some Codable>) throws
        -> WriteEntryTransformType
    {
        try .init(entry, table: self.table)
    }

    public func transform(_ entry: TransactionConstraintEntry<some PrimaryKeyAttributes, some Codable>) throws
        -> WriteTransactionConstraintType
    {
        try .init(entry, table: self.table)
    }
}
