//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/RowWithItemVersionProtocol.swift
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
//  RowWithItemVersionProtocol.swift
//  DynamoDBTables
//


/// Protocol for a item payload wrapper that declares an item version.
/// Primarily required to allow the constrained extension below.
public protocol RowWithItemVersionProtocol {
    associatedtype RowType: Codable

    /// The item version number
    var itemVersion: Int { get }
    /// The item payload
    var rowValue: RowType { get }

    /// Function that accepts a version and an updated row version and returns
    /// an instance of the implementing type
    func createUpdatedItem(
        withVersion itemVersion: Int?,
        withValue newRowValue: RowType
    ) -> Self

    /// Function that accepts an updated row version and returns
    /// an instance of the implementing type
    func createUpdatedItem(withValue newRowValue: RowType) -> Self
}

extension RowWithItemVersionProtocol {
    /// Default implementation that delegates to createUpdatedItem(withVersion:withValue:)
    public func createUpdatedItem(withValue newRowValue: RowType) -> Self {
        self.createUpdatedItem(withVersion: nil, withValue: newRowValue)
    }
}

/// Declare conformance of RowWithItemVersion to RowWithItemVersionProtocol
extension RowWithItemVersion: RowWithItemVersionProtocol {}
