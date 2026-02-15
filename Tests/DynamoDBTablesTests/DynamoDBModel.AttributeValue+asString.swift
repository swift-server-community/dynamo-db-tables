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
//  DynamoDBModel.AttributeValue+asString.swift
//  DynamoDBTablesTests
//

@testable import DynamoDBTables

extension DynamoDBModel.AttributeValue {
    var asString: String? {
        if case .s(let string) = self {
            return string
        }

        return nil
    }
}
