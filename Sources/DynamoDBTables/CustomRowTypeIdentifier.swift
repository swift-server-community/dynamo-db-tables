//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from https://github.com/amzn/smoke-dynamodb. Any commits
// prior to February 2024
// Copyright (c) 2021-2021 the DynamoDBTables authors
// Licensed under Apache License v2.0
//
// Subsequent commits
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
//  CustomRowTypeIdentifier.swift
//  DynamoDBTables
//

import Foundation

public protocol CustomRowTypeIdentifier {
    static var rowTypeIdentifier: String? { get }
}

func getTypeRowIdentifier(type: Any.Type) -> String {
    let typeRowIdentifier: String
    // if this type has a custom row identity
    if let customAttributesTypeType = type as? CustomRowTypeIdentifier.Type,
        let identifier = customAttributesTypeType.rowTypeIdentifier {
        typeRowIdentifier = identifier
    } else {
        typeRowIdentifier = String(describing: type)
    }
    
    return typeRowIdentifier
}
