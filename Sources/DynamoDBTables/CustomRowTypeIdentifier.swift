//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/CustomRowTypeIdentifier.swift
// Copyright 2018-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// Licensed under Apache License v2.0
//
// Changes specified by
// https://github.com/swift-server-community/dynamo-db-tables/compare/6fec4c8..main
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
