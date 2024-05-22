//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/String+DynamoDBKey.swift
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
//  String+DynamoDBKey.swift
//  DynamoDBTables
//

import Foundation

/// Extension for Arrays of Strings
public extension [String] {
    // Transforms the Array into a Dynamo key - putting dots between each element.
    var dynamodbKey: String {
        // return all elements joined with dots
        self.joined(separator: ".")
    }

    // Transforms an Array into a DynamoDB key prefix - a DynamoDB key with a dot on the end.
    var dynamodbKeyPrefix: String {
        let dynamodbKey = self.dynamodbKey
        if dynamodbKey.count == 0 {
            return ""
        }
        return dynamodbKey + "."
    }

    /**
     Returns the provided string with the DynamoDB key (with the trailing
     dot) corresponding to this array dropped as a prefix. Returns nil
     if the provided string doesn't have the prefix.
     */
    func dropAsDynamoDBKeyPrefix(from string: String) -> String? {
        let prefix = self.dynamodbKeyPrefix

        guard string.hasPrefix(prefix) else {
            return nil
        }

        return String(string.dropFirst(prefix.count))
    }

    /**
     Transforms the Array into a DynamoDB key - putting dots between each element - with a prefix
     element specifying the version.

     - Parameters:
        - versionNumber: The version number to prefix.
        - minimumFieldWidth: the minimum field width of the version field. Leading
        zeros will be padded if required.
     */
    func dynamodbKeyWithPrefixedVersion(_ versionNumber: Int, minimumFieldWidth: Int) -> String {
        let versionAsString = String(format: "%0\(minimumFieldWidth)d", versionNumber)
        return (["v\(versionAsString)"] + self).dynamodbKey
    }
}
