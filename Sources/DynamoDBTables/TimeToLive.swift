//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/TimeToLive.swift
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
//  TimeToLive.swift
//  DynamoDBTables
//

public protocol TimeToLiveAttributes {
    static var timeToLiveAttributeName: String { get }
}

public struct StandardTimeToLiveAttributes: TimeToLiveAttributes {
    public static var timeToLiveAttributeName: String {
        "ExpireDate"
    }
}

public typealias StandardTimeToLive = TimeToLive<StandardTimeToLiveAttributes>

public struct TimeToLive<AttributesType: TimeToLiveAttributes>: Sendable, Codable, CustomStringConvertible, Hashable {
    public var description: String {
        "TimeToLive(timeToLiveTimestamp: \(self.timeToLiveTimestamp)"
    }

    public let timeToLiveTimestamp: Int64

    public init(timeToLiveTimestamp: Int64) {
        self.timeToLiveTimestamp = timeToLiveTimestamp
    }

    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: DynamoDBAttributesTypeCodingKey.self)
        self.timeToLiveTimestamp = try values.decode(
            Int64.self,
            forKey: DynamoDBAttributesTypeCodingKey(stringValue: AttributesType.timeToLiveAttributeName)!
        )
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: DynamoDBAttributesTypeCodingKey.self)
        try container.encode(
            self.timeToLiveTimestamp,
            forKey: DynamoDBAttributesTypeCodingKey(stringValue: AttributesType.timeToLiveAttributeName)!
        )
    }
}
