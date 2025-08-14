// swift-tools-version:6.0

//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/Package.swift.
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

import CompilerPluginSupport
import PackageDescription

let swiftSettings: [SwiftSetting] = []

let package = Package(
    name: "dynamo-db-tables",
    platforms: [
        .macOS(.v12),
        .iOS(.v13),
        .tvOS(.v13),
        .watchOS(.v6),
    ],
    products: [
        .library(
            name: "DynamoDBTables",
            targets: ["DynamoDBTables"]),
    ],
    dependencies: [
        .package(url: "https://github.com/awslabs/aws-sdk-swift.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", "1.0.0" ..< "3.0.0"),
        .package(url: "https://github.com/JohnSundell/CollectionConcurrencyKit", from: "0.2.0"),
        .package(url: "https://github.com/apple/swift-syntax", from: "601.0.0"),
    ],
    targets: [
        .macro(name: "DynamoDBTablesMacros", dependencies: [
            .product(name: "SwiftSyntaxMacros", package: "swift-syntax"),
            .product(name: "SwiftCompilerPlugin", package: "swift-syntax"),
        ]),
        .target(
            name: "DynamoDBTables", dependencies: [
                .target(name: "DynamoDBTablesMacros"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Metrics", package: "swift-metrics"),
                .product(name: "AWSDynamoDB", package: "aws-sdk-swift"),
                .product(name: "CollectionConcurrencyKit", package: "CollectionConcurrencyKit"),
            ],
            swiftSettings: swiftSettings),
        .testTarget(
            name: "DynamoDBTablesTests", dependencies: [
                .target(name: "DynamoDBTables"),
                .product(name: "SwiftSyntaxMacrosTestSupport", package: "swift-syntax"),
            ],
            swiftSettings: swiftSettings),
    ])
