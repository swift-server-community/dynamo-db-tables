// swift-tools-version:6.3

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
// Copyright (c) 2026 the DynamoDBTables authors
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
        .macOS(.v15), .iOS(.v18), .watchOS(.v11), .tvOS(.v18),
    ],
    products: [
        .library(
            name: "DynamoDBTables",
            targets: ["DynamoDBTables"]
        ),
        .library(
            name: "DynamoDBTablesAWS",
            targets: ["DynamoDBTablesAWS"]
        ),
        .library(
            name: "DynamoDBTablesSoto",
            targets: ["DynamoDBTablesSoto"]
        ),
    ],
    // No default backend. A consumer names the SDK it wants — `traits: ["AWSSDK"]` or
    // `traits: ["SOTOSDK"]` — and gets exactly that one.
    //
    // This used to be `.default(enabledTraits: ["AWSSDK"])`, which made aws-sdk-swift the implicit
    // choice. Two reasons it is gone, one a matter of taste and one a bug.
    //
    // The taste: these two traits are alternatives, not options. A package that ships two mutually
    // exclusive backends has no defensible default — whichever it picks, half its consumers are
    // opted into a dependency they will not use, and the manifest reads as though one backend is
    // the real one. Making the choice explicit costs a consumer four words and removes the question.
    //
    // The bug is the reason it is worth a breaking change before 1.0. SwiftPM resets a dependency's
    // enabled traits to its *declared defaults* partway through PubGrub resolution rather than
    // propagating the set the root package configured (swiftlang/swift-package-manager#9269, whose
    // fix does not cover this path — reproduced on 6.4 snapshots from 2026-07-06 and 2026-08-01).
    // With `AWSSDK` as the default, a SOTO-only consumer's resolution therefore comes to believe
    // aws-sdk-swift is required. Nothing constrains it — no target under the consumer's real trait
    // set depends on it — so the resolver ranges over its ~500 tags and gives up:
    //
    //     error: exhausted attempts to resolve the dependencies graph, with the following
    //     dependencies unresolved: * 'aws-sdk-swift' from https://github.com/awslabs/aws-sdk-swift.git
    //
    // It bites any constrained re-solve — `swift package update`, even of one unrelated package —
    // while a clean resolve succeeds, which is what makes it look intermittent. An empty default set
    // resets to *nothing*, so no backend is ever spuriously required and the failure cannot occur in
    // either direction. Verified by flipping this one line: the same `swift package update` that
    // exhausted after 65s completes in 21s.
    traits: [
        .default(enabledTraits: []),
        .trait(name: "AWSSDK"),
        .trait(name: "SOTOSDK"),
    ],
    dependencies: [
        .package(url: "https://github.com/awslabs/aws-sdk-swift.git", from: "1.7.0"),
        .package(url: "https://github.com/soto-project/soto.git", from: "7.15.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", "1.0.0"..<"3.0.0"),
        .package(url: "https://github.com/apple/swift-configuration.git", from: "1.0.0"),
        .package(url: "https://github.com/swiftlang/swift-syntax", "603.0.0"..<"604.0.0"),
        .package(url: "https://github.com/tachyonics/smockable", from: "1.0.0-rc.5"),
        .package(url: "https://github.com/tachyonics/swift-local-containers", from: "0.9.0"),
    ],
    targets: [
        .macro(
            name: "DynamoDBTablesMacros",
            dependencies: [
                .product(name: "SwiftSyntaxMacros", package: "swift-syntax"),
                .product(name: "SwiftCompilerPlugin", package: "swift-syntax"),
            ]
        ),
        .target(
            name: "DynamoDBTables",
            dependencies: [
                .target(name: "DynamoDBTablesMacros"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Metrics", package: "swift-metrics"),
                .product(name: "Configuration", package: "swift-configuration"),
            ],
            swiftSettings: swiftSettings
        ),
        .target(
            name: "DynamoDBTablesAWS",
            dependencies: [
                .target(name: "DynamoDBTables"),
                .product(name: "AWSDynamoDB", package: "aws-sdk-swift", condition: .when(traits: ["AWSSDK"])),
            ],
            swiftSettings: swiftSettings
        ),
        .target(
            name: "DynamoDBTablesSoto",
            dependencies: [
                .target(name: "DynamoDBTables"),
                .product(name: "SotoDynamoDB", package: "soto", condition: .when(traits: ["SOTOSDK"])),
            ],
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "DynamoDBTablesTests",
            dependencies: [
                .target(name: "DynamoDBTables"),
                .product(name: "SwiftSyntaxMacrosTestSupport", package: "swift-syntax"),
                .product(name: "Smockable", package: "smockable"),
            ],
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "DynamoDBTablesMacrosTests",
            dependencies: [
                .target(name: "DynamoDBTablesMacros"),
                .product(name: "SwiftSyntaxMacrosTestSupport", package: "swift-syntax"),
            ],
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "IntegrationTests",
            dependencies: [
                .target(name: "DynamoDBTables"),
                .target(name: "DynamoDBTablesAWS", condition: .when(traits: ["AWSSDK"])),
                .target(name: "DynamoDBTablesSoto", condition: .when(traits: ["SOTOSDK"])),
                .product(name: "ContainerMacrosLib", package: "swift-local-containers"),
                .product(name: "ContainerTestSupport", package: "swift-local-containers"),
            ],
            swiftSettings: swiftSettings,
            plugins: [.plugin(name: "ContainerCodeGen", package: "swift-local-containers")]
        ),
    ]
)
