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
//  TraitGuard.swift
//  DynamoDBTablesSoto
//

// Every type in this module is inside `#if SOTOSDK`, so without the trait the module builds *empty* and
// a consumer's first symptom is `cannot find type ... in scope` — pointing at their own file and naming
// neither this package nor the trait they are missing.
//
// These stubs replace that with the answer. The names resolve whether or not the trait is on, so the
// compiler reports the *unavailability*, at the consumer's use site, with a message saying what to do.
// Only the two public entry points need one: everything else in this module is `internal` or an
// extension on an SDK type, and so was never nameable from outside.
#if !SOTOSDK
@available(
    *,
    unavailable,
    message: """
        SotoDynamoDBCompositePrimaryKeyTable requires the 'SOTOSDK' package trait, which is not enabled. \
        dynamo-db-tables has no default backend; add the trait where you declare the dependency: \
        .package(url: "https://github.com/swift-server-community/dynamo-db-tables", traits: ["SOTOSDK"], from: "...")
        """
)
public struct SotoDynamoDBCompositePrimaryKeyTable {}

@available(
    *,
    unavailable,
    message: """
        SotoDynamoDBCompositePrimaryKeysProjection requires the 'SOTOSDK' package trait, which is not enabled. \
        dynamo-db-tables has no default backend; add the trait where you declare the dependency: \
        .package(url: "https://github.com/swift-server-community/dynamo-db-tables", traits: ["SOTOSDK"], from: "...")
        """
)
public struct SotoDynamoDBCompositePrimaryKeysProjection {}
#endif
