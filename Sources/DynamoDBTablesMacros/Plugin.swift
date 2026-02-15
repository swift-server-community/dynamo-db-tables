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
//  Plugin.swift
//  DynamoDBTablesMacros
//

#if canImport(SwiftCompilerPlugin)
import SwiftCompilerPlugin
import SwiftSyntaxMacros

@main
struct DynamoDBTablesMacrosCompilerPlugin: CompilerPlugin {
    let providingMacros: [Macro.Type] = [
        PolymorphicWriteEntryMacro.self,
        PolymorphicTransactionConstraintEntryMacro.self,
        PolymorphicOperationReturnTypeMacro.self,
    ]
}
#endif
