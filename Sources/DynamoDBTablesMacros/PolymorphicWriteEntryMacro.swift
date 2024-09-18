//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of DynamoDBTables authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

//
//  PolymorphicWriteEntryMacro.swift
//  DynamoDBTablesMacros
//

import SwiftDiagnostics
import SwiftSyntax
import SwiftSyntaxMacros

struct PolymorphicWriteEntryMacroAttributes: MacroAttributes {
    static let macroName: String = "PolymorphicWriteEntry"

    static let protocolName: String = "PolymorphicWriteEntry"

    static let transformType: String = "WriteEntryTransformType"

    static let contextType: String = "PolymorphicWriteEntryContext"
}

public enum PolymorphicWriteEntryMacro: ExtensionMacro {
    public static func expansion(
        of node: AttributeSyntax,
        attachedTo declaration: some DeclGroupSyntax,
        providingExtensionsOf type: some TypeSyntaxProtocol,
        conformingTo protocols: [TypeSyntax],
        in context: some MacroExpansionContext) throws -> [ExtensionDeclSyntax]
    {
        try BaseEntryMacro<PolymorphicWriteEntryMacroAttributes>.expansion(of: node,
                                                                           attachedTo: declaration,
                                                                           providingExtensionsOf: type,
                                                                           conformingTo: protocols,
                                                                           in: context)
    }
}
