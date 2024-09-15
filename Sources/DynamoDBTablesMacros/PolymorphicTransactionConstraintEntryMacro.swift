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
//  PolymorphicTransactionConstraintEntryMacro.swift
//  DynamoDBTablesMacros
//

import SwiftDiagnostics
import SwiftSyntax
import SwiftSyntaxMacros

struct PolymorphicTransactionConstraintEntryMacroAttributes: MacroAttributes {
    static var macroName: String = "PolymorphicTransactionConstraintEntry"

    static var protocolName: String = "PolymorphicTransactionConstraintEntry"

    static var transformType: String = "WriteTransactionConstraintType"

    static var contextType: String = "PolymorphicWriteEntryContext"
}

public enum PolymorphicTransactionConstraintEntryMacro: ExtensionMacro {
    public static func expansion(
        of node: AttributeSyntax,
        attachedTo declaration: some DeclGroupSyntax,
        providingExtensionsOf type: some TypeSyntaxProtocol,
        conformingTo protocols: [TypeSyntax],
        in context: some MacroExpansionContext) throws -> [ExtensionDeclSyntax]
    {
        try BaseEntryMacro<PolymorphicTransactionConstraintEntryMacroAttributes>.expansion(of: node,
                                                                                           attachedTo: declaration,
                                                                                           providingExtensionsOf: type,
                                                                                           conformingTo: protocols,
                                                                                           in: context)
    }
}
