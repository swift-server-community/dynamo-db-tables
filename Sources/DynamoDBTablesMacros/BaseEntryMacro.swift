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
//  BaseEntryMacro.swift
//  DynamoDBTablesMacros
//

import SwiftDiagnostics
import SwiftSyntax
import SwiftSyntaxMacros

protocol CoreMacroAttributes {
    static var macroName: String { get }

    static var protocolName: String { get }
}

protocol MacroAttributes: CoreMacroAttributes {
    static var transformType: String { get }

    static var contextType: String { get }
}

enum BaseEntryDiagnostic<Attributes: CoreMacroAttributes>: String, DiagnosticMessage {
    case notAttachedToEnumDeclaration
    case enumMustNotHaveZeroCases
    case enumCasesMustHaveASingleParameter

    var diagnosticID: MessageID {
        MessageID(domain: "\(Attributes.macroName)Macro", id: rawValue)
    }

    var severity: DiagnosticSeverity { .error }

    static var obj: String { "" }

    var message: String {
        switch self {
        case .notAttachedToEnumDeclaration:
            "@\(Attributes.macroName) must be attached to an enum declaration."
        case .enumMustNotHaveZeroCases:
            "@\(Attributes.macroName) decorated enum must be have at least a singe case."
        case .enumCasesMustHaveASingleParameter:
            "@\(Attributes.macroName) decorated enum can only have case entries with a single parameter."
        }
    }
}

enum BaseEntryMacro<Attributes: MacroAttributes>: ExtensionMacro {
    private static func getCases(
        caseMembers: [EnumCaseDeclSyntax],
        context: some MacroExpansionContext
    )
        -> (hasDiagnostics: Bool, handleCases: SwitchCaseListSyntax, compositePrimaryKeyCases: SwitchCaseListSyntax)
    {
        var handleCases: SwitchCaseListSyntax = []
        var compositePrimaryKeyCases: SwitchCaseListSyntax = []
        var hasDiagnostics = false
        for caseMember in caseMembers {
            for element in caseMember.elements {
                // ensure that the enum case only has one parameter
                guard let parameterClause = element.parameterClause, parameterClause.parameters.count == 1 else {
                    context.diagnose(
                        .init(node: element, message: BaseEntryDiagnostic<Attributes>.enumCasesMustHaveASingleParameter)
                    )
                    hasDiagnostics = true
                    // do nothing for this case
                    continue
                }

                // TODO: when made possible by the language, check that the type of the parameter conforms to `WriteEntry` or `TransactionConstraintEntry`
                // https://github.com/swift-server-community/dynamo-db-tables/issues/38

                let handleCaseSyntax = SwitchCaseListSyntax.Element(
                    """
                    case let .\(element.name)(writeEntry):
                        return try context.transform(writeEntry)
                    """
                )

                handleCases.append(handleCaseSyntax)

                let compositePrimaryKeyCaseSyntax = SwitchCaseListSyntax.Element(
                    """
                    case let .\(element.name)(writeEntry):
                        return writeEntry.compositePrimaryKey
                    """
                )

                compositePrimaryKeyCases.append(compositePrimaryKeyCaseSyntax)
            }
        }

        return (hasDiagnostics, handleCases, compositePrimaryKeyCases)
    }

    static func expansion(
        of _: AttributeSyntax,
        attachedTo declaration: some DeclGroupSyntax,
        providingExtensionsOf type: some TypeSyntaxProtocol,
        conformingTo protocols: [TypeSyntax],
        in context: some MacroExpansionContext
    ) throws -> [ExtensionDeclSyntax] {
        // make sure this is attached to an enum
        guard let enumDeclaration = declaration as? EnumDeclSyntax else {
            context.diagnose(
                .init(node: declaration, message: BaseEntryDiagnostic<Attributes>.notAttachedToEnumDeclaration)
            )

            return []
        }

        let requiresProtocolConformance = protocols.reduce(false) { partialResult, protocolSyntax in
            if let identifierTypeSyntax = protocolSyntax.as(IdentifierTypeSyntax.self),
                identifierTypeSyntax.name.text == Attributes.protocolName
            {
                return true
            }

            return partialResult
        }

        let memberBlock = enumDeclaration.memberBlock.members

        let caseMembers: [EnumCaseDeclSyntax] = memberBlock.compactMap { member in
            if let caseMember = member.decl.as(EnumCaseDeclSyntax.self) {
                return caseMember
            }

            return nil
        }

        // make sure this is attached to an enum
        guard !caseMembers.isEmpty else {
            context.diagnose(
                .init(node: declaration, message: BaseEntryDiagnostic<Attributes>.enumMustNotHaveZeroCases)
            )

            return []
        }

        let (hasDiagnostics, handleCases, compositePrimaryKeyCases) = self.getCases(
            caseMembers: caseMembers,
            context: context
        )

        if hasDiagnostics {
            return []
        }

        let type = TypeSyntax(
            extendedGraphemeClusterLiteral: requiresProtocolConformance
                ? "\(type.trimmed): \(Attributes.protocolName) "
                : "\(type.trimmed) "
        )
        let extensionDecl = try ExtensionDeclSyntax(
            extendedType: type,
            memberBlockBuilder: {
                try FunctionDeclSyntax(
                    "func handle<Context: \(raw: Attributes.contextType)>(context: Context) throws -> Context.\(raw: Attributes.transformType)"
                ) {
                    SwitchExprSyntax(subject: ExprSyntax(stringLiteral: "self"), cases: handleCases)
                }

                try VariableDeclSyntax("var compositePrimaryKey: StandardCompositePrimaryKey") {
                    SwitchExprSyntax(subject: ExprSyntax(stringLiteral: "self"), cases: compositePrimaryKeyCases)
                }
            }
        )

        return [extensionDecl]
    }
}
