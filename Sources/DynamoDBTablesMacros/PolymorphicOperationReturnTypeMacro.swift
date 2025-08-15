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
//  PolymorphicOperationReturnTypeMacro.swift
//  DynamoDBTablesMacros
//

import SwiftDiagnostics
import SwiftSyntax
import SwiftSyntaxMacros

private struct Attributes: CoreMacroAttributes {
    static let macroName: String = "PolymorphicOperationReturnType"

    static let protocolName: String = "PolymorphicOperationReturnType"
}

private struct BasicDiagnosticMessage: DiagnosticMessage {
    let message: String
    let diagnosticID: MessageID
    let severity: SwiftDiagnostics.DiagnosticSeverity = .error

    init(message: String, rawValue: String) {
        self.message = message
        self.diagnosticID = MessageID(domain: "PolymorphicOperationReturnTypeMacro", id: rawValue)
    }
}

public enum PolymorphicOperationReturnTypeMacro: ExtensionMacro {
    public static func expansion(
        of node: AttributeSyntax,
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

        let databaseItemType: String
        let standardDatabaseType = "StandardTypedDatabaseItem"
        if let arguments = node.arguments, case let .argumentList(argumentList) = arguments,
            let firstArgument = argumentList.first, argumentList.count == 1,
            firstArgument.label?.text == "databaseItemType",
            let expression = firstArgument.expression.as(StringLiteralExprSyntax.self)
        {
            databaseItemType = expression.representedLiteralValue ?? standardDatabaseType
        } else {
            databaseItemType = standardDatabaseType
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

        let (hasDiagnostics, handleCases) = self.getCases(
            caseMembers: caseMembers,
            context: context,
            databaseItemType: databaseItemType
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
                try TypeAliasDeclSyntax("typealias AttributesType = StandardPrimaryKeyAttributes")
                try TypeAliasDeclSyntax("typealias TimeToLiveAttributesType = StandardTimeToLiveAttributes")

                let casesArray = ArrayExprSyntax(
                    leftSquare: .leftSquareToken(),
                    elements: handleCases,
                    rightSquare: .rightSquareToken()
                )

                try VariableDeclSyntax(
                    """
                    static let types: [(Codable.Type, PolymorphicOperationReturnOption<AttributesType, Self, TimeToLiveAttributesType>)] =
                    \(casesArray)
                    """
                )
            }
        )

        return [extensionDecl]
    }
}

extension PolymorphicOperationReturnTypeMacro {
    private static func getCases(
        caseMembers: [EnumCaseDeclSyntax],
        context: some MacroExpansionContext,
        databaseItemType: String
    )
        -> (hasDiagnostics: Bool, handleCases: ArrayElementListSyntax)
    {
        var handleCases: ArrayElementListSyntax = []
        var hasDiagnostics = false
        for caseMember in caseMembers {
            for element in caseMember.elements {
                // ensure that the enum case only has one parameter
                guard let parameters = element.parameterClause?.parameters,
                    let parameterType = parameters.first?.type.as(IdentifierTypeSyntax.self),
                    parameters.count == 1
                else {
                    context.diagnose(
                        .init(node: element, message: BaseEntryDiagnostic<Attributes>.enumCasesMustHaveASingleParameter)
                    )
                    hasDiagnostics = true
                    // do nothing for this case
                    continue
                }

                guard parameterType.name.text == databaseItemType,
                    let firstArgumentType = parameterType.genericArgumentClause?.arguments.first?.argument
                else {
                    let message =
                        "PolymorphicOperationReturnTypeMacro decorated enum cases parameter must be of \(databaseItemType) type."
                    context.diagnose(
                        .init(
                            node: element,
                            message: BasicDiagnosticMessage(
                                message: message,
                                rawValue: "enumCasesMustBeOfTheExpectedType"
                            )
                        )
                    )
                    hasDiagnostics = true
                    // do nothing for this case
                    continue
                }

                let handleCaseSyntax = ArrayElementSyntax(
                    expression: ExprSyntax(
                        """
                        (
                            \(firstArgumentType).self, .init { .\(element.name)($0) }
                        )
                        """
                    ),
                    trailingComma: .commaToken()
                )

                handleCases.append(handleCaseSyntax)
            }
        }

        return (hasDiagnostics, handleCases)
    }
}
