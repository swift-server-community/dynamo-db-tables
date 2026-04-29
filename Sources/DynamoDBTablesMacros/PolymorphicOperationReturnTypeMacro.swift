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

private struct OperationReturnTypeCases {
    var hasDiagnostics: Bool
    var typesArrayElements: ArrayElementListSyntax
    var getItemKeyCases: SwitchCaseListSyntax
    var assertions: [DeclSyntax]
}

public enum PolymorphicOperationReturnTypeMacro: ExtensionMacro {
    public static func expansion(
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

        let cases = self.getCases(caseMembers: caseMembers, context: context)

        if cases.hasDiagnostics {
            return []
        }

        let polymorphicType = TypeSyntax(
            extendedGraphemeClusterLiteral: requiresProtocolConformance
                ? "\(type.trimmed): \(Attributes.protocolName) "
                : "\(type.trimmed) "
        )
        let extensionDecl = try ExtensionDeclSyntax(
            extendedType: polymorphicType,
            memberBlockBuilder: {
                try TypeAliasDeclSyntax("typealias AttributesType = StandardPrimaryKeyAttributes")
                try TypeAliasDeclSyntax("typealias TimeToLiveAttributesType = StandardTimeToLiveAttributes")

                let casesArray = ArrayExprSyntax(
                    leftSquare: .leftSquareToken(),
                    elements: cases.typesArrayElements,
                    rightSquare: .rightSquareToken()
                )

                try VariableDeclSyntax(
                    """
                    static let types: [(Codable.Type, PolymorphicOperationReturnOption<AttributesType, Self, TimeToLiveAttributesType>)] =
                    \(casesArray)
                    """
                )

                for assertion in cases.assertions {
                    assertion
                }
            }
        )

        let batchCapableExtensionDecl = try self.batchCapableExtension(
            type: type,
            getItemKeyCases: cases.getItemKeyCases
        )

        return [extensionDecl, batchCapableExtensionDecl]
    }
}

extension PolymorphicOperationReturnTypeMacro {
    private static func batchCapableExtension(
        type: some TypeSyntaxProtocol,
        getItemKeyCases: SwitchCaseListSyntax
    ) throws -> ExtensionDeclSyntax {
        let batchCapableType = TypeSyntax(
            extendedGraphemeClusterLiteral: "\(type.trimmed): BatchCapableReturnType "
        )
        return try ExtensionDeclSyntax(
            extendedType: batchCapableType,
            memberBlockBuilder: {
                try FunctionDeclSyntax(
                    "func getItemKey() -> CompositePrimaryKey<AttributesType>"
                ) {
                    SwitchExprSyntax(subject: ExprSyntax(stringLiteral: "self"), cases: getItemKeyCases)
                }
            }
        )
    }

    private static func getCases(
        caseMembers: [EnumCaseDeclSyntax],
        context: some MacroExpansionContext
    ) -> OperationReturnTypeCases {
        var result = OperationReturnTypeCases(
            hasDiagnostics: false,
            typesArrayElements: [],
            getItemKeyCases: [],
            assertions: []
        )
        for caseMember in caseMembers {
            for element in caseMember.elements {
                // ensure that the enum case only has one parameter
                guard let parameters = element.parameterClause?.parameters, parameters.count == 1,
                    let parameter = parameters.first
                else {
                    context.diagnose(
                        .init(node: element, message: BaseEntryDiagnostic<Attributes>.enumCasesMustHaveASingleParameter)
                    )
                    result.hasDiagnostics = true
                    // do nothing for this case
                    continue
                }

                let paramType = parameter.type.trimmedDescription

                result.typesArrayElements.append(
                    ArrayElementSyntax(
                        expression: ExprSyntax(
                            """
                            (
                                \(raw: paramType).RowType.self, .init { .\(element.name)($0) }
                            )
                            """
                        ),
                        trailingComma: .commaToken()
                    )
                )

                result.getItemKeyCases.append(
                    SwitchCaseListSyntax.Element(
                        """
                        case let .\(element.name)(databaseItem):
                            return databaseItem.compositePrimaryKey
                        """
                    )
                )

                result.assertions.append(
                    self.assertionDecl(for: element, paramType: paramType, in: context)
                )
            }
        }

        return result
    }

    /// Emits a per-case assertion helper that forces a compile-time check that the case parameter type
    /// is a `TypedTTLDatabaseItem<StandardPrimaryKeyAttributes, _, StandardTimeToLiveAttributes>` (or
    /// any typealias thereof). When the case has a known source location, wraps the call in
    /// `#sourceLocation` directives so the diagnostic surfaces at the user's case declaration.
    private static func assertionDecl(
        for element: EnumCaseElementListSyntax.Element,
        paramType: String,
        in context: some MacroExpansionContext
    ) -> DeclSyntax {
        let assertionCall = "_assertPolymorphicOperationReturnTypeParameter(\(paramType).self)"
        let body: String
        if let location = context.location(of: element) {
            body = """
                #sourceLocation(file: \(location.file), line: \(location.line))
                \(assertionCall)
                #sourceLocation()
                """
        } else {
            body = assertionCall
        }
        return DeclSyntax(
            stringLiteral: """
                private static func _assertCase_\(element.name.text)() {
                    \(body)
                }
                """
        )
    }
}
