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
//  Macros.swift
//  DynamoDBTables
//

@attached(extension, conformances: PolymorphicWriteEntry, names: named(handle(context:)), named(compositePrimaryKey))
public macro PolymorphicWriteEntry() =
    #externalMacro(
        module: "DynamoDBTablesMacros",
        type: "PolymorphicWriteEntryMacro"
    )

@attached(
    extension,
    conformances: PolymorphicTransactionConstraintEntry,
    names: named(handle(context:)),
    named(compositePrimaryKey)
)
public macro PolymorphicTransactionConstraintEntry() =
    #externalMacro(
        module: "DynamoDBTablesMacros",
        type: "PolymorphicTransactionConstraintEntryMacro"
    )

@attached(
    extension,
    conformances: PolymorphicOperationReturnType,
    names: named(AttributesType),
    named(TimeToLiveAttributesType),
    named(types)
)
public macro PolymorphicOperationReturnType(databaseItemType: String = "StandardTypedDatabaseItem") =
    #externalMacro(
        module: "DynamoDBTablesMacros",
        type: "PolymorphicOperationReturnTypeMacro"
    )
