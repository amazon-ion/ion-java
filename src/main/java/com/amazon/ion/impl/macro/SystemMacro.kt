// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.SystemSymbols
import com.amazon.ion.impl.*
import com.amazon.ion.impl.SystemSymbols_1_1.*
import com.amazon.ion.impl.macro.ExpressionBuilderDsl.Companion.templateBody
import com.amazon.ion.impl.macro.ParameterFactory.exactlyOneTagged
import com.amazon.ion.impl.macro.ParameterFactory.zeroOrOneTagged
import com.amazon.ion.impl.macro.ParameterFactory.zeroToManyTagged
import com.amazon.ion.util.*
import com.amazon.ion.util.unreachable

/**
 * Macros that are built in, rather than being defined by a template.
 */
enum class SystemMacro(
    val id: Byte,
    val expansionKind: Byte,
    private val _systemSymbol: SystemSymbols_1_1?,
    override val signature: List<Macro.Parameter>,
    override val body: List<Expression.TemplateBodyExpression>? = null,
    override val bodyTape: ExpressionTape.Core = ExpressionTape.from(body ?: emptyList())
) : Macro {
    // Technically not system macros, but special forms. However, it's easier to model them as if they are macros in TDL.
    // We give them an ID of -1 to distinguish that they are not addressable outside TDL.
    IfNone(-1, ExpansionKinds.IF_NONE, IF_NONE, listOf(zeroToManyTagged("stream"), zeroToManyTagged("true_branch"), zeroToManyTagged("false_branch"))),
    IfSome(-1, ExpansionKinds.IF_SOME, IF_SOME, listOf(zeroToManyTagged("stream"), zeroToManyTagged("true_branch"), zeroToManyTagged("false_branch"))),
    IfSingle(-1, ExpansionKinds.IF_SINGLE, IF_SINGLE, listOf(zeroToManyTagged("stream"), zeroToManyTagged("true_branch"), zeroToManyTagged("false_branch"))),
    IfMulti(-1, ExpansionKinds.IF_MULTI, IF_MULTI, listOf(zeroToManyTagged("stream"), zeroToManyTagged("true_branch"), zeroToManyTagged("false_branch"))),

    // Unnameable, unaddressable macros used for the internals of certain other system macros
    // TODO: See if we can move these somewhere else so that they are not visible
    _Private_FlattenStruct(-1, ExpansionKinds.PRIVATE_FLATTEN_STRUCT, _systemSymbol = null, listOf(zeroToManyTagged("structs"))),
    _Private_MakeFieldNameAndValue(-1, ExpansionKinds.PRIVATE_MAKE_FIELD_NAME_AND_VALUE, _systemSymbol = null, listOf(exactlyOneTagged("fieldName"), exactlyOneTagged("value"))),

    // The real macros
    Values(1, ExpansionKinds.STREAM, VALUES, listOf(zeroToManyTagged("values")), templateBody { variable(0) }),
    None(0, ExpansionKinds.EMPTY, NONE, emptyList(), templateBody { macro(Values) { expressionGroup { } } }),
    Default(
        2, ExpansionKinds.UNINITIALIZED, DEFAULT, listOf(zeroToManyTagged("expr"), zeroToManyTagged("default_expr")),
        templateBody {
            macro(IfNone) { variable(0); variable(1); variable(0) }
        }
    ),
    Meta(3, ExpansionKinds.UNINITIALIZED, META, listOf(zeroToManyTagged("values")), templateBody { macro(None) {} }),
    Repeat(4, ExpansionKinds.REPEAT, REPEAT, listOf(exactlyOneTagged("n"), zeroToManyTagged("value"))),
    Flatten(5, ExpansionKinds.FLATTEN, FLATTEN, listOf(zeroToManyTagged("values"))),
    Delta(6, ExpansionKinds.DELTA, DELTA, listOf(zeroToManyTagged("deltas"))),
    Sum(7, ExpansionKinds.SUM, SUM, listOf(exactlyOneTagged("a"), exactlyOneTagged("b"))),

    Annotate(8, ExpansionKinds.ANNOTATE, ANNOTATE, listOf(zeroToManyTagged("ann"), exactlyOneTagged("value"))),
    MakeString(9, ExpansionKinds.MAKE_STRING, MAKE_STRING, listOf(zeroToManyTagged("text"))),
    MakeSymbol(10, ExpansionKinds.MAKE_SYMBOL, MAKE_SYMBOL, listOf(zeroToManyTagged("text"))),
    MakeDecimal(11, ExpansionKinds.MAKE_DECIMAL, MAKE_DECIMAL, listOf(exactlyOneTagged("coefficient"), exactlyOneTagged("exponent"))),
    MakeTimestamp(
        12, ExpansionKinds.MAKE_TIMESTAMP, MAKE_TIMESTAMP,
        listOf(
            exactlyOneTagged("year"),
            zeroOrOneTagged("month"),
            zeroOrOneTagged("day"),
            zeroOrOneTagged("hour"),
            zeroOrOneTagged("minute"),
            zeroOrOneTagged("second"),
            zeroOrOneTagged("offset_minutes"),
        )
    ),
    MakeBlob(13, ExpansionKinds.MAKE_BLOB, MAKE_BLOB, listOf(zeroToManyTagged("bytes"))),
    MakeList(
        14, ExpansionKinds.FLATTEN, MAKE_LIST, listOf(zeroToManyTagged("sequences")),
        templateBody {
            list {
                macro(Flatten) {
                    variable(0)
                }
            }
        }
    ),
    MakeSExp(
        15, ExpansionKinds.FLATTEN, MAKE_SEXP, listOf(zeroToManyTagged("sequences")),
        templateBody {
            sexp {
                macro(Flatten) {
                    variable(0)
                }
            }
        }
    ),

    MakeField(
        16, ExpansionKinds.PRIVATE_MAKE_FIELD_NAME_AND_VALUE, MAKE_FIELD, listOf(exactlyOneTagged("fieldName"), exactlyOneTagged("value")),
        templateBody {
            struct {
                macro(_Private_MakeFieldNameAndValue) {
                    variable(0)
                    variable(1)
                }
            }
        }
    ),

    MakeStruct(
        17, ExpansionKinds.PRIVATE_FLATTEN_STRUCT, MAKE_STRUCT, listOf(zeroToManyTagged("structs")),
        templateBody {
            struct {
                macro(_Private_FlattenStruct) {
                    variable(0)
                }
            }
        }
    ),
    ParseIon(18, ExpansionKinds.UNINITIALIZED, PARSE_ION, listOf(zeroToManyTagged("data"))), // TODO: parse_ion

    /**
     * ```ion
     * (macro set_symbols (symbols*)
     *        $ion::(module _
     *          (symbol_table [(%symbols)])
     *          (macro_table _)
     *        ))
     * ```
     */
    SetSymbols(
        19, ExpansionKinds.UNINITIALIZED, SET_SYMBOLS, listOf(zeroToManyTagged("symbols")),
        templateBody {
            annotated(ION, ::sexp) {
                symbol(MODULE)
                symbol(SystemSymbols.DEFAULT_MODULE)
                sexp {
                    symbol(SYMBOL_TABLE)
                    list { variable(0) }
                }
                sexp {
                    symbol(MACRO_TABLE)
                    symbol(SystemSymbols.DEFAULT_MODULE)
                }
            }
        }
    ),

    /**
     * ```ion
     * (macro add_symbols (symbols*)
     *        $ion::(module _
     *          (symbol_table _ [(%symbols)])
     *          (macro_table _)
     *        ))
     * ```
     */
    AddSymbols(
        20, ExpansionKinds.UNINITIALIZED, ADD_SYMBOLS, listOf(zeroToManyTagged("symbols")),
        templateBody {
            annotated(ION, ::sexp) {
                symbol(MODULE)
                symbol(com.amazon.ion.SystemSymbols.DEFAULT_MODULE)
                sexp {
                    symbol(SYMBOL_TABLE)
                    symbol(com.amazon.ion.SystemSymbols.DEFAULT_MODULE)
                    list { variable(0) }
                }
                sexp {
                    symbol(MACRO_TABLE)
                    symbol(SystemSymbols.DEFAULT_MODULE)
                }
            }
        }
    ),

    /**
     * ```ion
     * (macro set_macros (macros*)
     *        $ion::(module _
     *          (symbol_table _)
     *          (macro_table (%macros))
     *        ))
     * ```
     */
    SetMacros(
        21, ExpansionKinds.UNINITIALIZED, SET_MACROS, listOf(zeroToManyTagged("macros")),
        templateBody {
            annotated(ION, ::sexp) {
                symbol(MODULE)
                symbol(SystemSymbols.DEFAULT_MODULE)
                sexp {
                    symbol(SYMBOL_TABLE)
                    symbol(SystemSymbols.DEFAULT_MODULE)
                }
                sexp {
                    symbol(MACRO_TABLE)
                    variable(0)
                }
            }
        }
    ),

    /**
     * ```ion
     * (macro add_macros (macros*)
     *        $ion::(module _
     *          (symbol_table _)
     *          (macro_table _ (%macros))
     *        ))
     * ```
     */
    AddMacros(
        22, ExpansionKinds.UNINITIALIZED, ADD_MACROS, listOf(zeroToManyTagged("macros")),
        templateBody {
            annotated(ION, ::sexp) {
                symbol(MODULE)
                symbol(SystemSymbols.DEFAULT_MODULE)
                sexp {
                    symbol(SYMBOL_TABLE)
                    symbol(SystemSymbols.DEFAULT_MODULE)
                }
                sexp {
                    symbol(MACRO_TABLE)
                    symbol(SystemSymbols.DEFAULT_MODULE)
                    variable(0)
                }
            }
        }
    ),

    /**
     * ```ion
     * (macro use (catalog_key version?)
     *        $ion::(module _
     *          (import the_module catalog_key (.default (%version) 1))
     *          (symbol_table _ the_module)
     *          (macro_table _ the_module)
     *        ))
     * ```
     */
    Use(
        23, ExpansionKinds.UNINITIALIZED, USE, listOf(exactlyOneTagged("catalog_key"), zeroOrOneTagged("version")),
        templateBody {
            val theModule = _Private_Utils.newSymbolToken("the_module")
            annotated(ION, ::sexp) {
                symbol(MODULE)
                symbol(SystemSymbols.DEFAULT_MODULE)
                sexp {
                    symbol(IMPORT)
                    symbol(theModule)
                    variable(0)
                    // This is equivalent to `(.default (%version) 1)`, but eliminates a layer of indirection.
                    macro(IfNone) {
                        variable(1)
                        int(1)
                        variable(1)
                    }
                }
                sexp {
                    symbol(SYMBOL_TABLE)
                    symbol(SystemSymbols.DEFAULT_MODULE)
                    symbol(theModule)
                }
                sexp {
                    symbol(MACRO_TABLE)
                    symbol(SystemSymbols.DEFAULT_MODULE)
                    symbol(theModule)
                }
            }
        }
    ),
    ;

    override val isSimple by lazy {
        bodyTape.numberOfVariables == signature.size && bodyTape.areVariablesOrdered()
    }

    val systemSymbol: SystemSymbols_1_1
        get() = _systemSymbol ?: throw IllegalStateException("Attempted to get name for unaddressable macro $name")

    val macroName: String get() = this.systemSymbol.text

    override val dependencies: List<Macro>
        get() = body
            ?.filterIsInstance<Expression.MacroInvocation>()
            ?.map(Expression.MacroInvocation::macro)
            ?.distinct()
            ?: emptyList()

    companion object : MacroTable {

        private val MACROS_BY_NAME: Map<String, SystemMacro> = SystemMacro.entries
            .filter { it._systemSymbol != null }
            .associateBy { it.macroName }

        @JvmStatic
        fun size() = 24

        /** Gets a [SystemMacro] by its address in the system table */
        @JvmStatic
        operator fun get(id: Int): SystemMacro {
            return when (id) {
                0 -> None
                1 -> Values
                2 -> Default
                3 -> Meta
                4 -> Repeat
                5 -> Flatten
                6 -> Delta
                7 -> Sum
                8 -> Annotate
                9 -> MakeString
                10 -> MakeSymbol
                11 -> MakeDecimal
                12 -> MakeTimestamp
                13 -> MakeBlob
                14 -> MakeList
                15 -> MakeSExp
                16 -> MakeField
                17 -> MakeStruct
                18 -> ParseIon
                19 -> SetSymbols
                20 -> AddSymbols
                21 -> SetMacros
                22 -> AddMacros
                23 -> Use
                else -> unreachable()
            }
        }

        /** Gets, by name, a [SystemMacro] with an address in the system table (i.e. that can be invoked as E-Expressions) */
        @JvmStatic
        operator fun get(name: String): SystemMacro? = MACROS_BY_NAME[name]?.takeUnless { it.id < 0 }

        @JvmStatic
        override operator fun get(address: MacroRef): SystemMacro? {
            return when (address) {
                is MacroRef.ById -> get(address.id)
                is MacroRef.ByName -> get(address.name)
            }
        }

        /** Gets a [SystemMacro] by name, including those which are not in the system table (i.e. special forms) */
        @JvmStatic
        fun getMacroOrSpecialForm(ref: MacroRef): SystemMacro? {
            return when (ref) {
                is MacroRef.ById -> get(ref.id)
                is MacroRef.ByName -> MACROS_BY_NAME[ref.name]
            }
        }

        @JvmStatic
        val SYSTEM_MACRO_TABLE = this
    }
}
