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

/**
 * Macros that are built in, rather than being defined by a template.
 */
enum class SystemMacro(
    val id: Byte,
    val systemSymbol: SystemSymbols_1_1,
    override val signature: List<Macro.Parameter>,
    override val body: List<Expression.TemplateBodyExpression>? = null
) : Macro {
    // Technically not system macros, but special forms. However, it's easier to model them as if they are macros in TDL.
    // We give them an ID of -1 to distinguish that they are not addressable outside TDL.
    IfNone(-1, IF_NONE, listOf(zeroToManyTagged("stream"), zeroToManyTagged("true_branch"), zeroToManyTagged("false_branch"))),
    IfSome(-1, IF_SOME, listOf(zeroToManyTagged("stream"), zeroToManyTagged("true_branch"), zeroToManyTagged("false_branch"))),
    IfSingle(-1, IF_SINGLE, listOf(zeroToManyTagged("stream"), zeroToManyTagged("true_branch"), zeroToManyTagged("false_branch"))),
    IfMulti(-1, IF_MULTI, listOf(zeroToManyTagged("stream"), zeroToManyTagged("true_branch"), zeroToManyTagged("false_branch"))),

    // The real macros
    None(0, NONE, emptyList()),
    Values(1, VALUES, listOf(zeroToManyTagged("values"))),
    Annotate(2, ANNOTATE, listOf(zeroToManyTagged("ann"), exactlyOneTagged("value"))),
    MakeString(3, MAKE_STRING, listOf(zeroToManyTagged("text"))),
    MakeSymbol(4, MAKE_SYMBOL, listOf(zeroToManyTagged("text"))),
    MakeBlob(5, MAKE_BLOB, listOf(zeroToManyTagged("bytes"))),
    MakeDecimal(6, MAKE_DECIMAL, listOf(exactlyOneTagged("coefficient"), exactlyOneTagged("exponent"))),
    MakeTimestamp(
        7, MAKE_TIMESTAMP,
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
    // TODO: make_list
    // TODO: make_sexp
    // TODO: make_struct

    /**
     * ```ion
     * (macro set_symbols (symbols*)
     *        $ion::(
     *          (symbol_table [(%symbols)])
     *          (macro_table _)
     *        ))
     * ```
     */
    SetSymbols(
        11, SET_SYMBOLS, listOf(zeroToManyTagged("symbols")),
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
     *        $ion::(
     *          (symbol_table _ [(%symbols)])
     *          (macro_table _)
     *        ))
     * ```
     */
    AddSymbols(
        12, ADD_SYMBOLS, listOf(zeroToManyTagged("symbols")),
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
        13, SET_MACROS, listOf(zeroToManyTagged("macros")),
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
        14, ADD_MACROS, listOf(zeroToManyTagged("macros")),
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
        15, USE, listOf(exactlyOneTagged("catalog_key"), zeroOrOneTagged("version")),
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
    // TODO: parse_ion
    Repeat(17, REPEAT, listOf(exactlyOneTagged("n"), zeroToManyTagged("value"))),
    Delta(18, DELTA, listOf(zeroToManyTagged("deltas"))),
    // TODO: flatten
    Sum(20, SUM, listOf(exactlyOneTagged("a"), exactlyOneTagged("b"))),
    Comment(21, META, listOf(zeroToManyTagged("values")), templateBody { macro(None) {} }),
    MakeField(
        22, MAKE_FIELD,
        listOf(
            Macro.Parameter("field_name", Macro.ParameterEncoding.FlexSym, Macro.ParameterCardinality.ExactlyOne), exactlyOneTagged("value")
        )
    ),

    Default(
        23, DEFAULT, listOf(zeroToManyTagged("expr"), zeroToManyTagged("default_expr")),
        templateBody {
            macro(IfNone) {
                variable(0)
                variable(1)
                variable(0)
            }
        }
    ),
    ;

    val macroName: String get() = this.systemSymbol.text

    override val dependencies: List<Macro>
        get() = body
            ?.filterIsInstance<Expression.MacroInvocation>()
            ?.map(Expression.MacroInvocation::macro)
            ?.distinct()
            ?: emptyList()

    companion object : MacroTable {

        private val MACROS_BY_NAME: Map<String, SystemMacro> = SystemMacro.entries.associateBy { it.macroName }

        // TODO: Once all of the macros are implemented, replace this with an array as in SystemSymbols_1_1
        private val MACROS_BY_ID: Map<Byte, SystemMacro> = SystemMacro.entries
            .filterNot { it.id < 0 }
            .associateBy { it.id }

        @JvmStatic
        fun size() = MACROS_BY_ID.size

        /** Gets a [SystemMacro] by its address in the system table */
        @JvmStatic
        operator fun get(id: Int): SystemMacro? = MACROS_BY_ID[id.toByte()]

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
