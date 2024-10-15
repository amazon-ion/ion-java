// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.impl.*
import com.amazon.ion.impl.SystemSymbols_1_1.*
import com.amazon.ion.impl.macro.ExpressionBuilderDsl.Companion.templateBody
import com.amazon.ion.impl.macro.ParameterFactory.exactlyOneFlexInt
import com.amazon.ion.impl.macro.ParameterFactory.exactlyOneTagged
import com.amazon.ion.impl.macro.ParameterFactory.oneToManyTagged
import com.amazon.ion.impl.macro.ParameterFactory.zeroOrOneTagged
import com.amazon.ion.impl.macro.ParameterFactory.zeroToManyTagged

/**
 * Macros that are built in, rather than being defined by a template.
 */
enum class SystemMacro(val id: Byte, val macroName: String, override val signature: List<Macro.Parameter>, override val body: List<Expression.TemplateBodyExpression>? = null) : Macro {
    // Technically not system macros, but special forms. However, it's easier to model them as if they are macros in TDL.
    // We give them an ID of -1 to distinguish that they are not addressable outside TDL.
    IfNone(-1, "if_none", listOf(zeroToManyTagged("stream"), zeroToManyTagged("true_branch"), zeroToManyTagged("false_branch"))),
    IfSome(-1, "if_some", listOf(zeroToManyTagged("stream"), zeroToManyTagged("true_branch"), zeroToManyTagged("false_branch"))),
    IfSingle(-1, "if_single", listOf(zeroToManyTagged("stream"), zeroToManyTagged("true_branch"), zeroToManyTagged("false_branch"))),
    IfMulti(-1, "if_multi", listOf(zeroToManyTagged("stream"), zeroToManyTagged("true_branch"), zeroToManyTagged("false_branch"))),

    // The real macros
    None(0, "none", emptyList()),
    Values(1, "values", listOf(zeroToManyTagged("values"))),
    Annotate(2, "annotate", listOf(zeroToManyTagged("ann"), exactlyOneTagged("value"))),
    MakeString(3, "make_string", listOf(zeroToManyTagged("text"))),
    MakeSymbol(4, "make_symbol", listOf(zeroToManyTagged("text"))),
    MakeBlob(5, "make_blob", listOf(zeroToManyTagged("bytes"))),
    MakeDecimal(6, "make_decimal", listOf(exactlyOneFlexInt("coefficient"), exactlyOneFlexInt("exponent"))),

    /**
     * ```ion
     * (macro set_symbols (symbols*)
     *        $ion_encoding::(
     *          (symbol_table [(%symbols)])
     *          (macro_table $ion_encoding)
     *        ))
     * ```
     */
    SetSymbols(
        11, "set_symbols", listOf(zeroToManyTagged("symbols")),
        templateBody {
            annotated(ION_ENCODING, ::sexp) {
                sexp {
                    symbol(SYMBOL_TABLE)
                    list { variable(0) }
                }
                sexp {
                    symbol(MACRO_TABLE)
                    symbol(ION_ENCODING)
                }
            }
        }
    ),

    /**
     * ```ion
     * (macro add_symbols (symbols*)
     *        $ion_encoding::(
     *          (symbol_table $ion_encoding [(%symbols)])
     *          (macro_table $ion_encoding)
     *        ))
     * ```
     */
    AddSymbols(
        12, "add_symbols", listOf(zeroToManyTagged("symbols")),
        templateBody {
            annotated(ION_ENCODING, ::sexp) {
                sexp {
                    symbol(SYMBOL_TABLE)
                    symbol(ION_ENCODING)
                    list { variable(0) }
                }
                sexp {
                    symbol(MACRO_TABLE)
                    symbol(ION_ENCODING)
                }
            }
        }
    ),

    /**
     * ```ion
     * (macro set_macros (macros*)
     *        $ion_encoding::(
     *          (symbol_table $ion_encoding)
     *          (macro_table (%macros))
     *        ))
     * ```
     */
    SetMacros(
        13, "set_macros", listOf(zeroToManyTagged("macros")),
        templateBody {
            annotated(ION_ENCODING, ::sexp) {
                sexp {
                    symbol(SYMBOL_TABLE)
                    symbol(ION_ENCODING)
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
     *        $ion_encoding::(
     *          (symbol_table $ion_encoding)
     *          (macro_table $ion_encoding (%macros))
     *        ))
     * ```
     */
    AddMacros(
        14, "add_macros", listOf(zeroToManyTagged("macros")),
        templateBody {
            annotated(ION_ENCODING, ::sexp) {
                sexp {
                    symbol(SYMBOL_TABLE)
                    symbol(ION_ENCODING)
                }
                sexp {
                    symbol(MACRO_TABLE)
                    symbol(ION_ENCODING)
                    variable(0)
                }
            }
        }
    ),

    /**
     * ```ion
     * (macro use (catalog_key version?)
     *        $ion_encoding::(
     *          (import the_module (%catalog_key) (.if_none (%version) 1 (%version)))
     *          (symbol_table $ion_encoding the_module)
     *          (macro_table $ion_encoding the_module)
     *        ))
     * ```
     */
    Use(
        15, "use", listOf(exactlyOneTagged("catalog_key"), zeroOrOneTagged("version")),
        templateBody {
            val theModule = _Private_Utils.newSymbolToken("the_module")
            annotated(ION_ENCODING, ::sexp) {
                sexp {
                    symbol(IMPORT)
                    symbol(theModule)
                    variable(0)
                    macro(IfNone) {
                        variable(1)
                        int(1)
                        variable(1)
                    }
                }
                sexp {
                    symbol(SYMBOL_TABLE)
                    symbol(ION_ENCODING)
                    symbol(theModule)
                }
                sexp {
                    symbol(MACRO_TABLE)
                    symbol(ION_ENCODING)
                    symbol(theModule)
                }
            }
        }
    ),

    Repeat(17, "repeat", listOf(exactlyOneTagged("n"), oneToManyTagged("value"))),

    Comment(21, "comment", listOf(zeroToManyTagged("values")), templateBody { macro(None) {} }),
    MakeField(
        22, "make_field",
        listOf(
            Macro.Parameter("field_name", Macro.ParameterEncoding.CompactSymbol, Macro.ParameterCardinality.ExactlyOne), exactlyOneTagged("value")
        )
    ),

    // TODO: Other system macros
    ;

    override val dependencies: List<Macro>
        get() = body
            ?.filterIsInstance<Expression.MacroInvocation>()
            ?.map(Expression.MacroInvocation::macro)
            ?.distinct()
            ?: emptyList()

    companion object {

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
        operator fun get(address: MacroRef): SystemMacro? {
            return when (address) {
                is MacroRef.ById -> get(address.id)
                is MacroRef.ByName -> get(address.name)
            }
        }

        /** Gets a [SystemMacro] by name, including those which are not in the system table (i.e. special forms) */
        @JvmStatic
        fun getMacroOrSpecialForm(name: String): SystemMacro? = MACROS_BY_NAME[name]
    }
}
