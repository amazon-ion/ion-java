// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin10

import com.amazon.ion.IonException
import com.amazon.ion.bytecode.GeneratorTestUtil.assertEqualBytecode
import com.amazon.ion.bytecode.ir.Instructions.I_DIRECTIVE_ADD_SYMBOLS
import com.amazon.ion.bytecode.ir.Instructions.I_DIRECTIVE_SET_SYMBOLS
import com.amazon.ion.bytecode.ir.Instructions.I_DIRECTIVE_USE
import com.amazon.ion.bytecode.ir.Instructions.I_END_CONTAINER
import com.amazon.ion.bytecode.ir.Instructions.I_INT_I32
import com.amazon.ion.bytecode.ir.Instructions.I_NULL_NULL
import com.amazon.ion.bytecode.ir.Instructions.I_STRING_CP
import com.amazon.ion.bytecode.ir.Instructions.I_SYMBOL_CP
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.ConstantPool
import com.amazon.ion.system.IonBinaryWriterBuilder
import com.amazon.ion.system.IonSystemBuilder
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.ByteArrayOutputStream
import com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE as ion_symbol_table

class SymbolTableHelperTest {

    @Test
    fun `symbol table with one symbol`() = expectBytecodeForLst(
        lstText = """ { symbols: [ "a" ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_SYMBOL_CP.packInstructionData(0),
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("a")
    )

    @Test
    fun `symbol table with multiple symbols`() = expectBytecodeForLst(
        lstText = """ { symbols: [ "a", "b", "c", "d" ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_SYMBOL_CP.packInstructionData(0),
            I_SYMBOL_CP.packInstructionData(1),
            I_SYMBOL_CP.packInstructionData(2),
            I_SYMBOL_CP.packInstructionData(3),
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("a", "b", "c", "d"),
    )

    @Test
    fun `import field that is not a list or '$ion_symbol_table' should be ignored`() = expectBytecodeForLst(
        lstText = """ { imports: name } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
        ),
    )

    @Test
    fun `symbol table with LST append and no symbols`() = expectBytecodeForLst(
        lstText = """ { imports: $ion_symbol_table } """,
        // It would also work to have I_ADD_MACROS, I_END_CONTAINER
        expectedBytecode = intArrayOf(),
    )

    @Test
    fun `symbol table with LST append and one symbol`() = expectBytecodeForLst(
        lstText = """ { imports: $ion_symbol_table, symbols: [ "a" ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_ADD_SYMBOLS,
            I_SYMBOL_CP.packInstructionData(0),
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("a")
    )

    @Test
    fun `symbol table with one symbol followed by LST append`() = expectBytecodeForLst(
        lstText = """ { symbols: [ "a" ], imports: $ion_symbol_table } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_ADD_SYMBOLS,
            I_SYMBOL_CP.packInstructionData(0),
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("a")
    )

    @Test
    fun `symbol table with LST append and multiple symbols`() = expectBytecodeForLst(
        lstText = """ { imports: $ion_symbol_table, symbols: [ "a", "b", "c", "d" ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_ADD_SYMBOLS,
            I_SYMBOL_CP.packInstructionData(0),
            I_SYMBOL_CP.packInstructionData(1),
            I_SYMBOL_CP.packInstructionData(2),
            I_SYMBOL_CP.packInstructionData(3),
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("a", "b", "c", "d"),
    )

    @Test
    fun `symbol table with imports and no symbols`() = expectBytecodeForLst(
        lstText = """ { imports: [ {name:"foo",version:1} ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
            I_DIRECTIVE_USE,
            I_STRING_CP.packInstructionData(0),
            I_INT_I32, 1,
            I_NULL_NULL,
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("foo"),
    )

    @Test
    fun `symbol table with import followed by one symbol`() = expectBytecodeForLst(
        lstText = """ { imports: [ {name:"foo",version:1} ], symbols: [ "a" ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
            I_DIRECTIVE_USE,
            I_STRING_CP.packInstructionData(0),
            I_INT_I32, 1,
            I_NULL_NULL,
            I_END_CONTAINER,
            I_DIRECTIVE_ADD_SYMBOLS,
            I_SYMBOL_CP.packInstructionData(1),
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("foo", "a")
    )

    @Test
    fun `symbol table with one symbol followed by import`() = expectBytecodeForLst(
        lstText = """ { symbols: [ "a" ], imports: [ {name:"foo",version:1} ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
            I_DIRECTIVE_USE,
            I_STRING_CP.packInstructionData(1),
            I_INT_I32, 1,
            I_NULL_NULL,
            I_END_CONTAINER,
            I_DIRECTIVE_ADD_SYMBOLS,
            I_SYMBOL_CP.packInstructionData(0),
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("a", "foo")
    )

    @Test
    fun `symbol table with imports and multiple symbols`() = expectBytecodeForLst(
        lstText = """ { imports: [ {name:"foo",version:1} ], symbols: [ "a", "b", "c", "d" ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
            I_DIRECTIVE_USE,
            I_STRING_CP.packInstructionData(0),
            I_INT_I32, 1,
            I_NULL_NULL,
            I_END_CONTAINER,
            I_DIRECTIVE_ADD_SYMBOLS,
            I_SYMBOL_CP.packInstructionData(1),
            I_SYMBOL_CP.packInstructionData(2),
            I_SYMBOL_CP.packInstructionData(3),
            I_SYMBOL_CP.packInstructionData(4),
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("foo", "a", "b", "c", "d"),
    )

    @Test
    fun `symbol table with multiple imports and multiple symbols`() = expectBytecodeForLst(
        lstText = """ { imports: [ {name:"foo",version:1}, {name:"bar",version:2} ], symbols: [ "a", "b", "c", "d" ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
            I_DIRECTIVE_USE,
            // First import
            I_STRING_CP.packInstructionData(0),
            I_INT_I32, 1,
            I_NULL_NULL,
            // Second import
            I_STRING_CP.packInstructionData(1),
            I_INT_I32, 2,
            I_NULL_NULL,
            I_END_CONTAINER,
            // Symbols
            I_DIRECTIVE_ADD_SYMBOLS,
            I_SYMBOL_CP.packInstructionData(2),
            I_SYMBOL_CP.packInstructionData(3),
            I_SYMBOL_CP.packInstructionData(4),
            I_SYMBOL_CP.packInstructionData(5),
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("foo", "bar", "a", "b", "c", "d"),
    )

    @Test
    fun `annotations on symbol table fields should be ignored`() = expectBytecodeForLst(
        lstText = """ { imports: name::[], symbols: version::[] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
        ),
    )

    @Test
    fun `null symbol table struct`() = expectBytecodeForLst(
        lstText = """ null.struct """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
        )
    )

    @Test
    fun `empty symbol table struct`() = expectBytecodeForLst(
        lstText = """ { } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
        )
    )

    @Test
    fun `unspecified fields in symbol table should be ignored`() = expectBytecodeForLst(
        lstText = """ { name: 1 } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
        )
    )

    @Test
    fun `symbol table with empty symbols list`() = expectBytecodeForLst(
        lstText = """ { symbols: [] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
        )
    )

    @Test
    fun `symbol table with symbols field that is not a list`() = expectBytecodeForLst(
        lstText = """ { symbols: 0 } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
        )
    )

    @Test
    fun `symbol table with symbols field that is not a list and one that is a list`() = expectBytecodeForLst(
        lstText = """ { symbols: 0, symbols: [] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
        )
    )

    @Test
    fun `symbol table with two symbols lists`() = assertFails(
        lstText = """ { symbols: [], symbols: [] } """,
        reason = "Multiple symbols fields"
    )

    @Test
    fun `symbol table with two imports lists`() = assertFails(
        lstText = """ { imports: [], imports: [] } """,
        reason = "Multiple imports fields"
    )

    @Test
    fun `symbol table with imports list and LST append`() = assertFails(
        lstText = """ { imports: [], imports: $ion_symbol_table } """,
        reason = "Multiple imports fields"
    )

    @Test
    fun `symbol table with LST append and import list`() = assertFails(
        lstText = """ { imports: $ion_symbol_table, imports: [] } """,
        reason = "Multiple imports fields"
    )

    @Test
    fun `symbol table with two LST append`() = assertFails(
        lstText = """ { imports: $ion_symbol_table, imports: $ion_symbol_table } """,
        reason = "Multiple imports fields"
    )

    // Symbol specific tests

    @Test
    fun `annotations on symbols should be ignored`() = expectBytecodeForLst(
        lstText = """ { symbols: [version::"a"] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_SYMBOL_CP.packInstructionData(0),
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("a")
    )

    @Test
    fun `non-string values in symbol list should result in a symbol with unknown text`() = expectBytecodeForLst(
        lstText = """ { symbols: [1] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_SYMBOL_CP.packInstructionData(0),
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf(null)
    )

    @Test
    fun `null values in symbol list should result in a symbol with unknown text`() = expectBytecodeForLst(
        lstText = """ { symbols: [null, null.string] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_SYMBOL_CP.packInstructionData(0),
            I_SYMBOL_CP.packInstructionData(1),
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf(null, null)
    )

    // Import-specific tests

    @Test
    fun `imports that are null or not structs should be ignored`() = expectBytecodeForLst(
        lstText = """ { imports: [ 1, 2e0, null, null.struct ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
        ),
    )

    @Test
    fun `import with null name should be ignored`() = expectBytecodeForLst(
        lstText = """ { imports: [ {name:null, version:1} ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
        ),
    )

    @Test
    fun `import with no name should be ignored`() = expectBytecodeForLst(
        lstText = """ { imports: [ {version:2} ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
        ),
    )

    @Test
    fun `import with name '$ion' should be ignored`() = expectBytecodeForLst(
        lstText = """ { imports: [ {name:$1, version:1} ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
        ),
    )

    @Test
    fun `import with no version defaults to 1`() = expectBytecodeForLst(
        lstText = """ { imports: [ {name:"foo"} ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
            I_DIRECTIVE_USE,
            I_STRING_CP.packInstructionData(0),
            I_INT_I32, 1,
            I_NULL_NULL,
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("foo")
    )

    @Test
    fun `import with non-default version`() = expectBytecodeForLst(
        lstText = """ { imports: [ {name:"foo", version: 2} ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
            I_DIRECTIVE_USE,
            I_STRING_CP.packInstructionData(0),
            I_INT_I32, 2,
            I_NULL_NULL,
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("foo")
    )

    @Test
    fun `import with max_id specified`() = expectBytecodeForLst(
        lstText = """ { imports: [ {name:"foo", max_id: 10} ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
            I_DIRECTIVE_USE,
            I_STRING_CP.packInstructionData(0),
            I_INT_I32, 1,
            I_INT_I32, 10,
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("foo")
    )

    @Test
    fun `import with max_id unspecified`() = expectBytecodeForLst(
        lstText = """ { imports: [ {name:"foo", version: 1} ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
            I_DIRECTIVE_USE,
            I_STRING_CP.packInstructionData(0),
            I_INT_I32, 1,
            I_NULL_NULL,
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("foo")
    )

    @Test
    fun `max_id that is not a non-negative integer should be ignored`() = expectBytecodeForLst(
        lstText = """
            { 
                imports: [ 
                    {name:"foo", version:2, max_id:-10, max_id:"a", max_id:2.0 },
                    {name:"bar", version:3, max_id:99, max_id:-10, max_id:"a", max_id:2.0 },
                    {name:"baz", version:4, max_id:-10, max_id:"a", max_id:2.0, max_id:100 } 
                ]
            } 
            """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
            I_DIRECTIVE_USE,
            I_STRING_CP.packInstructionData(0),
            I_INT_I32, 2,
            I_NULL_NULL,
            I_STRING_CP.packInstructionData(1),
            I_INT_I32, 3,
            I_INT_I32, 99,
            I_STRING_CP.packInstructionData(2),
            I_INT_I32, 4,
            I_INT_I32, 100,
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("foo", "bar", "baz")
    )

    @Test
    fun `version that is not a positive integer should be ignored`() = expectBytecodeForLst(
        lstText = """
            { 
                imports: [ 
                    {name:"a", version:0 },
                    {name:"b", version:-10, version:"a", version:2.0 },
                    {name:"c", version:3, version:-10, version:"a", version:2.0 },
                    {name:"d", version:-10, version:"a", version:2.0, version:4 } 
                ]
            } 
            """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
            I_DIRECTIVE_USE,
            I_STRING_CP.packInstructionData(0),
            I_INT_I32, 1,
            I_NULL_NULL,
            I_STRING_CP.packInstructionData(1),
            I_INT_I32, 1,
            I_NULL_NULL,
            I_STRING_CP.packInstructionData(2),
            I_INT_I32, 3,
            I_NULL_NULL,
            I_STRING_CP.packInstructionData(3),
            I_INT_I32, 4,
            I_NULL_NULL,
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("a", "b", "c", "d")
    )

    @Test
    fun `unspecified fields in import should be ignored`() = expectBytecodeForLst(
        lstText = """ { imports: [ {name:"foo", symbols: 99, version: 1, imports: 100, max_id: 1} ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
            I_DIRECTIVE_USE,
            I_STRING_CP.packInstructionData(0),
            I_INT_I32, 1,
            I_INT_I32, 1,
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("foo")
    )

    @Test
    fun `annotations in import should be ignored`() = expectBytecodeForLst(
        lstText = """ { imports: [ {name: symbols::"foo", version: imports::1, max_id: $ion_symbol_table::1} ] } """,
        expectedBytecode = intArrayOf(
            I_DIRECTIVE_SET_SYMBOLS,
            I_END_CONTAINER,
            I_DIRECTIVE_USE,
            I_STRING_CP.packInstructionData(0),
            I_INT_I32, 1,
            I_INT_I32, 1,
            I_END_CONTAINER,
        ),
        expectedConstantPool = arrayOf("foo")
    )

    @Test
    fun `import with two name fields`() = assertFails(
        lstText = """ { imports: [ {name:"foo", name:"foo", max_id: 99, version: 1} ]} """,
        reason = "Multiple name fields"
    )

    @Test
    fun `import with two version fields`() = assertFails(
        lstText = """ { imports: [ {name:"foo", max_id: 99, version: 1, version: 1} ]} """,
        reason = "Multiple version fields"
    )

    @Test
    fun `import with two max_id`() = assertFails(
        lstText = """ { imports: [ {name:"foo", max_id: 99, max_id: 99, version: 1} ]} """,
        reason = "Multiple max_id fields"
    )

    /**
     * [lstText] is the Ion text of the symbol table struct, _without_ any annotations. Don't use any user symbols
     * in the text, or the stream positioning logic in this method will get all messed up.
     */
    private fun assertFails(lstText: String, reason: String) {
        val e = assertThrows<IonException> { expectBytecodeForLst(lstText, intArrayOf(/* It should fail before we make this comparison */)) }
        assertTrue(reason in e.message!!, "Exception message \"${e.message}\" does not contain the expected substring \"$reason\"")
    }

    /**
     * [lstText] is the Ion text of the symbol table struct, _without_ any annotations. Don't use any user symbols
     * in the text, or the stream positioning logic in this method will get all messed up.
     */
    private fun expectBytecodeForLst(lstText: String, expectedBytecode: IntArray, expectedConstantPool: Array<Any?> = emptyArray()) {
        val source = IonSystemBuilder.standard().build().singleValue(lstText).let {
            val baos = ByteArrayOutputStream()
            val writer = IonBinaryWriterBuilder.standard().build(baos)
            it.writeTo(writer)
            writer.close()
            baos.toByteArray()
        }

        var position = 4 // After the IVM
        val structTid = source[position++].toInt().and(0xFF)
        val length = when (structTid) {
            0xDE -> {
                val lengthAndSizeOfLength = VarIntHelper.readVarUIntValueAndLength(source, position)
                position += lengthAndSizeOfLength.and(0xFF).toInt()
                lengthAndSizeOfLength.shr(8).toInt()
            }
            0xDF -> 0
            else -> structTid.and(0xF)
        }

        val bytecode = BytecodeBuffer()
        val cp = ConstantPool()
        SymbolTableHelper.compileSymbolTable(source, position, length, bytecode, cp)
        assertEqualBytecode(expectedBytecode, bytecode.toArray())
        assertArrayEquals(expectedConstantPool, cp.toArray())
    }
}
