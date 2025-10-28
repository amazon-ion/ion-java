# Bytecode Reference

View the rendered version of this file [on GitHub](https://github.com/amazon-ion/ion-java/tree/master/src/main/java/com/amazon/ion/bytecode/ir/instruction_reference.md).

## Bytecode IR

The bytecode intermediate representation (IR) is a sequence of _instructions_ and _operands_, each represented by a 32-bit integer.
* An **instruction** is composed of 8 bits of _operation_, 2 bits of _operand count_, and 22 bits of _data_. 
* An **operation** tells the Bytecode interpreter what to do. It is composed of 5 bits representing the _operation kind_ and 3 bits representing the _operation variant_.
* **Operation Kind** is the general category of the operation (Is it a String, Int, Bool, Struct, IVM, Directive, etc.) and is used for general control flow.
* **Operation Variant** denotes the specific variant of an operation.
* **Operand Count** is 2 bits that describes the number of operands for the instruction (so that no lookup table is required when advancing past an instruction and its operands).
  When the value is `11`, the instruction can be said to "contain" other instructions (i.e. it is a container) and the value of _data_ indicates the number of 
  instructions and operands to skip. Otherwise (`00`, `01`, `10`) it is the number of operands for that instruction.
  Operations that cannot be skipped have an operand count of `00`.
* The **data** is 22 bits of operation-specific information that can be embedded directly into the instruction. See the operation table for the interpretation of _data_. 
* Each **operand** itself is an Integer in (i.e. one line of) the bytecode.

## Constant Pool

The constant pool is an `Array<Object>` (or `Array<Any>` in Kotlin).
Instructions with constant pool indices have the index stored in the _data_ component of the instruction.

## Example

```
  ┌── Operation Kind = String
  │     ┌── Operation Variant 0
  │     │    ┌── Operand Count = 0
  │     │    │            ┌── CP_INDEX = 5 
┌─┴─┐  ┌┴┐  ┌┤  ┌─────────┴──────────┐
00111  000  00  0000000000000000000101
└────┬───┘
     └── Operation 0x38: STRING_CP
```
This instruction indicates that there is a String value, and the text of the string is in the constant pool at index 5.


## Operation Reference Table

* K: Operation Kind
* V: Variant
* O: Operand Count Bits

| Operation Name        | Oper.  |       K |   V   | O    | Data                  | Operand(s)   | Notes                                                           |
|-----------------------|--------|--------:|:-----:|:-----|-----------------------|--------------|-----------------------------------------------------------------|
| NULL_NULL             | `0x0F` | `00001` | `111` | `00` | -                     | -            |                                                                 |
| BOOL                  | `0x10` | `00010` | `000` | `00` | boolean               | -            | Lowest order bit is either 0 (false) or 1 (true)                |
| NULL_BOOL             | `0x17` | `00010` | `111` | `00` | -                     | -            |                                                                 |
| INT_I16               | `0x18` | `00011` | `000` | `00` | i16                   | -            |                                                                 |
| INT_I32               | `0x19` | `00011` | `001` | `01` | -                     | i32          |                                                                 |
| INT_I64               | `0x1A` | `00011` | `010` | `10` | -                     | i64          | split across operands: high order i32, then low order i32       |
| INT_CP                | `0x1B` | `00011` | `011` | `00` | cp_index (u22)        | -            | Non-null [BigInteger] in constant pool                          |
| INT_REF               | `0x1C` | `00011` | `100` | `01` | ref_length (u22)      | offset (u32) | Should only be used for ints larger than 8 bytes                |
| NULL_INT              | `0x1F` | `00011` | `111` | `00` | -                     | -            |                                                                 |
| FLOAT_F32             | `0x20` | `00100` | `000` | `01` | -                     | f32          |                                                                 |
| FLOAT_F64             | `0x21` | `00100` | `001` | `10` | -                     | f64          | split across 2 operands: high order i32, then low order i32     |
| NULL_FLOAT            | `0x27` | `00100` | `111` | `00` | -                     | -            |                                                                 |
| DECIMAL_CP            | `0x28` | `00101` | `000` | `00` | cp_index (u22)        | -            | Non-null [Decimal] in constant pool                             |
| DECIMAL_REF           | `0x29` | `00101` | `001` | `01` | ref_length (u22)      | offset (u32) |                                                                 |
| NULL_DECIMAL          | `0x2F` | `00101` | `111` | `00` | -                     | -            |                                                                 |
| TIMESTAMP_CP          | `0x30` | `00110` | `000` | `00` | cp_index (u22)        | -            | Non-null [Timestamp] in constant pool                           |
| SHORT_TIMESTAMP_REF   | `0x31` | `00110` | `001` | `01` | opcode (u8)           | offset (u32) |                                                                 |
| TIMESTAMP_REF         | `0x32` | `00110` | `010` | `01` | ref_length (u22)      | offset (u32) |                                                                 |
| NULL_TIMESTAMP        | `0x37` | `00110` | `111` | `00` | -                     | -            |                                                                 |
| STRING_CP             | `0x38` | `00111` | `000` | `00` | cp_index (u22)        | -            | Non-null [String] in constant pool                              |
| STRING_REF            | `0x39` | `00111` | `001` | `01` | ref_length (u22)      | offset (u32) | Reference to UTF-8 bytes                                        |
| NULL_STRING           | `0x3F` | `00111` | `111` | `00` | -                     | -            |                                                                 |
| SYMBOL_CP             | `0x40` | `01000` | `000` | `00` | cp_index (u22)        | -            | Non-null [String] in constant pool                              |
| SYMBOL_REF            | `0x41` | `01000` | `001` | `01` | ref_length (u22)      | offset (u32) | Reference to UTF-8 bytes                                        |
| SYMBOL_SID            | `0x42` | `01000` | `010` | `00` | sid (u22)             | -            |                                                                 |
| SYMBOL_CHAR           | `0x43` | `01000` | `011` | `00` | char                  | -            | Single character symbol.                                        |
| NULL_SYMBOL           | `0x47` | `01000` | `111` | `00` | -                     | -            |                                                                 |
| CLOB_CP               | `0x48` | `01001` | `000` | `00` | cp_index (u22)        | -            | Clob ([ByteSlice]) from constant pool                           |
| CLOB_REF              | `0x49` | `01001` | `001` | `01` | ref_length (u22)      | offset (u32) |                                                                 |
| NULL_CLOB             | `0x4F` | `01001` | `111` | `00` | -                     | -            |                                                                 |
| BLOB_CP               | `0x50` | `01010` | `000` | `00` | cp_index (u22)        | -            | Blob ([ByteSlice]) from constant pool                           |
| BLOB_REF              | `0x51` | `01010` | `001` | `01` | ref_length (u22)      | offset (u32) |                                                                 |
| NULL_BLOB             | `0x57` | `01010` | `111` | `00` | -                     | -            |                                                                 |
| LIST_START            | `0x58` | `01011` | `000` | `11` | bytecode_length (u22) | -            | Length must include the END_CONTAINER instruction               |
| NULL_LIST             | `0x5F` | `01011` | `111` | `00` | -                     | -            |                                                                 |
| SEXP_START            | `0x60` | `01100` | `000` | `11` | bytecode_length (u22) | -            | Length must include the END_CONTAINER instruction               |
| NULL_SEXP             | `0x67` | `01100` | `111` | `00` | -                     | -            |                                                                 |
| STRUCT_START          | `0x68` | `01101` | `000` | `11` | bytecode_length (u22) | -            | Length must include the END_CONTAINER instruction               |
| NULL_STRUCT           | `0x6F` | `01101` | `111` | `00` | -                     | -            |                                                                 |
| ANNOTATION_CP         | `0x70` | `01110` | `000` | `00` | cp_index (u22)        | -            | Non-null [String] in constant pool                              |
| ANNOTATION_REF        | `0x71` | `01110` | `001` | `01` | ref_length (u22)      | offset (u32) | Reference to UTF-8 bytes                                        |
| ANNOTATION_SID        | `0x72` | `01110` | `010` | `00` | sid (u22)             | -            |                                                                 |
| FIELD_NAME_CP         | `0x78` | `01111` | `000` | `00` | cp_index (u22)        | -            | Non-null [String] in constant pool                              |
| FIELD_NAME_REF        | `0x79` | `01111` | `001` | `01` | ref_length (u22)      | offset (u32) | Reference to UTF-8 bytes                                        |
| FIELD_NAME_SID        | `0x7A` | `01111` | `010` | `00` | sid (u22)             | -            |                                                                 |
| IVM                   | `0x80` | `10000` | `000` | `00` | version (u8, u8)      | -            | version is packed as u8 major, u8 minor                         |
| DIRECTIVE_SET_SYMBOLS | `0x88` | `10001` | `000` | `00` | -                     | -            | Must have END_CONTAINER instruction to delimit end of directive |
| DIRECTIVE_ADD_SYMBOLS | `0x89` | `10001` | `001` | `00` | -                     | -            | Must have END_CONTAINER instruction to delimit end of directive |
| DIRECTIVE_SET_MACROS  | `0x8A` | `10001` | `010` | `00` | -                     | -            | Must have END_CONTAINER instruction to delimit end of directive |
| DIRECTIVE_ADD_MACROS  | `0x8B` | `10001` | `011` | `00` | -                     | -            | Must have END_CONTAINER instruction to delimit end of directive |
| DIRECTIVE_USE         | `0x8C` | `10001` | `100` | `00` | -                     | -            | Must have END_CONTAINER instruction to delimit end of directive |
| DIRECTIVE_MODULE      | `0x8D` | `10001` | `101` | `00` | -                     | -            | Must have END_CONTAINER instruction to delimit end of directive |
| DIRECTIVE_IMPORT      | `0x8E` | `10001` | `110` | `00` | -                     | -            | Must have END_CONTAINER instruction to delimit end of directive |
| DIRECTIVE_ENCODING    | `0x8F` | `10001` | `111` | `00` | -                     | -            | Must have END_CONTAINER instruction to delimit end of directive |
| PLACEHOLDER_TAGGED    | `0x90` | `10010` | `000` | `11` | bytecode_length (u22) | -            | Optional tagged macro parameter.[^0x91]                         |
| PLACEHOLDER_TAGLESS   | `0x91` | `10010` | `001` | `00` | opcode (u8)           | -            | Tagless macro parameter                                         |
| ARGUMENT_NONE         | `0x98` | `10011` | `000` | `00` | -                     | -            | Represents an argument that is absent.                          |
| INVOKE                | `0xA0` | `10100` | `000` | `00` | macro_id (u22)        | -            | Only used when bypassing macro evaluation.[^0xA0]               |
| REFILL                | `0xA8` | `10101` | `000` | `00` | -                     | -            | End of bytecode, reader must request refill of bytecode buffer  |
| END_TEMPLATE          | `0xB0` | `10110` | `000` | `00` | -                     | -            | End of template, return to caller.[^0xB0]                       |
| END_OF_INPUT          | `0xB1` | `10110` | `001` | `00` | -                     | -            | No more values; insufficient source data in the generator.      |
| END_CONTAINER         | `0xB2` | `10110` | `010` | `00` | -                     | -            | Delimits the end of a directive, list, sexp, or struct.         |
| META_OFFSET           | `0xB8` | `10111` | `000` | `01` |                       | offset (u32) | To support input >4GB, pack 22 high-order bits in "data".       |
| META_ROWCOL           | `0xB9` | `10111` | `001` | `01` | column (u22)          | row (u32)    | 0-based, row/col position                                       |
| META_COMMENT          | `0xBA` | `10111` | `010` | `01` | ref_length (u22)      | offset (u32) | Hypothetical.[^0xBA]                                            |

Possible TODOs:
 * Could we consolidate `END_TEMPLATE` and `END_OF_INPUT` into a single `RETURN` instruction?
 * OR rename `END_OF_INPUT` to `NEEDS_DATA`?
 * Consider renaming `REFILL` to `GENERATE_MORE_BYTECODE`
 * When we implement the BytecodeIonReader, we might find that we need to separate `END_CONTAINER` from the other end 
   types into its own operation kind since they have different conditions before you can resume them. You can theoretically
   call `next()` after an `END_OF_INPUT`, and more data might have arrived. However, you can never get more values after
   a `END_CONTAINER` without first calling `stepOut()`.

[^0x91]: There is no delimited end marker for the default value. If there is no default value, then bytecode_length=0.
[^0xA0]: Must be followed by value instructions for all parameters in macro signature. Absent arguments are represented
         with `ARGUMENT_NONE`.
[^0xB0]: Only used in the macro table—for marking the end of a template body
[^0xBA]: Potential inclusion to make it possible to expose comments from Ion text, or it could reference arbitrary data
         from inside a lengthy NOP. Comments that are longer than u22 max value could be encoded using multiple comment 
         instructions. The span should include the comment-delimiting characters.

## Integers

* All eagerly-read integer values MUST be encoded using the instruction with the smallest integer size in which the integer value can fit.
* The length value for an `INT_REF` instruction must include the sign bit.

## Exposing unevaluated macro invocations

* Generators may be configured to expose some or all macro invocations.
* Generators should expose macro invocations using the `INVOKE` instruction
* The `INVOKE` instruction is followed by one argument for each parameter in the signature (making any "no argument" expressions explicit).
* The arguments are followed by the `END_CONTAINER` instruction. (TODO: Should we have a distinct `END_INVOKE` instruction?)

## Directive Content

### `SET_SYMBOLS`, `ADD_SYMBOLS`

Child values are any `STRING_*` or `SYMBOL_*` instructions, each one representing a single symbol definition.
Unknown symbol text is denoted using the `SYMBOL_SID 0` instruction.

### `SET_MACROS`, `ADD_MACROS`

Child values are pairs of:
* a name (`SYMBOL_*`/`NULL_NULL`) 
* any instructions representing a single value.

Example:
```text
DIRECTIVE_ADD_MACROS
  SYMBOL_SID     $23
  FLOAT_F32
    3.1415
  SYMBOL_SID     $24
  INT_I16        42
END_CONTAINER
```

### `USE`

Content is triples of `name` (`STRING_*`), `version` (`INT_*`), `maxId` (`INT_*`/`NULL_NULL`).
For Ion 1.1 bytecode generators, `maxId` is always `NULL_NULL`.

### `MODULE`

Content is bytecode that is a 1:1 equivalent of the values and expressions used to define the module.

Example:
```text
DIRECTIVE_MODULE
  SEXP_START
    SYMBOL_CP 4    // "macros"
    SEXP_START
      SYMBOL_CP 5  // "macro"
      SYMBOL_CP 6  // "foo"
      // ...
```

### `IMPORT`

Content is triples of `bindingName` (`SYMBOL_*`), `catalogName` (`STRING_*`), and version (`INT_*`).

### `ENCODING`

Content is `SYMBOL_*` instructions, each one representing the name of a module.
