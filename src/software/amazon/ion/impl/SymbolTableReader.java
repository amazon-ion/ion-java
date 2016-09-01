/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl;


import static software.amazon.ion.SystemSymbols.IMPORTS;
import static software.amazon.ion.SystemSymbols.IMPORTS_SID;
import static software.amazon.ion.SystemSymbols.ION_SHARED_SYMBOL_TABLE;
import static software.amazon.ion.SystemSymbols.ION_SHARED_SYMBOL_TABLE_SID;
import static software.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static software.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static software.amazon.ion.SystemSymbols.MAX_ID;
import static software.amazon.ion.SystemSymbols.MAX_ID_SID;
import static software.amazon.ion.SystemSymbols.NAME;
import static software.amazon.ion.SystemSymbols.NAME_SID;
import static software.amazon.ion.SystemSymbols.SYMBOLS;
import static software.amazon.ion.SystemSymbols.SYMBOLS_SID;
import static software.amazon.ion.SystemSymbols.VERSION;
import static software.amazon.ion.SystemSymbols.VERSION_SID;
import static software.amazon.ion.impl.PrivateUtils.newSymbolToken;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import software.amazon.ion.Decimal;
import software.amazon.ion.IntegerSize;
import software.amazon.ion.IonException;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonType;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.Timestamp;

/**
 *   This is a reader that traverses a {@link SymbolTable}
 *   and returns the contents as if the table was serialized
 *   in Ion in a standard fashion (in fact this serialization
 *   defines the "standard fashion").
 *
 *   This does not support open content in symbol tables and
 *   should.  Support for open content will require additional
 *   data be kept in the {@link SymbolTable}, probably as an
 *   IonStruct or an IonReader or binary buffer.
 *
 *   The reader uses the _state member to track its progress
 *   through the members of the outer struct, the import
 *   list and the symbol list.  It used _idx to track its
 *   progress thought the elements of the import list and
 *   the elements of the symbol list.
 *
 *

    These are the states in a local symbol table:

        <S_BOF>
        <S_STRUCT>
        $ion_symbol_table:: {
          <S_IN_STRUCT>
          <S_MAX_ID>
            max_id:3,
          <S_SYMBOL_LIST>
            symbols:[
          <S_IN_SYMBOLS>
          <S_SYMBOL>
              "symbol1",
          <S_SYMBOL>
              "symbol2",
          <S_SYMBOL>
              "symbol3"
          <S_SYMBOL_LIST_CLOSE>
            ]
          <S_STRUCT_CLOSE>
        }
        <S_EOF>

    States for a shared symbol Table:

        <S_BOF>
        <S_STRUCT>
            $ion_shared_symbol_table::{
          <S_IN_STRUCT>
          <S_NAME>
              name:"mySharedTable",
          <S_VERSION>
              version:1,
          <S_MAX_ID>
              max_id:3,
          <S_IMPORT_LIST>
              imports:[
            <S_IN_IMPORTS>
            <S_IMPORT_STRUCT>
                {
              <S_IN_IMPORT_STRUCT>
              <S_IMPORT_NAME>
                  name:"otherTable",
              <S_IMPORT_VERSION>
                  version:2,
              <S_IMPORT_MAX_ID>
                  max_id:192
              <S_IMPORT_STRUCT_CLOSE>
                },
            <S_IMPORT_STRUCT>
                {
              <S_IN_IMPORT_STRUCT>
              <S_IMPORT_NAME>
                  name:"yetAnotherTable",
              <S_IMPORT_VERSION>
                  version:4,
              <S_IMPORT_MAX_ID>
                  max_id:34
              <S_IMPORT_STRUCT_CLOSE>
                },
            <S_IMPORT_LIST_CLOSE>
              ],
          <S_AFTER_IMPORT_LIST>  new
          <S_SYMBOL_LIST>
              symbols:[
            <S_IN_SYMBOLS>
            <S_SYMBOL>
                "symbol1",
            <S_SYMBOL>
                "symbol2",
            <S_SYMBOL>
                "symbol3"
            <S_SYMBOL_LIST_CLOSE>
              ]
          <S_STRUCT_CLOSE>
            }
        <S_EOF>


        Version 1.0 system symbol table:

        <S_BOF>
        <S_STRUCT>
            $ion_symbol_table::{
        <S_IN_STRUCT>
        <S_VERSION>
              version:1,
        <S_MAX_ID>
              max_id:9,
        <S_SYMBOL_LIST>
              symbols:[
        <S_IN_SYMBOLS>
        <S_SYMBOL>
            "$ion",
        <S_SYMBOL>
            "$ion_1_0",
        <S_SYMBOL>
            "$ion_symbol_table",
        <S_SYMBOL>
            "name",
        <S_SYMBOL>
            "version",
        <S_SYMBOL>
            "imports",
        <S_SYMBOL>
            "symbols",
        <S_SYMBOL>
            "max_id",
        <S_SYMBOL>
            "$ion_shared_symbol_table"
        <S_SYMBOL_LIST_CLOSE>
              ]
        <S_STRUCT_CLOSE>
            }
        <S_EOF>

 */

/*  --------------------------------------------------------------------------------

Notes regarding the various states that may be encountered.
The reader depth is in parenthesis after the name

state                      next()                  stepIn()                stepOut()                     container
------------------------   ---------------------   ---------------------   -------------------------     ---------
S_BOF(0)                 = S_STRUCT              | <error>                | <error>                    |           |

S_STRUCT(0)              = S_EOF                 | S_IN_STRUCT            | <error>                    | true      |

S_IN_STRUCT(1)           = S_NAME (load)         | <error>                | S_EOF                      |           |
            [if no name] : S_VERSION (load)      | <error>                | S_EOF                      |           |
             [if no ver] : S_MAX_ID (load)       | <error>                | S_EOF                      |           |

S_NAME(1)                = S_VERSION (load)      | <error>                | S_EOF                      |           |
             [if no ver] : S_MAX_ID (load)       | <error>                | S_EOF                      |           |

S_VERSION(1)             = S_MAX_ID (load)       | <error>                | S_EOF                      |           |

S_MAX_ID(1)              = S_IMPORT_LIST (load)  | <error>                | S_EOF                      |           |
         [if no imports] : S_SYMBOL_LIST         | <error>                | S_EOF                      |           |
          [if no locals] : S_EOF                 | <error>                | S_EOF                      |           |

S_IMPORT_LIST(1)         = S_SYMBOL_LIST (load)  | S_IN_IMPORTS           | S_EOF                      | true      |
          [if no locals] : S_EOF                 | <error>                | S_EOF                      |           |

S_IN_IMPORTS(2)          = S_IMPORT_STRUCT       | <error>                | S_AFTER_IMPORT_LIST (load) |           |
    [if no more imports] : S_EOF                 | <error>                | S_AFTER_IMPORT_LIST (load) |           |
          [if no locals] : -------               | -------                | S_STRUCT_CLOSE             |           |

S_IMPORT_STRUCT(2)       = S_IMPORT_STRUCT       | S_IN_IMPORT_STRUCT     | S_AFTER_IMPORT_LIST (load) | true      |
    [if no more imports] : S_IMPORT_STRUCT_CLOSE | <error>                | S_AFTER_IMPORT_LIST (load) |           |
          [if no locals] : -------               | -------                | S_STRUCT_CLOSE             |           |

S_IN_IMPORT_STRUCT(3)    = S_IMPORT_NAME (load)  | <error>                | S_IMPORT_STRUCT            |           |
    [if no more imports] : -------               | -------                | S_IMPORT_STRUCT_CLOSE      |           |

S_IMPORT_NAME(3)         = S_IMPORT_VERSION(load)| <error>                | S_IMPORT_STRUCT            |           |
    [if no more imports] : -------               | -------                | S_IMPORT_STRUCT_CLOSE      |           |

S_IMPORT_VERSION(3)      = S_IMPORT_MAX_ID (load)| <error>                | S_IMPORT_STRUCT            |           |
    [if no more imports] : -------               | -------                | S_IMPORT_STRUCT_CLOSE      |           |

S_IMPORT_MAX_ID(3)       = S_IMPORT_STRUCT_CLOSE | <error>                | S_IMPORT_STRUCT            |           |
    [if no more imports] : -------               | -------                | S_IMPORT_STRUCT_CLOSE      |           |

S_IMPORT_STRUCT_CLOSE(3) = S_IMPORT_STRUCT_CLOSE | <error>                | S_IMPORT_STRUCT            |           |
    [if no more imports] : -------               | -------                | S_STRUCT_CLOSE             |           |

S_IMPORT_LIST_CLOSE(2)   = S_IMPORT_LIST_CLOSE   | <error>                | S_AFTER_IMPORT_LIST (load) |           |
          [if no locals] : S_IMPORT_LIST_CLOSE   | <error>                | S_STRUCT_CLOSE             |           |

S_AFTER_IMPORT_LIST(1)   = S_SYMBOL_LIST         | <error>                | S_EOF                      |           |

S_SYMBOL_LIST(1)         = S_STRUCT_CLOSE        | S_IN_SYMBOLS           | S_EOF                      | true      |
          [if no locals] : -------               | S_SYMBOL_LIST_CLOSE    | S_EOF                      |           |

S_IN_SYMBOLS(2)          = S_SYMBOL (load)       | <error>                | S_STRUCT_CLOSE             |           |
     [if no more locals] : S_SYMBOL_LIST_CLOSE   | <error>                | S_STRUCT_CLOSE             |           |

S_SYMBOL(2)              = S_SYMBOL (load)       | <error>                | S_STRUCT_CLOSE             |           |
     [if no more locals] : S_SYMBOL_LIST_CLOSE   | <error>                | S_STRUCT_CLOSE             |           |

S_SYMBOL_LIST_CLOSE(2)   = S_EOF                 | <error>                | S_STRUCT_CLOSE             |           |

S_STRUCT_CLOSE(1)        = S_EOF                 | <error>                | S_EOF                      |           |
S_EOF(0)                 = S_EOF                 | <error>                | S_EOF                      |           |


Same list sorted by depth (i.e. by the various sequences that
the reader progresses through to a local or global eof)

state
------------------------
S_BOF(0)                   system
S_STRUCT(0)                required
S_EOF(0)                   system


S_IN_STRUCT(1)             system
S_NAME(1)                  optional   if name not null (or if isLocalSymbolTable == false)
S_VERSION(1)               optional   if version > 0  (or if isLocalSymbolTable == false)
S_MAX_ID(1)                optional   if max_id > 0
S_IMPORT_LIST(1)           optional   if import count > 0
S_AFTER_IMPORT_LIST(1)     optional   if import count > 0 && local count > 0 (this state exists only after stepOut of the import list)
S_SYMBOL_LIST(1)           optional:  if local count > 0
S_STRUCT_CLOSE(1)          system


S_IN_IMPORTS(2)            system
S_IMPORT_STRUCT(2)         variable   if _imports.hasNext() (import symbol table iterator)
S_IMPORT_LIST_CLOSE(2)     system


S_IN_IMPORT_STRUCT(3)      system
S_IMPORT_NAME(3)           required
S_IMPORT_VERSION(3)        required
S_IMPORT_MAX_ID(3)         required
S_IMPORT_STRUCT_CLOSE(3)   system


S_IN_SYMBOLS(2)            system
S_SYMBOL(2)                variable  if _symbols.hasNext() (local symbol iterator)
S_SYMBOL_LIST_CLOSE(2)     system

*/


final class SymbolTableReader
    implements IonReader
{

    /**
     * these are the states of the reader.
     * Each state tells the reader it is
     * just before (or on as you might want
     * to think about it) one of the possible
     * values in the serialized image.
     */
    static final int S_BOF                  =  0;
    static final int S_STRUCT               =  1;
    static final int S_IN_STRUCT            =  2;
    static final int S_NAME                 =  3;
    static final int S_VERSION              =  4;
    static final int S_MAX_ID               =  5;
    static final int S_IMPORT_LIST          =  6;
    static final int S_IN_IMPORTS           =  7;
    static final int S_IMPORT_STRUCT        =  8;
    static final int S_IN_IMPORT_STRUCT     =  9;
    static final int S_IMPORT_NAME          = 10;
    static final int S_IMPORT_VERSION       = 11;
    static final int S_IMPORT_MAX_ID        = 12;
    static final int S_IMPORT_STRUCT_CLOSE  = 13;
    static final int S_IMPORT_LIST_CLOSE    = 14;
    static final int S_AFTER_IMPORT_LIST    = 15;
    static final int S_SYMBOL_LIST          = 16;
    static final int S_IN_SYMBOLS           = 17;
    static final int S_SYMBOL               = 18;
    static final int S_SYMBOL_LIST_CLOSE    = 19;
    static final int S_STRUCT_CLOSE         = 20;
    static final int S_EOF                  = 21;

    private final static String get_state_name(int state)
    {
        switch (state) {
        case S_BOF:                     return "S_BOF";
        case S_STRUCT:                  return "S_STRUCT";
        case S_IN_STRUCT:               return "S_IN_STRUCT";
        case S_NAME:                    return "S_NAME";
        case S_VERSION:                 return "S_VERSION";
        case S_MAX_ID:                  return "S_MAX_ID";
        case S_IMPORT_LIST:             return "S_IMPORT_LIST";
        case S_IN_IMPORTS:              return "S_IN_IMPORTS";
        case S_IMPORT_STRUCT:           return "S_IMPORT_STRUCT";
        case S_IN_IMPORT_STRUCT:        return "S_IN_IMPORT_STRUCT";
        case S_IMPORT_NAME:             return "S_IMPORT_NAME";
        case S_IMPORT_VERSION:          return "S_IMPORT_VERSION";
        case S_IMPORT_MAX_ID:           return "S_IMPORT_MAX_ID";
        case S_IMPORT_STRUCT_CLOSE:     return "S_IMPORT_STRUCT_CLOSE";
        case S_IMPORT_LIST_CLOSE:       return "S_IMPORT_LIST_CLOSE";
        case S_AFTER_IMPORT_LIST:       return "S_AFTER_IMPORT_LIST";
        case S_SYMBOL_LIST:             return "S_SYMBOL_LIST";
        case S_IN_SYMBOLS:              return "S_IN_SYMBOLS";
        case S_SYMBOL:                  return "S_SYMBOL";
        case S_SYMBOL_LIST_CLOSE:       return "S_SYMBOL_LIST_CLOSE";
        case S_STRUCT_CLOSE:            return "S_STRUCT_CLOSE";
        case S_EOF:                     return "S_EOF";
        default:                        return "<Unrecognized state: "+state+">";
        }
    }

    static final IonType stateType(int state)
    {
        switch (state)
        {
        case S_BOF:                  return null;
        case S_STRUCT:               return IonType.STRUCT;
        case S_IN_STRUCT:            return null;
        case S_NAME:                 return IonType.STRING;
        case S_VERSION:              return IonType.INT;
        case S_MAX_ID:               return IonType.INT;
        case S_IMPORT_LIST:          return IonType.LIST;
        case S_IN_IMPORTS:           return null;
        case S_IMPORT_STRUCT:        return IonType.STRUCT;
        case S_IN_IMPORT_STRUCT:     return null;
        case S_IMPORT_NAME:          return IonType.STRING;
        case S_IMPORT_VERSION:       return IonType.INT;
        case S_IMPORT_MAX_ID:        return IonType.INT;
        case S_IMPORT_STRUCT_CLOSE:  return null;
        case S_IMPORT_LIST_CLOSE:    return null;
        case S_AFTER_IMPORT_LIST:    return null;
        case S_SYMBOL_LIST:          return IonType.LIST;
        case S_IN_SYMBOLS:           return null;
        case S_SYMBOL:               return IonType.STRING;
        case S_SYMBOL_LIST_CLOSE:    return null;
        case S_STRUCT_CLOSE:         return null;
        case S_EOF:                  return null;
        default:
            throwUnrecognizedState(state);
            return null;
        }
    }

    static final int stateDepth(int state)
    {
        switch (state)
        {
        case S_BOF:                  return 0;
        case S_STRUCT:               return 0;
        case S_IN_STRUCT:            return 1;
        case S_NAME:                 return 1;
        case S_VERSION:              return 1;
        case S_MAX_ID:               return 1;
        case S_IMPORT_LIST:          return 1;
        case S_IN_IMPORTS:           return 2;
        case S_IMPORT_STRUCT:        return 2;
        case S_IN_IMPORT_STRUCT:     return 3;
        case S_IMPORT_NAME:          return 3;
        case S_IMPORT_VERSION:       return 3;
        case S_IMPORT_MAX_ID:        return 3;
        case S_IMPORT_STRUCT_CLOSE:  return 3;
        case S_IMPORT_LIST_CLOSE:    return 2;
        case S_AFTER_IMPORT_LIST:    return 1;
        case S_SYMBOL_LIST:          return 1;
        case S_IN_SYMBOLS:           return 2;
        case S_SYMBOL:               return 2;
        case S_SYMBOL_LIST_CLOSE:    return 2;
        case S_STRUCT_CLOSE:         return 1;
        case S_EOF:                  return 0;
        default:
            throwUnrecognizedState(state);
            return -1;
        }
    }


    /**
     * these define the bit masked flags used
     * to inform iteration over the optional
     * values that may be present
     */
    private static final int HAS_NAME           = 0x01;
    private static final int HAS_VERSION        = 0x02;
    private static final int HAS_MAX_ID         = 0x04;
    private static final int HAS_IMPORT_LIST    = 0x08;
    private static final int HAS_SYMBOL_LIST    = 0x10;


    /**
     * The symbol table we are reading.
     *
     * We MUST NOT call methods whose value may change! Otherwise there's a
     * thread-safety problem.
     */
    private final SymbolTable _symbol_table;

    private final int _maxId;

    /**
     * _state tracks the progress through the various
     * members, including the substructures in the
     * import list and symbol list.  The previous state
     * is needed when we run into an end of container.
     */
    int _current_state  = S_BOF;

    /**
     * state variables that hold the values referenced
     * by the current reader position.  These are filled
     * in by nextStateDepthFirst() and used by the value
     * getting methods.
     *
     * The iterators are opened when the user steps into
     * one of the two lists (imports and symbols).
     *
     */
    int                          _flags;                  // has name, has ... optional top level values
    String                       _string_value;
    long                         _int_value;
    private SymbolTable[]        _imported_tables;
    private Iterator<SymbolTable> _import_iterator;
    private SymbolTable          _current_import;
    Iterator<String>             _local_symbols;

    public SymbolTableReader(SymbolTable symbol_table)
    {
        _symbol_table = symbol_table;

        synchronized (symbol_table)
        {
            _maxId = symbol_table.getMaxId();
            _local_symbols = symbol_table.iterateDeclaredSymbolNames();
        }

        if (symbol_table.isLocalTable() == false) {
            set_flag(HAS_NAME, true);
            set_flag(HAS_VERSION, true);
        }
        if (_maxId > 0) {
            // FIXME: is this ever true?            set_flag(HAS_MAX_ID, true);
        }
        _imported_tables = _symbol_table.getImportedTables();
        if (_imported_tables != null && _imported_tables.length != 0) {
            set_flag(HAS_IMPORT_LIST, true);
        }
        if (_symbol_table.getImportedMaxId() < _maxId) {
            set_flag(HAS_SYMBOL_LIST, true);
        }
    }


    /**
     * @return This implementation always returns null.
     */
    public <T> T asFacet(Class<T> facetType)
    {
        return null;
    }

    //========================================================================


    private final void set_flag(int flag_bit, boolean flag_state)
    {
        if (flag_state) {
            _flags |= flag_bit;
        }
        else {
            _flags &= ~flag_bit;
        }
    }

    private final boolean test_flag(int flag_bit)
    {
        boolean flag_state = (_flags & flag_bit) != 0;
        return flag_state;
    }

    final boolean hasName() {
        boolean flag_state = test_flag(HAS_NAME);
        return flag_state;
    }

    final boolean hasVersion() {
        boolean flag_state = test_flag(HAS_VERSION);
        return flag_state;
    }

    final boolean hasMaxId() {
        boolean flag_state = test_flag(HAS_MAX_ID);
        return flag_state;
    }

    final boolean hasImports() {
        boolean flag_state = test_flag(HAS_IMPORT_LIST);
        return flag_state;
    }

    final boolean hasLocalSymbols() {
        boolean flag_state = test_flag(HAS_SYMBOL_LIST);
        return flag_state;
    }

    private final boolean has_next()
    {
        // this just tells us whether or not we have more
        // value coming at our current scanning depth
        switch (_current_state)
        {
        case S_BOF:
            // outer struct always follows
            return true;

        case S_STRUCT:
            // only top level value
            return false;

        case S_IN_STRUCT:
            if (stateFirstInStruct() != S_STRUCT_CLOSE) {
                return true;
            }
            return false;

        case S_NAME:
            // if we have name we have version
            return true;

        case S_VERSION:
            if (hasMaxId()) {
                return true;
            }
            if (stateFollowingMaxId() != S_STRUCT_CLOSE) {
                return true;
            }
            return false;

        case S_MAX_ID:
            // maybe something follows, but not always
            if (stateFollowingMaxId() != S_STRUCT_CLOSE) {
                return true;
            }
            return false;

        case S_IMPORT_LIST:
            // locals are the only thing that might follow imports
            if (hasLocalSymbols())  return true;
            return false;

        case S_IN_IMPORTS:
        case S_IMPORT_STRUCT:
            // we have more if there is
            boolean more_imports = _import_iterator.hasNext();
            return more_imports;

        case S_IN_IMPORT_STRUCT:
        case S_IMPORT_NAME:
            // we always have a name and version
            return true;

        case S_IMPORT_VERSION:
            // we always have a max_id on imports
            return true;

        case S_IMPORT_MAX_ID:
        case S_IMPORT_STRUCT_CLOSE:
            return false;

        case S_IMPORT_LIST_CLOSE:
            return false;

        case S_AFTER_IMPORT_LIST:
            // locals are the only thing that might follow imports
            if (hasLocalSymbols())  return true;
            return false;

        case S_SYMBOL_LIST:
            // the symbol list is the last member, so it has no "next sibling"
            // but ... just in case we put something after the local symbol list
            assert(stateFollowingLocalSymbols() == S_STRUCT_CLOSE);
            return false;

        case S_IN_SYMBOLS:
        case S_SYMBOL:
            if (_local_symbols.hasNext()) return true;
            return false;

        case S_SYMBOL_LIST_CLOSE:
        case S_STRUCT_CLOSE:
        case S_EOF:
            // these are all at the end of their respective containers
            return false;

        default:
            throwUnrecognizedState(_current_state);
            return false;
        }
    }

    private final static void throwUnrecognizedState(int state)
    {
        String message = "Internal error: "
            + "UnifiedSymbolTableReader"
            + " is in an unrecognized state: "
            + get_state_name(state);
        throw new IonException(message);
    }


    // helpers to resolve the existence of a number
    // "follow" states where the answer depends on the
    // details of the symbol table we're reading.

    private final int stateFirstInStruct()
    {
        int new_state;

        if (hasName()) {
            new_state = S_NAME;
        }
        else if (hasMaxId()) {
            new_state = S_MAX_ID;
        }
        else if (hasImports()) {
            new_state = S_IMPORT_LIST;
        }
        else if (hasLocalSymbols()) {
            new_state = S_SYMBOL_LIST;
        }
        else {
            new_state = S_STRUCT_CLOSE;
        }
        return new_state;
    }

    private final int stateFollowingMaxId()
    {
        int new_state;

        if (hasImports()) {
            new_state = S_IMPORT_LIST;
        }
        else if (hasLocalSymbols()) {
            new_state = S_SYMBOL_LIST;
        }
        else {
            new_state = S_STRUCT_CLOSE;
        }
        return new_state;
    }

    private final int nextImport()
    {
        assert(_import_iterator != null);

        int new_state;
        if (_import_iterator.hasNext()) {
            _current_import = _import_iterator.next();
            new_state = S_IMPORT_STRUCT;
        }
        else {
            // the import list is empty, so we jump to
            // the close list and null out our current
            _current_import = null;
            new_state = S_IMPORT_LIST_CLOSE;
        }
        return new_state;
    }

    private static enum Op {NEXT, STEPOUT}
    private final int stateFollowingImportList(Op op)
    {
        int new_state = -1;

        if (hasLocalSymbols()) {
            switch (op) {
            case NEXT:
                new_state = S_SYMBOL_LIST;
                break;
            case STEPOUT:
                new_state = S_AFTER_IMPORT_LIST;
                break;
            }
        }
        else {
            new_state = S_STRUCT_CLOSE;
        }
        return new_state;
    }

    private final int stateFollowingLocalSymbols()
    {
        return S_STRUCT_CLOSE;
    }

    /**
     * this computes the actual move to the next state
     *
     * It does take into account the existence or absence
     * of various properties, such as the symbol table
     * name which is only present in some symbol tables.
     *
     * This also fills in a variety of current state
     * variables that are used to return correct values
     * by the "get" methods.
     *
     */
    public IonType next()
    {
        if (has_next() == false) {
            return null;
        }
        int new_state;

        switch (_current_state)
        {
        case S_BOF:
            new_state = S_STRUCT;
            break;

        case S_STRUCT:
            new_state = S_EOF;
            break;

        case S_IN_STRUCT:
            new_state = stateFirstInStruct();
            loadStateData(new_state);
            break;

        case S_NAME:
            assert(hasVersion());
            new_state = S_VERSION;
            loadStateData(new_state);
            break;

        case S_VERSION:
            if (hasMaxId()) {
                new_state = S_MAX_ID;
                loadStateData(new_state);
            }
            else {
                new_state = stateFollowingMaxId();
            }
            break;

        case S_MAX_ID:
            new_state = stateFollowingMaxId();
            break;

        case S_IMPORT_LIST:
            new_state = this.stateFollowingImportList(Op.NEXT);
            break;

        case S_IN_IMPORTS:
        case S_IMPORT_STRUCT:
            // we only need to get the import list once, which we
            // do as we step into the import list, so it should
            // be waiting for us here.
            assert(_import_iterator != null);
            new_state = nextImport();
            break;

        case S_IN_IMPORT_STRUCT:
            // shared tables have to have a name
            new_state = S_IMPORT_NAME;
            loadStateData(new_state);
            break;

        case S_IMPORT_NAME:
            // shared tables have to have a version
            new_state = S_IMPORT_VERSION;
            loadStateData(new_state);
            break;

        case S_IMPORT_VERSION:
            // and they also always have a max id - so we set up
            // for it
            new_state = S_IMPORT_MAX_ID;
            loadStateData(new_state);
            break;

        case S_IMPORT_MAX_ID:
            new_state = S_IMPORT_STRUCT_CLOSE;
            break;

        case S_IMPORT_STRUCT_CLOSE:
            // no change here - we just bump up against this local eof
            new_state = S_IMPORT_STRUCT_CLOSE;
            break;

        case S_IMPORT_LIST_CLOSE:
            // no change here - we just bump up against this local eof
            new_state = S_IMPORT_LIST_CLOSE;
            break;

        case S_AFTER_IMPORT_LIST:
            assert(_symbol_table.getImportedMaxId() < _maxId);
            new_state = S_SYMBOL_LIST;
            break;

        case S_SYMBOL_LIST:
            assert(_symbol_table.getImportedMaxId() < _maxId);
            new_state = stateFollowingLocalSymbols();
            break;

        case S_IN_SYMBOLS:
            // we have some symbols - so we'll set up to read them,
            // which we *have* to do once and *need* to do only once.
            assert(_local_symbols != null);
            // since we only get into the symbol list if
            // there are some symbols - our next state
            // is at the first symbol
            assert(_local_symbols.hasNext() == true);
            // so we just fall through to and let the S_SYMBOL
            // state do it's thing (which it will do every time
            // we move to the next symbol)
        case S_SYMBOL:
            if (_local_symbols.hasNext())
            {
                _string_value = _local_symbols.next();
                // null means this symbol isn't defined
                new_state = S_SYMBOL;
            }
            else {
                new_state = S_SYMBOL_LIST_CLOSE;
            }
            break;

        case S_SYMBOL_LIST_CLOSE:
            // no change here - we just bump up against this local eof
            new_state = S_SYMBOL_LIST_CLOSE;
            break;

        case S_STRUCT_CLOSE:
            // no change here - we just bump up against this local eof
            new_state = S_STRUCT_CLOSE;
            break;

        case S_EOF:
            new_state = S_EOF;
            break;

        default:
            throwUnrecognizedState(_current_state);
            new_state = -1;
            break;
        }

        _current_state = new_state;
        return stateType(_current_state);
    }


    private final void loadStateData(int new_state)
    {
        switch(new_state) {
        case S_NAME:
            assert(hasName());
            String name = _symbol_table.getName();
            _string_value = name;
            assert(_string_value != null);
            break;

        case S_VERSION:
            int value = _symbol_table.getVersion();
            _int_value = value;
            assert(value != 0);
            break;

        case S_MAX_ID:
            _int_value = _maxId;
            break;

        case S_IMPORT_LIST:
        case S_SYMBOL_LIST:
            // no op to simplify the initial fields logic in next()
            break;

        case S_IMPORT_NAME:
            assert(_current_import != null);
            _string_value = _current_import.getName();
            break;

        case S_IMPORT_VERSION:
            // shared tables have to have a version
            _string_value = null;
            _int_value = _current_import.getVersion();
            break;

        case S_IMPORT_MAX_ID:
            // and they also always have a max id - so we set up
            // for it
            _int_value = _current_import.getMaxId();
            break;

        default:
            String message = "UnifiedSymbolTableReader in state "
                           + SymbolTableReader.get_state_name(new_state)
                           + " has no state to load.";
            throw new IonException(message);
        }
    }

    public void stepIn()
    {
        int new_state;

        switch (_current_state) {
        case S_STRUCT:
            new_state = S_IN_STRUCT;
            break;
        case S_IMPORT_LIST:
            _import_iterator = Arrays.asList(_imported_tables).iterator();
            new_state = S_IN_IMPORTS;
            break;
        case S_IMPORT_STRUCT:
            assert(_current_import != null);
            new_state = S_IN_IMPORT_STRUCT;
            break;
        case S_SYMBOL_LIST:
            new_state = S_IN_SYMBOLS;
            break;
        default:
            throw new IllegalStateException("current value is not a container");
        }

        _current_state = new_state;
        return;
    }

    public void stepOut()
    {
        int new_state = -1;

        switch (_current_state) {
            case S_IN_STRUCT:
            case S_NAME:
            case S_VERSION:
            case S_MAX_ID:
            case S_IMPORT_LIST:
            case S_AFTER_IMPORT_LIST:
            case S_SYMBOL_LIST:
            case S_STRUCT_CLOSE:
                // these are all top level so stepOut()
                // ends up at the end of our data
                new_state = S_EOF;
                break;

            case S_IN_IMPORTS:
            case S_IMPORT_STRUCT:
            case S_IMPORT_LIST_CLOSE:
                // if we're outside a struct, and we're in the import
                // list stepOut will be whatever follows the import list
                // close and we're done with these
                _current_import = null;
                _import_iterator = null;
                new_state = stateFollowingImportList(Op.STEPOUT);
                break;

            case S_IN_IMPORT_STRUCT:
            case S_IMPORT_NAME:
            case S_IMPORT_VERSION:
            case S_IMPORT_MAX_ID:
            case S_IMPORT_STRUCT_CLOSE:
                // if there is a next import the next state
                // will be its struct open
                // otherwise next will be the list close
                if (_import_iterator.hasNext()) {
                    new_state = S_IMPORT_STRUCT;
                }
                else {
                    new_state = S_IMPORT_LIST_CLOSE;
                }
                break;

            case S_IN_SYMBOLS:
            case S_SYMBOL:
            case S_SYMBOL_LIST_CLOSE:
                // I think this is just S_EOF, but if we ever
                // put anything after the symbol list this
                // will need to be updated.  And we're done
                // with our local symbol references.
                _string_value = null;
                _local_symbols = null;
                new_state = stateFollowingLocalSymbols();
                break;

            default:
                throw new IllegalStateException("current value is not in a container");
        }

        _current_state = new_state;
        return;
    }

    public int getDepth()
    {
        return stateDepth(_current_state);
    }

    public SymbolTable getSymbolTable()
    {
        // TODO: this should return a system symbol table
        //       but, for now, we don't really know which
        //       one to return;
        //
        // UnifiedSymbolTable.makeSystemSymbolTable(_sys, _version);
        //
        // although, with this reader in place we should be
        // able to cons one up without a system
        return null;
    }

    public IonType getType()
    {
        return stateType(_current_state);
    }


    public String[] getTypeAnnotations()
    {
        if (_current_state == S_STRUCT) {
            // Must return a new array each time to prevent user from changing it
            if (_symbol_table.isLocalTable() || _symbol_table.isSystemTable())
            {
                return new String[] { ION_SYMBOL_TABLE };
            }
            return new String[] { ION_SHARED_SYMBOL_TABLE };
        }
        return PrivateUtils.EMPTY_STRING_ARRAY;
    }

    private static final SymbolToken ION_SYMBOL_TABLE_SYM =
        newSymbolToken(ION_SYMBOL_TABLE, ION_SYMBOL_TABLE_SID);

    private static final SymbolToken ION_SHARED_SYMBOL_TABLE_SYM =
        newSymbolToken(ION_SHARED_SYMBOL_TABLE, ION_SHARED_SYMBOL_TABLE_SID);

    public SymbolToken[] getTypeAnnotationSymbols()
    {
        if (_current_state == S_STRUCT) {
            SymbolToken sym;
            if (_symbol_table.isLocalTable() || _symbol_table.isSystemTable())
            {
                sym = ION_SYMBOL_TABLE_SYM;
            }
            else
            {
                sym = ION_SHARED_SYMBOL_TABLE_SYM;
            }

            // Must return a new array each time to prevent user from changing it
            return new SymbolToken[] { sym };
        }
        return SymbolToken.EMPTY_ARRAY;
    }


    public Iterator<String> iterateTypeAnnotations()
    {
        String[] annotations = getTypeAnnotations();
        return PrivateUtils.stringIterator(annotations);
    }

    public String getFieldName()
    {
        switch (_current_state)
        {
        case S_STRUCT:
        case S_IN_STRUCT:
        case S_IN_IMPORTS:
        case S_IMPORT_STRUCT:
        case S_IN_IMPORT_STRUCT:
        case S_IMPORT_STRUCT_CLOSE:
        case S_IMPORT_LIST_CLOSE:
        case S_AFTER_IMPORT_LIST:
        case S_IN_SYMBOLS:
        case S_SYMBOL:
        case S_SYMBOL_LIST_CLOSE:
        case S_STRUCT_CLOSE:
        case S_EOF:
            return null;

        case S_NAME:
        case S_IMPORT_NAME:
            return NAME;

        case S_VERSION:
        case S_IMPORT_VERSION:
            return VERSION;

        case S_MAX_ID:
        case S_IMPORT_MAX_ID:
            return MAX_ID;

        case S_IMPORT_LIST:
            return IMPORTS;

        case S_SYMBOL_LIST:
            return SYMBOLS;

        default:
            throw new IonException("Internal error: UnifiedSymbolTableReader is in an unrecognized state: "+_current_state);
        }
    }

    public SymbolToken getFieldNameSymbol()
    {
        switch (_current_state)
        {
        case S_STRUCT:
        case S_IN_STRUCT:
        case S_IN_IMPORTS:
        case S_IMPORT_STRUCT:
        case S_IN_IMPORT_STRUCT:
        case S_IMPORT_STRUCT_CLOSE:
        case S_IMPORT_LIST_CLOSE:
        case S_AFTER_IMPORT_LIST:
        case S_IN_SYMBOLS:
        case S_SYMBOL:
        case S_SYMBOL_LIST_CLOSE:
        case S_STRUCT_CLOSE:
        case S_EOF:
            return null;

        case S_NAME:
        case S_IMPORT_NAME:
            return new SymbolTokenImpl(NAME, NAME_SID);

        case S_VERSION:
        case S_IMPORT_VERSION:
            return new SymbolTokenImpl(VERSION, VERSION_SID);

        case S_MAX_ID:
        case S_IMPORT_MAX_ID:
            return new SymbolTokenImpl(MAX_ID, MAX_ID_SID);

        case S_IMPORT_LIST:
            return new SymbolTokenImpl(IMPORTS, IMPORTS_SID);

        case S_SYMBOL_LIST:
            return new SymbolTokenImpl(SYMBOLS, SYMBOLS_SID);

        default:
            throw new IonException("Internal error: UnifiedSymbolTableReader is in an unrecognized state: "+_current_state);
        }
    }

    public boolean isNullValue()
    {
        switch (_current_state)
        {
        case S_STRUCT:
        case S_IN_STRUCT:
        case S_NAME:
        case S_VERSION:
        case S_MAX_ID:
        case S_IMPORT_LIST:
        case S_IN_IMPORTS:
        case S_IMPORT_STRUCT:
        case S_IN_IMPORT_STRUCT:
        case S_IMPORT_NAME:
        case S_IMPORT_VERSION:
        case S_IMPORT_MAX_ID:
        case S_IN_SYMBOLS:
        case S_SYMBOL:
            // these values are either present and non-null
            // or entirely absent (in which case they will
            // have been skipped and we won't be in a state
            // to return them).
            return false;

        case S_IMPORT_STRUCT_CLOSE:
        case S_IMPORT_LIST_CLOSE:
        case S_AFTER_IMPORT_LIST:
        case S_SYMBOL_LIST:
        case S_SYMBOL_LIST_CLOSE:
        case S_STRUCT_CLOSE:
        case S_EOF:
            // here we're not really on a value, so we're not
            // on a value that is a null - so false again.
            return false;

        default:
            throw new IonException("Internal error: UnifiedSymbolTableReader is in an unrecognized state: "+_current_state);
        }
    }

    public boolean isInStruct()
    {
        switch (_current_state)
        {
        case S_STRUCT:
        case S_IN_IMPORTS:
        case S_IMPORT_STRUCT:
        case S_IN_SYMBOLS:
        case S_SYMBOL:
            // these values are either not contained, or
            // contained in a list.  So we aren't in a
            // struct if they're pending.
            return false;

        case S_IN_STRUCT:
        case S_NAME:
        case S_VERSION:
        case S_MAX_ID:
        case S_IMPORT_LIST:
        case S_IN_IMPORT_STRUCT:
        case S_IMPORT_NAME:
        case S_IMPORT_VERSION:
        case S_IMPORT_MAX_ID:
        case S_AFTER_IMPORT_LIST:
        case S_SYMBOL_LIST:
            // the values above are all members
            // of a struct, so we must be in a
            // struct to have them pending
            return true;

        case S_IMPORT_STRUCT_CLOSE:
        case S_STRUCT_CLOSE:
            // if we're closing a struct we're in a struct
            return true;

        case S_IMPORT_LIST_CLOSE:
        case S_SYMBOL_LIST_CLOSE:
        case S_EOF:
            // if we're closing a list we in a list, not a struct
            // and EOF is not in a struct
            return false;

        default:
            throw new IonException("Internal error: UnifiedSymbolTableReader is in an unrecognized state: "+_current_state);
        }
    }

    public boolean booleanValue()
    {
        throw new IllegalStateException("only valid if the value is a boolean");
    }

    public int intValue()
    {
        return (int)_int_value;
    }

    public long longValue()
    {
        return _int_value;
    }

    public BigInteger bigIntegerValue()
    {
        String value = Long .toString(_int_value);
        BigInteger bi = new BigInteger(value);     // this is SOOOOO broken <sigh>
        return bi;
    }

    public double doubleValue()
    {
        throw new IllegalStateException("only valid if the value is a double");
    }

    public BigDecimal bigDecimalValue()
    {
        throw new IllegalStateException("only valid if the value is a decimal");    }

    public Decimal decimalValue()
    {
        throw new IllegalStateException("only valid if the value is a decimal");
    }

    public Date dateValue()
    {
        throw new IllegalStateException("only valid if the value is a timestamp");
    }

    public Timestamp timestampValue()
    {
        throw new IllegalStateException("only valid if the value is a timestamp");
    }

    public String stringValue()
    {
        return _string_value;
    }

    public SymbolToken symbolValue()
    {
        // TODO handle null
        throw new UnsupportedOperationException();
    }

    public int getBytes(byte[] buffer, int offset, int len)
    {
        throw new IllegalStateException("getBytes() is only valid if the reader is on a lob value, not a "+stateType(_current_state)+" value");
    }

    public int byteSize()
    {
        throw new IllegalStateException("byteSize() is only valid if the reader is on a lob value, not a "+stateType(_current_state)+" value");
    }

    public byte[] newBytes()
    {
        throw new IllegalStateException("newBytes() is only valid if the reader is on a lob value, not a "+stateType(_current_state)+" value");
    }

    public void close() throws IOException
    {
        _current_state = S_EOF;
    }

    public IntegerSize getIntegerSize()
    {
        if (stateType(_current_state) != IonType.INT)
        {
            return null;
        }
        return IntegerSize.INT; // all of SymbolTable's integers are type int
    }
}
