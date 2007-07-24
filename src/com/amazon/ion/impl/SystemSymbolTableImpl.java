/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.InvalidSystemSymbolException;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SystemSymbolTable;

/**
 *
 */
public class SystemSymbolTableImpl
    implements SystemSymbolTable
{
    private static final String[] SYSTEM_SYMBOLS =
    {
        ION,
        ION_1_0,
        ION_SYMBOL_TABLE,
        "name",
        "version",
        "imports",
        "symbols",
        "max_id"
    };

    public static final int ION_1_0_SID = 2;

    static
    {
        // REMEMBER: sids are one-based, not zero-based!
        assert ION_1_0.equals(SYSTEM_SYMBOLS[ION_1_0_SID - 1]);
    }


    /**
     * Generate the string representation of a symbol with an unknown id.
     * @param id must be a value greater than zero.
     * @return the symbol name, of the form <code>$NNN</code> where NNN is the
     * integer rendering of <code>id</code>
     */
    public static String unknownSymbolName(int id)
    {
        return "$" + id;
    }


    //=========================================================================


    public SystemSymbolTableImpl()
    {
    }

    public String getSystemId()
    {
        return ION_1_0;
    }


    public int findSymbol(String name)
    {
        for (int i = 0; i < SYSTEM_SYMBOLS.length; i++)
        {
            if (name.equals(SYSTEM_SYMBOLS[i]))
            {
                return i + 1;
            }
        }

        if (name.charAt(0) == '$')
        {
            String sidText = name.substring(1);
            try
            {
                int sid = Integer.parseInt(sidText);
                if (sid > 0) return sid;
                // else fall through
            }
            catch (NumberFormatException e)
            {
                if (name.startsWith(ION_RESERVED_PREFIX))
                {
                    throw new InvalidSystemSymbolException(name);
                }
                // else fall through
            }
        }

        return IonSymbol.UNKNOWN_SYMBOL_ID;
    }

    public String findKnownSymbol(int id)
    {
        if (id <= SYSTEM_SYMBOLS.length) {
            return SYSTEM_SYMBOLS[id - 1];
        }

        return null;
    }

    public String findSymbol(int id)
    {
        if (id <= SYSTEM_SYMBOLS.length) {
            return SYSTEM_SYMBOLS[id - 1];
        }

        return SystemSymbolTableImpl.unknownSymbolName(id);
    }

    public int getMaxId()
    {
        return SYSTEM_SYMBOLS.length;
    }


    public int size()
    {
        return SYSTEM_SYMBOLS.length;
    }


    public IonStruct getIonRepresentation()
    {
        throw new UnsupportedOperationException();
    }


    public boolean isCompatible(SymbolTable other)
    {
        // FIXME Auto-generated method stub
        return false;
    }
}
