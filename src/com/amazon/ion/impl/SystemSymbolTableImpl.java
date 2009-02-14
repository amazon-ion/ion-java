/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.InvalidSystemSymbolException;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SystemSymbolTable;

/**
 *
 */
@Deprecated
public class SystemSymbolTableImpl
    implements SystemSymbolTable
{
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
        assert id > 0;
        return "$" + id;
    }


    //=========================================================================


    public SystemSymbolTableImpl()
    {
    }



    public final boolean isLocalTable()
    {
        return false;
    }

    public final boolean isSharedTable()
    {
        return true;
    }

    public final boolean isSystemTable()
    {
        return true;
    }

    public final String getName()
    {
        return ION;
    }


    public int getVersion()
    {
        return 1;
    }


    public SymbolTable getSystemSymbolTable()
    {
        return this;
    }

    public String getSystemId()
    {
        return ION_1_0;
    }


    public SymbolTable[] getImportedTables()
    {
        return null;
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


    public int addSymbol(String name)
    {
        String message = "Cannot call addSymbol on system symbol table";
        throw new UnsupportedOperationException(message);
    }

    public void defineSymbol(String name, int id)
    {
        String message = "Cannot call defineSymbol on system symbol table";
        throw new UnsupportedOperationException(message);
    }


    public int getMaxId()
    {
        return SYSTEM_SYMBOLS.length;
    }


    public int size()
    {
        return SYSTEM_SYMBOLS.length;
    }

    public boolean isTrivial()
    {
        return false;
    }

    public void writeTo(IonWriter writer)
    {
        throw new UnsupportedOperationException();
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
