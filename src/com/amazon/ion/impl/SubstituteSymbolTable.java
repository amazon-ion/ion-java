// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.InternedSymbol;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.UnknownSymbolException;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * A symbol table used for an import where no exact match could be found.
 * This ensures that the declared max_id and version are exposed through the
 * library.
 * <p>
 * Note that {@link #isSubstitute()} is always true for instances of this
 * class.
 */
class SubstituteSymbolTable
    implements SymbolTable
{
    /** Either null or a non-system shared table. */
    private final SymbolTable myDelegate;
    private final String myName;
    private final int myVersion;
    private final int myMaxId;


    /**
     * @param name
     * @param version
     * @param maxId
     */
    SubstituteSymbolTable(String name, int version,
                          int maxId)
    {
        myDelegate = null;
        myName = name;
        myVersion = version;
        myMaxId = maxId;
    }

    SubstituteSymbolTable(SymbolTable delegate, int version, int maxId)
    {
        assert delegate.isSharedTable() && ! delegate.isSystemTable();
        assert (delegate.getVersion() != version
                || delegate.getMaxId() != maxId);

        myDelegate = delegate;
        myName = delegate.getName();
        myVersion = version;
        myMaxId = maxId;
    }

    public String getName()
    {
        return myName;
    }

    public int getVersion()
    {
        return myVersion;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation always returns true.
     */
    public boolean isSubstitute()
    {
        return true;
    }

    public boolean isLocalTable()
    {
        return false;
    }

    public boolean isSharedTable()
    {
        return true;
    }

    public boolean isSystemTable()
    {
        return false;
    }

    public boolean isReadOnly()
    {
        return true;
    }

    public void makeReadOnly()
    {
    }

    public SymbolTable getSystemSymbolTable()
    {
        return null;
    }

    public String getIonVersionId()
    {
        return null;
    }

    public SymbolTable[] getImportedTables()
    {
        return null;
    }

    public int getImportedMaxId()
    {
        return 0;
    }

    public int getMaxId()
    {
        return myMaxId;
    }

    public InternedSymbol intern(String text)
    {
        InternedSymbol is = find(text);
        if (is == null)
        {
            throw new IonException("Cannot add new entries to shared symtab");
        }
        return is;
    }

    public InternedSymbol find(String text)
    {
        InternedSymbol is = null;
        if (myDelegate != null)
        {
            is = myDelegate.find(text);
            if (is != null && is.getId() > myMaxId)
            {
                is = null;
            }
        }
        return is;
    }

    public int findSymbol(String text)
    {
        int sid = UNKNOWN_SYMBOL_ID;
        if (myDelegate != null)
        {
            sid = myDelegate.findSymbol(text);
            if (sid > myMaxId)
            {
                sid = UNKNOWN_SYMBOL_ID;
            }
        }
        return sid;
    }

    @SuppressWarnings("deprecation")
    public String findSymbol(int id)
    {
        if (id > myMaxId || myDelegate == null)
        {
            throw new UnknownSymbolException(id);
        }
        return myDelegate.findSymbol(id);
    }

    public String findKnownSymbol(int id)
    {
        if (id > myMaxId || myDelegate == null)
        {
            return null;
        }
        return myDelegate.findKnownSymbol(id);
    }

    public int addSymbol(String name)
    {
        if (myDelegate != null)
        {
            @SuppressWarnings("deprecation")
            int sid = myDelegate.addSymbol(name);
            if (sid <= myMaxId)
            {
                return sid;
            }
        }
        throw new UnsupportedOperationException("Cannot add new entries to shared symtab");
    }

    public Iterator<String> iterateDeclaredSymbolNames()
    {
        Iterator<String> delegateIter;
        if (myDelegate != null)
        {
            delegateIter = myDelegate.iterateDeclaredSymbolNames();
        }
        else
        {
            delegateIter = Collections.EMPTY_LIST.iterator();
        }
        return new SymbolIterator(delegateIter);
    }

    public void writeTo(IonWriter writer) throws IOException
    {
        IonReader reader = new UnifiedSymbolTableReader(this);
        writer.writeValues(reader);
    }

    private final class SymbolIterator implements Iterator<String>
    {
        private Iterator<String> myDelegate;
        private int myIndex = 0;

        SymbolIterator(Iterator<String> delegate)
        {
            myDelegate = delegate;
        }

        public boolean hasNext()
        {
            if (myIndex < myMaxId) {
                return true;
            }
            return false;
        }

        public String next()
        {
            // TODO bad failure mode if next() called beyond end
            if (myIndex < myMaxId) {
                String name = null;
                if (myDelegate.hasNext())
                {
                    name = myDelegate.next();
                }
                myIndex++;
                return name;
            }
            return null;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}
