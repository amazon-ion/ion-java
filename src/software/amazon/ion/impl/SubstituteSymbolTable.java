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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonWriter;
import software.amazon.ion.ReadOnlyValueException;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;

/**
 * A symbol table used for an import where no exact match could be found.
 * This ensures that the declared max_id and version are exposed through the
 * library.
 * <p>
 * Note that {@link #isSubstitute()} is always true for instances of this
 * class.
 */
final class SubstituteSymbolTable
    implements SymbolTable
{
    /**
     * This is the original symbol table that is being substituted.
     * Either null or a non-system shared table.
     */
    private final SymbolTable myOriginalSymTab;
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
        myOriginalSymTab = null;
        myName = name;
        myVersion = version;
        myMaxId = maxId;
    }

    SubstituteSymbolTable(SymbolTable original, int version, int maxId)
    {
        assert original.isSharedTable() && ! original.isSystemTable();
        assert (original.getVersion() != version
                || original.getMaxId() != maxId);

        myOriginalSymTab = original;
        myName = original.getName();
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

    public SymbolToken intern(String text)
    {
        SymbolToken tok = find(text);
        if (tok == null)
        {
            throw new ReadOnlyValueException(SymbolTable.class);
        }
        return tok;
    }

    public SymbolToken find(String text)
    {
        SymbolToken tok = null;
        if (myOriginalSymTab != null)
        {
            tok = myOriginalSymTab.find(text);
            // If symbol token is found but its sid is beyond the correct max
            // id of the substitute, then return null, as it should not be
            // found at all.
            if (tok != null && tok.getSid() > myMaxId)
            {
                tok = null;
            }
        }
        return tok;
    }

    public int findSymbol(String text)
    {
        int sid = UNKNOWN_SYMBOL_ID;
        if (myOriginalSymTab != null)
        {
            sid = myOriginalSymTab.findSymbol(text);
            if (sid > myMaxId)
            {
                sid = UNKNOWN_SYMBOL_ID;
            }
        }
        return sid;
    }

    public String findKnownSymbol(int id)
    {
        if (id > myMaxId || myOriginalSymTab == null)
        {
            return null;
        }
        return myOriginalSymTab.findKnownSymbol(id);
    }

    @SuppressWarnings("unchecked")
    public Iterator<String> iterateDeclaredSymbolNames()
    {
        Iterator<String> originalIterator;
        if (myOriginalSymTab != null)
        {
            originalIterator = myOriginalSymTab.iterateDeclaredSymbolNames();
        }
        else
        {
            originalIterator = Collections.EMPTY_LIST.iterator();
        }
        return new SymbolIterator(originalIterator);
    }

    public void writeTo(IonWriter writer) throws IOException
    {
        IonReader reader = new SymbolTableReader(this);
        writer.writeValues(reader);
    }

    private final class SymbolIterator implements Iterator<String>
    {
        private Iterator<String> myOriginalIterator;
        private int myIndex = 0;

        SymbolIterator(Iterator<String> originalIterator)
        {
            myOriginalIterator = originalIterator;
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
                if (myOriginalIterator.hasNext())
                {
                    name = myOriginalIterator.next();
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
