/*
 * Copyright 2015-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.impl.bin;

import static software.amazon.ion.IonType.LIST;
import static software.amazon.ion.IonType.STRUCT;
import static software.amazon.ion.SystemSymbols.IMPORTS_SID;
import static software.amazon.ion.SystemSymbols.ION_1_0;
import static software.amazon.ion.SystemSymbols.ION_SHARED_SYMBOL_TABLE_SID;
import static software.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static software.amazon.ion.SystemSymbols.MAX_ID_SID;
import static software.amazon.ion.SystemSymbols.NAME_SID;
import static software.amazon.ion.SystemSymbols.SYMBOLS_SID;
import static software.amazon.ion.SystemSymbols.VERSION_SID;
import static software.amazon.ion.impl.bin.Symbols.systemSymbol;

import java.io.IOException;
import java.util.Iterator;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;

/**
 * Provides the basic implementation bits for {@link SymbolTable}.
 */
/*package*/ abstract class AbstractSymbolTable implements SymbolTable
{
    private final String name;
    private final int version;

    public AbstractSymbolTable(final String name, final int version)
    {
        this.name = name;
        this.version = version;
    }

    public final String getName()
    {
        return name;
    }

    public final int getVersion()
    {
        return version;
    }

    public final String getIonVersionId()
    {
        return ION_1_0;
    }

    public final int findSymbol(final String name)
    {
        final SymbolToken token = find(name);
        if (token == null)
        {
            return UNKNOWN_SYMBOL_ID;
        }
        return token.getSid();
    }

    public final void writeTo(final IonWriter writer) throws IOException
    {
        if (isSharedTable())
        {
            writer.setTypeAnnotationSymbols(systemSymbol(ION_SHARED_SYMBOL_TABLE_SID));
        }
        else if (isLocalTable())
        {
            writer.setTypeAnnotationSymbols(systemSymbol(ION_SYMBOL_TABLE_SID));
        }
        else
        {
            throw new IllegalStateException("Invalid symbol table, neither shared nor local");
        }
        writer.stepIn(STRUCT);
        {
            if (isSharedTable())
            {
                writer.setFieldNameSymbol(systemSymbol(NAME_SID));
                writer.writeString(name);
                writer.setFieldNameSymbol(systemSymbol(VERSION_SID));
                writer.writeInt(version);
            }
            final SymbolTable[] imports = getImportedTables();
            if (imports != null && imports.length > 0)
            {
                writer.setFieldNameSymbol(systemSymbol(IMPORTS_SID));
                writer.stepIn(LIST);
                for (final SymbolTable st : imports)
                {
                    writer.stepIn(STRUCT);
                    {
                        writer.setFieldNameSymbol(systemSymbol(NAME_SID));
                        writer.writeString(st.getName());
                        writer.setFieldNameSymbol(systemSymbol(VERSION_SID));
                        writer.writeInt(st.getVersion());
                        writer.setFieldNameSymbol(systemSymbol(MAX_ID_SID));
                        writer.writeInt(st.getMaxId());
                    }
                    writer.stepOut();
                }
                writer.stepOut();
            }
            writer.setFieldNameSymbol(systemSymbol(SYMBOLS_SID));
            writer.stepIn(LIST);
            {
                final Iterator<String> iter = iterateDeclaredSymbolNames();
                while (iter.hasNext())
                {
                    writer.writeString(iter.next());
                }
            }
            writer.stepOut();
        }
        writer.stepOut();
    }

    public void makeReadOnly() {}

}
