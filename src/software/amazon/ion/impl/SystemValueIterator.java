/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.ion.impl;

import software.amazon.ion.IonCatalog;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonValue;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.impl.IonBinary.BufferManager;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;


interface SystemValueIterator
    extends Iterator<IonValue>, Closeable
{
    /********************************************************************
     *
     *                  Iterator<IonValue>
     *
     */
    public boolean hasNext();
    public IonValue next();
    public void remove();

    /********************************************************************
     *
     *                  SystemReader
     *
     */

    // constructors in original SystemReader:
    /*  make these static
    public SystemReader makeSystemReader(IonSystemImpl system, String s);
    public SystemReader makeSystemReader(IonSystemImpl system,
                        IonCatalog catalog,
                        Reader input);
    public SystemReader makeSystemReader(IonSystemImpl system,
                        IonCatalog catalog,
                        SymbolTable initialSymboltable,
                        Reader input);
    @Deprecated
    public SystemReader makeSystemReader(IonSystemImpl system,
                        IonCatalog catalog,
                        BufferManager buffer);

    public SystemReader makeSystemReader(IonSystemImpl system,
                        IonCatalog catalog,
                        InputStream stream);
    */

    public IonSystem getSystem();
    public IonCatalog getCatalog();

    /**
     * Returns the current symtab, either system or local.
     * @return may be null.
     */
    public SymbolTable getSymbolTable();

    /**
     * Gets the current symtab if its local, otherwise creates a local symtab
     * and makes it current.
     * @return not null.
     */
    public SymbolTable getLocalSymbolTable();

    public boolean currentIsHidden();
//    public boolean canSetLocalSymbolTable();
//    public void setLocalSymbolTable(SymbolTable symbolTable);
    public BufferManager getBuffer();
    public void resetBuffer();
    public void close() throws IOException;
}
