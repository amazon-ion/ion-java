/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.impl.lite;

import software.amazon.ion.SymbolTable;


/**
 * Context for child values of an IonDatagramLite. The
 * datagram's child values that share the same local symbol table
 * will share the same TopLevelContext.
 */
final class TopLevelContext
    implements IonContext
{
    /**
     * References the containing datagram
     */
    private final IonDatagramLite _datagram;

    /**
     * This will be a local symbol table, or null.  It is not valid
     * for this to be a shared symbol table since shared
     * symbol tables are only shared.  It will not be a
     * system symbol table as the system object will be
     * able to resolve its symbol table to the system
     * symbol table and following the parent/owning_context
     * chain will lead to a system object.
     * <p>
     * TODO amznlabs/ion-java#19 we cannot assume that the IonSystem knows the proper IVM
     * in this context
     */
    private final SymbolTable _symbols;

    private TopLevelContext(SymbolTable symbols, IonDatagramLite datagram)
    {
        assert datagram != null;
        _symbols = symbols;
        _datagram = datagram;
    }

    static TopLevelContext wrap(SymbolTable symbols,
                                IonDatagramLite datagram)
    {
        TopLevelContext context = new TopLevelContext(symbols, datagram);
        return context;
    }

    public IonDatagramLite getContextContainer()
    {
        return _datagram;
    }

    public SymbolTable getContextSymbolTable()
    {
        return _symbols;
    }

    public IonSystemLite getSystem()
    {
        return _datagram.getSystem();
    }

}
