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

package software.amazon.ion.impl;

import software.amazon.ion.IonCatalog;
import software.amazon.ion.IonContainer;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.system.IonWriterBuilder.InitialIvmHandling;

/**
 * This is the factory class for constructing writers with various capabilities.
 *
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public final class PrivateIonWriterFactory
{
    /**
     * @param container must not be null.
     */
    public static IonWriter makeWriter(IonContainer container)
    {
        IonSystem sys = container.getSystem();
        IonCatalog cat = sys.getCatalog();
        IonWriter writer = makeWriter(cat, container);
        return writer;
    }

    /**
     * @param container must not be null.
     */
    public static IonWriter makeWriter(IonCatalog catalog,
                                       IonContainer container)
    {
        IonSystem sys = container.getSystem();
        SymbolTable defaultSystemSymtab = sys.getSystemSymbolTable();

        // TODO the SUPPRESS here is a nasty discontinuity with other places
        // that create this kind of reader.  It prevents the Lite DG system
        // iterator from returning two IVMs at the start of the data.
        // The Span tests detect that problem.
        IonWriterSystemTree system_writer =
            new IonWriterSystemTree(defaultSystemSymtab, catalog, container,
                                    InitialIvmHandling.SUPPRESS);

        return new IonWriterUser(catalog, sys, system_writer);
    }


    /**
     * @param container must not be null.
     */
    public static IonWriter makeSystemWriter(IonContainer container)
    {
        IonSystem sys = container.getSystem();
        IonCatalog cat = sys.getCatalog();
        SymbolTable defaultSystemSymtab = sys.getSystemSymbolTable();
        IonWriter writer =
            new IonWriterSystemTree(defaultSystemSymtab, cat, container,
                                    null /* initialIvmHandling */);
        return writer;
    }
}
