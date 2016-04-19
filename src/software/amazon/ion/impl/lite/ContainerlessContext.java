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

package software.amazon.ion.impl.lite;

import software.amazon.ion.SymbolTable;

/**
 * Context for IonValues that are not contained in any Container or Datagram
 */
/*package*/ class ContainerlessContext
    implements IonContext
{
    private final IonSystemLite _system;
    private final SymbolTable _symbols;

    public static ContainerlessContext wrap(IonSystemLite system){
        return new ContainerlessContext(system, null);
    }

    public static ContainerlessContext wrap(IonSystemLite system, SymbolTable symbols){
        return new ContainerlessContext(system, symbols);
    }

    private ContainerlessContext(IonSystemLite system, SymbolTable symbols){
        _system = system;
        _symbols = symbols;
    }

    public IonContainerLite getContextContainer()
    {
        return null;
    }

    public IonSystemLite getSystem()
    {
        return _system;
    }

    public SymbolTable getContextSymbolTable()
    {
        return _symbols;
    }

}
