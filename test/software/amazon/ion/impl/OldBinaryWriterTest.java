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

import software.amazon.ion.IonBinaryWriter;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolTable;


@SuppressWarnings("deprecation")
public class OldBinaryWriterTest
    extends IonWriterTestCase
{
    private IonBinaryWriter myWriter;


    @Override
    protected IonWriter makeWriter(SymbolTable... imports)
        throws Exception
    {
        myOutputForm = OutputForm.BINARY;
        myWriter = system().newBinaryWriter(imports);
        return myWriter;
    }

    @Override
    protected byte[] outputByteArray()
        throws Exception
    {
        return myWriter.getBytes();
    }

    @Override
    protected void checkClosed()
    {
        // No-op.
    }

    @Override
    protected void checkFlushed(boolean expectFlushed)
    {
        // No-op.
    }
}
