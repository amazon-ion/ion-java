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

package com.amazon.ion;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests TextReader - BinaryWriter - BinaryReader
 */
public class TrBwBrProcessingTest
    extends BinaryReaderSystemProcessingTest
{
    @Override
    protected void prepare(String text)
        throws Exception
    {
        myMissingSymbolTokensHaveText = false;

        IonReader textReader = system().newSystemReader(text);
        IonBinaryWriter binaryWriter = system().newBinaryWriter();
        binaryWriter.writeValues(textReader);
        myBytes = binaryWriter.getBytes();
    }

    @Override
    @Ignore
    @Test
    public void testLocalSymtabWithMalformedSymbolEntries() throws Exception {
        // TODO amazon-ion/ion-java#151 this test exercises null slots in the local symbol table. The reader should collapse
        // all local symbol table null slots to $0. Currently, since this doesn't happen, the reader passes $10 to the
        // writer, which fails due to an out-of-range symbol ID.
        super.testLocalSymtabWithMalformedSymbolEntries();
    }
}
