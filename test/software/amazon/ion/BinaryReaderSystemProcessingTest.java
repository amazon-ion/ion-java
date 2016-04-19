/*
 * Copyright 2008-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion;

import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonLoader;
import software.amazon.ion.IonReader;

public class BinaryReaderSystemProcessingTest
    extends ReaderSystemProcessingTestCase
{
    protected byte[] myBytes;


    @Override
    protected void prepare(String text)
        throws Exception
    {
        myMissingSymbolTokensHaveText = false;

        IonLoader loader = loader();
        IonDatagram datagram = loader.load(text);
        myBytes = datagram.getBytes();
    }

    @Override
    public IonReader read() throws Exception
    {
        return system().newReader(myBytes);
    }

    @Override
    public IonReader systemRead() throws Exception
    {
        return system().newSystemReader(myBytes);
    }
}
