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

package software.amazon.ion;

import java.util.Iterator;
import software.amazon.ion.IonValue;


public class BinaryByteArrayIteratorSystemProcessingTest
    extends IteratorSystemProcessingTestCase
{
    private byte[] myBytes;


    @Override
    protected void prepare(String text)
        throws Exception
    {
        myMissingSymbolTokensHaveText = false;
        myBytes = encode(text);
    }

    @Override
    protected Iterator<IonValue> iterate()
    {
        return system().iterate(myBytes);
    }

    @Override
    protected Iterator<IonValue> systemIterate()
    {
        return system().systemIterate(myBytes);
    }
}
