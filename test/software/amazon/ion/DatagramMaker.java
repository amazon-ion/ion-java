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

import static software.amazon.ion.TestUtils.ensureBinary;
import static software.amazon.ion.TestUtils.ensureText;

import java.util.ArrayList;
import java.util.Arrays;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonLoader;
import software.amazon.ion.IonSystem;
import software.amazon.ion.impl.PrivateUtils;

/**
 * Abstracts the various ways that {@link IonDatagram}s can be created, so test
 * cases can cover all the APIs.
 */
public enum DatagramMaker
{
    /**
     * Invokes {@link IonLoader#load(String)}.
     */
    FROM_STRING(true, false)
    {
        @Override
        public IonDatagram newDatagram(IonSystem system, String ionText)
        {
            IonLoader loader = system.getLoader();
            IonDatagram datagram = loader.load(ionText);
            return datagram;
        }
    },

    /**
     * Invokes {@link IonLoader#load(byte[])} with Ion binary.
     */
    FROM_BYTES_BINARY(false, true)
    {
        @Override
        public IonDatagram newDatagram(IonSystem system, byte[] ionData)
        {
            ionData = ensureBinary(system, ionData);
            IonLoader loader = system.getLoader();
            IonDatagram datagram = loader.load(ionData);
            return datagram;
        }
    },

    /**
     * Invokes {@link IonLoader#load(byte[])} with Ion text.
     */
    FROM_BYTES_TEXT(true, false)
    {
        @Override
        public IonDatagram newDatagram(IonSystem system, byte[] ionData)
        {
            ionData = ensureText(system, ionData);
            IonLoader loader = system.getLoader();
            IonDatagram datagram = loader.load(ionData);
            return datagram;
        }
    };

    //========================================================================

    private final boolean mySourceIsText;
    private final boolean mySourceIsBinary;

    private DatagramMaker(boolean sourceIsText, boolean sourceIsBinary)
    {
        mySourceIsText   = sourceIsText;
        mySourceIsBinary = sourceIsBinary;
    }

    public boolean sourceIsText()
    {
        return mySourceIsText;
    }

    public boolean sourceIsBinary()
    {
        return mySourceIsBinary;
    }

    public IonDatagram newDatagram(IonSystem system, String ionText)
    {
        byte[] utf8 = PrivateUtils.utf8(ionText);
        return newDatagram(system, utf8);
    }

    public IonDatagram newDatagram(IonSystem system, byte[] ionData)
    {
        IonDatagram dg = system.getLoader().load(ionData);
        String ionText = dg.toString();
        return newDatagram(system, ionText);
    }

    public static DatagramMaker[] valuesExcluding(DatagramMaker... exclusions)
    {
        DatagramMaker[] all = values();
        ArrayList<DatagramMaker> retained =
            new ArrayList<DatagramMaker>(Arrays.asList(all));
        retained.removeAll(Arrays.asList(exclusions));
        return retained.toArray(new DatagramMaker[retained.size()]);
    }
}
