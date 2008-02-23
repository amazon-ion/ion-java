/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package amazon.platform.clienttoolkit.util;

import java.util.Map;

/**
 *
 */
public class OldIonDatagramConverter
{

    static {
        System.out.println("Using old Ion Datagram Converter.\n");
    }
    private final OldIonTypedCreator ionMaker = new OldIonTypedCreator();


    public byte[] serialize(Map message)
    {
        return ionMaker.getIonDatagram(message).toBytes();
    }


    public byte[] appendElements(byte[] datagram, Map toAppend)
    {
        Map m = deserialize(datagram);
        m.putAll(toAppend);
        return serialize(m);
    }

    public Map deserialize(byte[] buf)
    {
        return (Map)ionMaker.getOrigionalValue(buf);
    }

    public void test() {
        ionMaker.test();
    }

}
