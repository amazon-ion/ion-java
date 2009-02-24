/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import junit.framework.Assert;

/**
 *
 */
public class TestUtils
{

    /**
     * Reads everything until the end of the current container, traversing
     * down nested containers.
     *
     * @param reader
     */
    public static void deepRead(IonReader reader)
    {
        while (reader.hasNext())
        {
            IonType t = reader.next();
            switch (t)
            {
                case NULL:
                case BOOL:
                case INT:
                case FLOAT:
                case DECIMAL:
                case TIMESTAMP:
                case STRING:
                case SYMBOL:
                case BLOB:
                case CLOB:
                    break;

                case STRUCT:
                case LIST:
                case SEXP:
                    reader.stepIn();
                    deepRead(reader);
                    reader.stepOut();
                    break;

                default:
                    Assert.fail("unexpected type: " + t);
            }
        }
    }

}
