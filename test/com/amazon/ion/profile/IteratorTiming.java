/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.profile;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.SystemFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Iterator;

/**
 *
 */
public class IteratorTiming
{
    public static void main(String[] args)
    {
        IonSystem ion = SystemFactory.newSystem();

        System.out.println("Start at " + new Date());

        try
        {
            InputStream is = new FileInputStream(args[0]);
            Iterator<IonValue> itera = ion.iterate(new InputStreamReader(is));

            int count = 0;


            long start  = System.currentTimeMillis();
            long millis = start;

            while (itera.hasNext()){
                itera.next();

                if ((++count % 100) == 0)
                {
                    long diff = System.currentTimeMillis() - millis;
                    System.out.println(diff);
                    millis = System.currentTimeMillis();
                }
            }

            long now = System.currentTimeMillis();
            long elapsed = now - start;

            System.out.println();
            System.out.println("End at " + new Date());
            System.out.println("Total millis: " + elapsed);
            System.out.println("# values: " + count);
            System.out.println("avg millis/value: " + ((float)elapsed) / count);

            Runtime rt = Runtime.getRuntime();
            System.out.println("Total memory: "+ rt.totalMemory());
        }
        catch (IOException e)
        {
            System.err.println(e);
        }
    }
}
