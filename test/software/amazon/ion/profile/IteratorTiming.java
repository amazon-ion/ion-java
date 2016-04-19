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

package software.amazon.ion.profile;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Iterator;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonValue;
import software.amazon.ion.system.IonSystemBuilder;

public class IteratorTiming
{
    public static void main(String[] args)
    {
        IonSystem ion = IonSystemBuilder.standard().build();

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
