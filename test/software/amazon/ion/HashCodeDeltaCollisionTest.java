/*
 * Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static software.amazon.ion.Timestamp.UTC_OFFSET;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.ion.IonDecimal;
import software.amazon.ion.IonFloat;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonTimestamp;
import software.amazon.ion.IonValue;
import software.amazon.ion.Timestamp;
import software.amazon.ion.IonValueDeltaGenerator.IonDecimalDeltaType;
import software.amazon.ion.IonValueDeltaGenerator.IonFloatDeltaType;
import software.amazon.ion.IonValueDeltaGenerator.IonIntDeltaType;
import software.amazon.ion.IonValueDeltaGenerator.IonSymbolDeltaType;
import software.amazon.ion.IonValueDeltaGenerator.IonTimestampDeltaType;


public class HashCodeDeltaCollisionTest
    extends IonTestCase
{
    private static final int NO_COLLISION = 0;
    private static final int DELTA_LIMIT = 1 << 4;

    /**
     * A point in time within +-100 years from now.
     * Generated non-deterministically by {@link #initTimestampBaseMillis()}.
     * Used by Timestamp delta collision test methods as a base millis value.
     */
    private static long TIMESTAMP_BASE_MILLIS;

    @BeforeClass
    public static void initTimestampBaseMillis()
    {
        long currentMillis = System.currentTimeMillis();
        long yearMillis = 60 * 60 * 24 * 365 * 1000L; // milliseconds in a year

        Random random = new Random(currentMillis);
        long randomRange = random.nextInt(100) * yearMillis; // 100 years range

        // Timestamp internal validation.
        // Range of TIMESTAMP_BASE_MILLIS is [-100, 100] from currentMillis.
        TIMESTAMP_BASE_MILLIS = (random.nextBoolean()
                                     ? currentMillis + randomRange
                                     : currentMillis - randomRange);

        System.out.println(HashCodeDeltaCollisionTest.class.getSimpleName() +
                           ".TIMESTAMP_BASE_MILLIS=" +
                           TIMESTAMP_BASE_MILLIS + "L");
    }

    /**
     * Checks if the generated IonValues from the {@code generator} passes/fails
     * a hash code collision test.
     *
     * @param generator
     *          the generator of IonValues
     * @param limit
     *          the limit of collisions allowed, an {@link AssertionError} will
     *          be thrown if the limit is exceeded
     */
    protected void checkIonValueDeltaCollisions(IonValueDeltaGenerator generator,
                                                int limit)
    {
        // Set of hashcodes that are generated from IonValues
        Set<Integer> hashCodeSet = new HashSet<Integer>();
        Set<IonValue> values = generator.generateValues();
        int collisions = 0;

        for (IonValue value : values)
        {
            boolean collision = !hashCodeSet.add(value.hashCode());
            if (collision)
            {
                collisions++;
            }
        }

        if (collisions > limit)
        {
            fail("checkIonValueDeltaCollisions failed on " +
                 generator.getValueType() + "\n" +
                 " collisions: " + collisions +
                 " limit: "      + limit);
        }
    }

    @Test
    public void testIonIntLongDeltaCollisions() throws Exception
    {
        IonSystem ionSystem = system();
        IonInt baseInt = ionSystem.newInt(1337L); // IonInt is long-backed.
        IonIntDeltaType deltaType = IonIntDeltaType.LONG;

        for (int delta = 1; delta < DELTA_LIMIT; delta <<= 1)
        {
            IonValueDeltaGenerator generator =
                new IonValueDeltaGenerator.Builder()
                    .ionSystem(ionSystem)
                    .delta(delta)
                    .size(100000)
                    .baseValue(baseInt)
                    .deltaType(deltaType)
                    .build();

            checkIonValueDeltaCollisions(generator, NO_COLLISION);
        }
    }

    @Test
    public void testIonIntBigIntegerDeltaCollisions() throws Exception
    {
        IonSystem ionSystem = system();
        // Create a BigDecimal that exceeds range of Long, so IonInt is forcibly
        // BigInteger-backed.
        BigDecimal bigDec = BigDecimal.valueOf(Long.MAX_VALUE + 1337e10);
        IonInt baseInt = ionSystem.newInt(bigDec);
        IonIntDeltaType deltaType = IonIntDeltaType.BIGINTEGER;

        for (int delta = 1; delta < DELTA_LIMIT; delta <<= 1)
        {
            IonValueDeltaGenerator generator =
                new IonValueDeltaGenerator.Builder()
                    .ionSystem(ionSystem)
                    .delta(delta)
                    .size(100000)
                    .baseValue(baseInt)
                    .deltaType(deltaType)
                    .build();

            checkIonValueDeltaCollisions(generator, NO_COLLISION);
        }
    }

    @Test
    public void testIonFloatDeltaCollisions() throws Exception
    {
        IonSystem ionSystem = system();
        IonFloat baseFloat = ionSystem.newFloat(1337.1337d);

        for (IonFloatDeltaType deltaType : IonFloatDeltaType.values())
        {
            for (int delta = 1; delta < DELTA_LIMIT; delta <<= 1)
            {
                IonValueDeltaGenerator generator =
                    new IonValueDeltaGenerator.Builder()
                        .ionSystem(ionSystem)
                        .delta(delta)
                        .size(100000)
                        .baseValue(baseFloat)
                        .deltaType(deltaType)
                        .build();

                checkIonValueDeltaCollisions(generator, NO_COLLISION);
            }
        }
    }

    @Test
    public void testIonDecimalDeltaCollisions() throws Exception
    {
        IonSystem ionSystem = system();
        IonDecimal baseDecimal = ionSystem
            .newDecimal(BigDecimal.valueOf(1337.1337d));

        for (IonDecimalDeltaType deltaType : IonDecimalDeltaType.values())
        {
            for (int delta = 1; delta < DELTA_LIMIT; delta <<= 1)
            {
                IonValueDeltaGenerator generator =
                    new IonValueDeltaGenerator.Builder()
                        .ionSystem(ionSystem)
                        .delta(delta)
                        .size(10000)
                        .baseValue(baseDecimal)
                        .deltaType(deltaType)
                        .build();

                checkIonValueDeltaCollisions(generator, 5);
            }
        }
    }

    @Test
    public void testIonTimestampDeltaCollisions() throws Exception
    {
        IonSystem ionSystem = system();
        IonTimestamp baseTimestamp = ionSystem
            .newTimestamp(Timestamp.forMillis(TIMESTAMP_BASE_MILLIS, UTC_OFFSET));

        for (IonTimestampDeltaType deltaType : IonTimestampDeltaType.values())
        {
            for (int delta = 1; delta < DELTA_LIMIT; delta <<= 1)
            {
                IonValueDeltaGenerator generator =
                    new IonValueDeltaGenerator.Builder()
                        .ionSystem(ionSystem)
                        .delta(delta)
                        .size(800)
                        .baseValue(baseTimestamp)
                        .deltaType(deltaType)
                        .build();

                checkIonValueDeltaCollisions(generator, NO_COLLISION);
            }
        }
    }

    @Test
    public void testIonSymbolSidDeltaCollisions() throws Exception
    {
        IonSystem ionSystem = system();
        IonSymbol baseSymbol = ionSystem
            .newSymbol(new FakeSymbolToken(null, 1337));

        IonSymbolDeltaType deltaType = IonSymbolDeltaType.SID;

        for (int delta = 1; delta < DELTA_LIMIT; delta <<= 1)
        {
            IonValueDeltaGenerator generator =
                new IonValueDeltaGenerator.Builder()
                    .ionSystem(ionSystem)
                    .delta(delta)
                    .size(100000)
                    .baseValue(baseSymbol)
                    .deltaType(deltaType)
                    .build();

            checkIonValueDeltaCollisions(generator, NO_COLLISION);
        }
    }


    /**
     * Sometimes there are sets of timestamps within a brief period,
     * and they shouldn't collide.
     */
    @Test
    public void testTimestampDeltaCollisions()
        throws Exception
    {
        final long limit = NO_COLLISION;

        // Walk through this many adjacent windows of time.
        final int windowCount = 100;

        // Each window is this long.
        final int windowMillis = 10031;

        for (int i = 0; i < windowCount; ++i)
        {
            // Look for collisions across each millisecond within the window.
            final long base = TIMESTAMP_BASE_MILLIS + i*windowMillis;

            Set<Integer> hashCodeSet = new HashSet<Integer>();
            int collisions = 0;

            for (int j = 0; j < windowMillis; ++j)
            {
                Timestamp value = Timestamp.forMillis(base+j, UTC_OFFSET);
                boolean collision = !hashCodeSet.add(value.hashCode());
                if (collision)
                {
                    collisions++;
                }
            }

            if (collisions > limit)
            {
                fail("Timestamp collisions: " + collisions +
                         " limit: " + limit);
            }
        }
    }
}
