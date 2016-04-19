/*
 * Copyright 2009-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static software.amazon.ion.IonType.LIST;
import static software.amazon.ion.IonType.SEXP;
import static software.amazon.ion.IonType.STRING;
import static software.amazon.ion.IonType.STRUCT;
import static software.amazon.ion.IonType.SYMBOL;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Ignore;
import org.junit.Test;
import software.amazon.ion.IonContainer;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;

/** Chi-square test for {@link IonValue#hashCode()} implementations.
 * Using selected test data, determine Chi^2 value for various hash table
 * sizes and determine whether it is within 95% confidence interval using a
 * goodness of fit test. The Chi-square test tests for the fact that hashes
 * generated from the hash functions are random, with a 95% confidence.
 * <p>
 * See Knuth vol. 2 sec 3.3.1 for background.
 */
@Ignore
public class HashCodeDistributionTest
    extends IonTestCase
{
    /**
     * This value is the initial capacity of the data set (i.e. number of
     * buckets). This must be a power of 2.
     * <p>
     * We want to keep halving the number of buckets in the data set to test on
     * different sizes of buckets. The halving of the buckets is performed at
     * {@link #collapse(int[])}.
     * <p>
     * The rationale behind this is that we want to mimic the internal
     * implementation of how {@link HashMap} and {@link ConcurrentHashMap}
     * stores its hash table, which is an {@link Entry} array with size that is
     * a power of 2.
     */
    private static final int DATA_SET_INITIAL_CAPACITY = 1 << 12;

    /**
     * This is sync'ed with {@link HashMap}'s DEFAULT_INITIAL_CAPACITY
     * constant, which is 16.
     */
    private static final int HASH_MAP_INITIAL_CAPACITY = 1 << 4;

    private static final String BULK_TEST_DATA_PATH = "items-A";

    protected Set<IonValue> loadBulkDataSet()
        throws IOException
    {
        Set<IonValue> dataSet = new HashSet<IonValue>();
        File directory = getBulkTestdataFile(BULK_TEST_DATA_PATH);

        File[] testFiles = directory.listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.endsWith(".ion") || name.endsWith(".10n");
            }
        });

        IonSystem ionSystem = system();
        for (File testFile : testFiles)
        {
            InputStream in =
                new BufferedInputStream(new FileInputStream(testFile));
            try
            {
                Iterator<IonValue> i = ionSystem.iterate(in);
                while (i.hasNext())
                {
                    loadValue(i.next(), dataSet);
                }
            }
            finally
            {
                in.close();
            }
        }

        return dataSet;
    }

    /**
     * Loads the {@link IonValue} and its nested values into the {@code dataSet}.
     *
     * @param value
     * @param dataSet
     */
    protected void loadValue(IonValue value, Set<IonValue> dataSet)
    {
        dataSet.add(value);
        if (value instanceof IonContainer)
        {
            IonContainer container = (IonContainer) value;
            for (IonValue v : container)
            {
                loadValue(v, dataSet);
            }
        }
    }

    /**
     * Analyze key entries, {@code counts}, against the Chi^2 distribution as
     * provided, while collapsing {@code counts} down to {@code limit} from the
     * initial size.
     *
     * @param counts key entries int array
     * @param totalCount total number of key entries
     * @param limit limit for {@link #collapse(int[])}
     */
    protected void analyzeDistribution(int[] counts, int totalCount, int limit)
    {
        while (counts.length >= limit)
        {
            checkChiSquareValue(counts, totalCount);
            counts = collapse(counts);
        }
    }

    /**
     * Analyze counts against Chi^2 distribution at 95% confidence interval.
     *
     * @param counts key entries int array
     */
    protected void checkChiSquareValue(int[] counts, int totalCount)
    {
        int expectedTotalCount = 0;
        for (int count : counts)
        {
            expectedTotalCount += count;
        }

        assertEquals("Total counts is incorrect", totalCount, expectedTotalCount);

        if (expectedTotalCount == 0)
        {
            return; // nothing to test
        }

        double expectedValue = (double) totalCount / (double) counts.length;
        double chiSquared = 0.0;
        for (int count : counts)
        {
            chiSquared += Math.pow((count - expectedValue), 2) / expectedValue;
        }
        int degreesOfFreedom = counts.length - 1;

        double threshold;
        if (degreesOfFreedom < upperTailChiSquared95Table.length)
        {
            threshold = upperTailChiSquared95Table[degreesOfFreedom];
        }
        else
        {
            // Calculate critical chi-square value using formula for degrees of
            // freedom greater than 100. Refer to Knuth vol. 2 sec 3.3.1
            double x_p = 1.64;
            threshold = degreesOfFreedom
                        + Math.sqrt(2*degreesOfFreedom)*x_p
                        + (2.0/3.0)*Math.pow(x_p, 2)
                        - (2.0/3.0);
        }

        if (chiSquared >= threshold)
        {
            fail(String.format("Chi-square value above 95 percent " +
                               "threshold: %1$f, limit: %2$f, nu: %3$d",
                               chiSquared, threshold, degreesOfFreedom));
        }
    }

    /**
     * Collapse key entries int array and return a new int array.
     *
     * @param counts
     * @return the new key entries int array
     */
    protected int[] collapse(int[] counts)
    {
        int[] new_counts = new int[counts.length >> 1]; // halve the number of buckets
        for (int i = 0; i < new_counts.length; ++i)
        {
            new_counts[i] = counts[i] + counts[i + new_counts.length];
        }
        return new_counts;
    }

    /**
     * Duplicate protected method {@link HashMap#hash(int)}.  Formula from
     * decompiled class in JDK 1.6.0.
     * @param raw_hash Raw hash value
     * @return cooked hash value
     */
    protected int hashMapHash(int raw_hash)
    {
        int cooked_hash = raw_hash;
        cooked_hash ^= (cooked_hash >>> 20) ^ (cooked_hash >>> 12);
        return cooked_hash ^ (cooked_hash >>> 7) ^ (cooked_hash >>> 4);
    }

    /**
     * Counts each occurrence of {@link IonValue#hashCode()} and perform
     * Chi^2 analysis over it.
     *
     * @param dataSet
     *          the data set containing IonValue test data
     * @param type
     *          filter over IonValue test data; all IonValues are used if
     *          {@code null}
     * @param adjusted
     *          {@code true} if using {@link #hashMapHash(int)} as a
     *          supplementary hash function
     * @param limit
     *          the limit to {@link #collapse(int[])}
     */
    protected void checkChiSquareIonValueHashCode(Set<IonValue> dataSet,
                                                  IonType type,
                                                  boolean adjusted,
                                                  int limit)
    {
        int[] counts = new int[DATA_SET_INITIAL_CAPACITY]; // must be power of 2
        int countsLength = counts.length;
        int totalCount = 0;
        for (IonValue v : dataSet)
        {
            if (type == null || v.getType().equals(type))
            {
                int index = adjusted ?
                            hashMapHash(v.hashCode()) % countsLength :
                            v.hashCode() % countsLength;
                if (index < 0)
                {
                    index += countsLength;
                }
                counts[index]++;
                totalCount++;
            }
        }
        analyzeDistribution(counts, totalCount, limit);
    }




    @Test
    public void testChiSquareBulkDataSetUnadjusted() throws Exception
    {
        Set<IonValue> dataSet = loadBulkDataSet();

        checkChiSquareIonValueHashCode(dataSet, null, false,
                                       HASH_MAP_INITIAL_CAPACITY);
    }

    @Test
    public void testChiSquareBulkDataSetAdjusted() throws Exception
    {
        Set<IonValue> dataSet = loadBulkDataSet();

        checkChiSquareIonValueHashCode(dataSet, null, true,
                                       HASH_MAP_INITIAL_CAPACITY);
    }

    @Test
    public void testChiSquareBulkDataSetStructAdjusted() throws Exception
    {
        Set<IonValue> dataSet = loadBulkDataSet();

        checkChiSquareIonValueHashCode(dataSet, STRUCT, true,
                                       HASH_MAP_INITIAL_CAPACITY);
    }

    @Test
    public void testChiSquareBulkDataSetSequenceAdjusted() throws Exception
    {
        Set<IonValue> dataSet = loadBulkDataSet();

        checkChiSquareIonValueHashCode(dataSet, LIST, true,
                                       HASH_MAP_INITIAL_CAPACITY);
        checkChiSquareIonValueHashCode(dataSet, SEXP, true,
                                       HASH_MAP_INITIAL_CAPACITY);
    }

    @Test
    public void testChiSquareBulkDataSetTextAdjusted() throws Exception
    {
        Set<IonValue> dataSet = loadBulkDataSet();

        checkChiSquareIonValueHashCode(dataSet, STRING, true,
                                       HASH_MAP_INITIAL_CAPACITY);
        checkChiSquareIonValueHashCode(dataSet, SYMBOL, true,
                                       HASH_MAP_INITIAL_CAPACITY);
    }





    /**
     * Chi-squre critical values for 95% confidence interval. One-based.
     *
     * @see <a href="http://www.itl.nist.gov/div898/handbook/eda/section3/eda3674.htm">Chi-square Critical Values Table</a>
     */
    protected static final double[] upperTailChiSquared95Table = {
           -1, // nu =   0 is undefined
        3.841, // nu =   1
        5.991, // nu =   2
        7.815, // nu =   3
        9.488, // nu =   4
       11.070, // nu =   5
       12.592, // nu =   6
       14.067, // nu =   7
       15.507, // nu =   8
       16.919, // nu =   9
       18.307, // nu =  10
       19.675, // nu =  11
       21.026, // nu =  12
       22.362, // nu =  13
       23.685, // nu =  14
       24.996, // nu =  15
       26.296, // nu =  16
       27.587, // nu =  17
       28.869, // nu =  18
       30.144, // nu =  19
       31.410, // nu =  20
       32.671, // nu =  21
       33.924, // nu =  22
       35.172, // nu =  23
       36.415, // nu =  24
       37.652, // nu =  25
       38.885, // nu =  26
       40.113, // nu =  27
       41.337, // nu =  28
       42.557, // nu =  29
       43.773, // nu =  30
       44.985, // nu =  31
       46.194, // nu =  32
       47.400, // nu =  33
       48.602, // nu =  34
       49.802, // nu =  35
       50.998, // nu =  36
       52.192, // nu =  37
       53.384, // nu =  38
       54.572, // nu =  39
       55.758, // nu =  40
       56.942, // nu =  41
       58.124, // nu =  42
       59.304, // nu =  43
       60.481, // nu =  44
       61.656, // nu =  45
       62.830, // nu =  46
       64.001, // nu =  47
       65.171, // nu =  48
       66.339, // nu =  49
       67.505, // nu =  50
       68.669, // nu =  51
       69.832, // nu =  52
       70.993, // nu =  53
       72.153, // nu =  54
       73.311, // nu =  55
       74.468, // nu =  56
       75.624, // nu =  57
       76.778, // nu =  58
       77.931, // nu =  59
       79.082, // nu =  60
       80.232, // nu =  61
       81.381, // nu =  62
       82.529, // nu =  63
       83.675, // nu =  64
       84.821, // nu =  65
       85.965, // nu =  66
       87.108, // nu =  67
       88.250, // nu =  68
       89.391, // nu =  69
       90.531, // nu =  70
       91.670, // nu =  71
       92.808, // nu =  72
       93.945, // nu =  73
       95.081, // nu =  74
       96.217, // nu =  75
       97.351, // nu =  76
       98.484, // nu =  77
       99.617, // nu =  78
      100.749, // nu =  79
      101.879, // nu =  80
      103.010, // nu =  81
      104.139, // nu =  82
      105.267, // nu =  83
      106.395, // nu =  84
      107.522, // nu =  85
      108.648, // nu =  86
      109.773, // nu =  87
      110.898, // nu =  88
      112.022, // nu =  89
      113.145, // nu =  90
      114.268, // nu =  91
      115.390, // nu =  92
      116.511, // nu =  93
      117.632, // nu =  94
      118.752, // nu =  95
      119.871, // nu =  96
      120.990, // nu =  97
      122.108, // nu =  98
      123.225, // nu =  99
      124.342, // nu = 100
    };
}
