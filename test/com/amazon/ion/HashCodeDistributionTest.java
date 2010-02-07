// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazon.ion.system.SystemFactory;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Chi-square test for IonHasher.  Using selected test data, determine chi^2
 * for various hash table sizes and determine whether it is within 95%
 * confidence interval.  See Knuth vol. 2 sec 3.3.1 for background.
 */
public class HashCodeDistributionTest
{

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        File data_dir = findDataDir();
        if (data_dir != null)  {
            loadTestData(data_dir);
        }
    }

    /**
     * Test method for {@link IonValue#hashCode()} output value
     * distribution. This is a test of the raw hash code, not the
     * hash code as actually used by {@link HashMap} and
     * {@link HashSet}.
     */
    @Test
    public void testIonHashCodeUnadjustedAll() throws Exception
    {
        int[] counts = new int[1 << 12];   // must be power of 2
        dataSize = 0;
        for (IonValue v : testData)  {
            int index = v.hashCode() % counts.length;
            if (index < 0) {
                index += counts.length;
            }
            counts[index]++;
            dataSize++;
        }
        analyzeDistribution(counts, 1 << 10);
    }

    /**
     * Test method for {@link IonValue#hashCode()} output value
     * distribution, adjusting the hash codes as per {@link HashMap}.
     * This tests against a bigger range of hash map concentrations
     * since it's the more important use case.
     */
    @Test
    public void testIonHashCodeAdjustedAll() throws Exception
    {
        int[] counts = new int[1 << 12];   // must be power of 2
        dataSize = 0;
        for (IonValue v : testData)  {
            int index = hashMapHash(v.hashCode()) % counts.length;
            if (index < 0) {
                index += counts.length;
            }
            counts[index]++;
            dataSize++;
        }
        analyzeDistribution(counts, 1 << 4);
    }

    /**
     * Test method for {@link IonStruct#hashCode()} output value
     * distribution, adjusting the hash codes as per {@link HashMap}.
     */
    @Test
    public void testIonHashCodeAdjustedStructs() throws Exception
    {
        int[] counts = new int[1 << 12];   // must be power of 2
        dataSize = 0;
        for (IonValue v : testData)  {
            if (v instanceof IonStruct)  {
                int index = hashMapHash(v.hashCode()) % counts.length;
                if (index < 0) {
                    index += counts.length;
                }
                counts[index]++;
                dataSize++;
            }
        }
        analyzeDistribution(counts, 1 << 4);
    }

    /**
     * Test method for {@link IonSequence#hashCode()} output value
     * distribution, adjusting the hash codes as per {@link HashMap}.
     */
    @Test
    public void testIonHashCodeAdjustedSequences() throws Exception
    {
        int[] counts = new int[1 << 12];   // must be power of 2
        dataSize = 0;
        for (IonValue v : testData)  {
            if (v instanceof IonSequence)  {
                int index = hashMapHash(v.hashCode()) % counts.length;
                if (index < 0) {
                    index += counts.length;
                }
                counts[index]++;
                dataSize++;
            }
        }
        analyzeDistribution(counts, 1 << 4);
    }

    /**
     * Test method for {@link IonText#hashCode()} output value
     * distribution, adjusting the hash codes as per {@link HashMap}.
     */
    @Test
    public void testIonHashCodeAdjustedText() throws Exception
    {
        int[] counts = new int[1 << 12];   // must be power of 2
        dataSize = 0;
        for (IonValue v : testData)  {
            if (v instanceof IonText)  {
                int index = hashMapHash(v.hashCode()) % counts.length;
                if (index < 0) {
                    index += counts.length;
                }
                counts[index]++;
                dataSize++;
            }
        }
        analyzeDistribution(counts, 1 << 4);
    }

    /**
     * Analyze counts again Chi^2 distribution as provide and collapsed
     * down to limit entries from initial size
     * @param counts Input array
     * @param limit Limit for collapse
     */
    protected void analyzeDistribution(int[] counts, int limit)
    {
        while (counts.length >= limit) {
            testChiSquared(counts);
            counts = collapse(counts);
        }
    }

    /**
     * Analyze counts again Chi^2 distribution at 95% threshold
     * @param counts Input array
     */
    protected void testChiSquared(int[] counts)  {
        int total = 0;
        for (int count : counts) {
            total += count;
        }
        if (total == 0)  {
            return;     // nothing to test
        }
        assertEquals("Data size not equal to total counts", dataSize, total);
        double expected_value = (double) total / (double) counts.length;
        double chi_squared = 0.0;
        for (int count : counts) {
            chi_squared += (count - expected_value)*(count - expected_value)
                                / expected_value;
        }
        int degrees_of_freedom = counts.length - 1;
        // Compute 95% point of chi-square distribution
        double threshold;
        if (degrees_of_freedom < chiSquared95Table.length)  {
            threshold = chiSquared95Table[degrees_of_freedom];
        }
        else {
            double x_p = 1.64;
            threshold = degrees_of_freedom
                        + Math.sqrt(2*degrees_of_freedom)*x_p
                        + 2.0/3.0*x_p*x_p
                        - 2.0/3.0;
        }
        assertTrue(String.format("Chi-squared measured: %1$f, "
                                 + "limit: %2$f, nu: %3$d",
                                 chi_squared, threshold, degrees_of_freedom),
                   chi_squared < threshold);
    }

    /**
     * Collapse array and reanalyze
     * @param counts Input array
     */
    private static int[] collapse(int[] counts)
    {
        int[] new_counts = new int[counts.length >> 1];
        for (int i = 0; i < new_counts.length; ++i)  {
            new_counts[i] = counts[i] + counts[i + new_counts.length];
        }
        return new_counts;
    }

    protected static File findDataDir()  {

        // Look for test data
        File curr_dir = new File(System.getProperty("user.dir"));
        File data_dir = getDataDir(curr_dir);

        // If no data in curr directory, look up and over at IonTests,
        // per short format Brazil layout

        if (!data_dir.exists() || !data_dir.isDirectory())  {
            data_dir = getDataDir(new File(curr_dir.getParentFile(),
                                           "IonTests"));

            // If no data in curr directory, look up and over at IonTests,
            // per Brazil layout
            if (!data_dir.exists() || !data_dir.isDirectory())  {
                data_dir = getDataDir(new File(curr_dir.getParentFile(),
                                               "IonTests"));

                // Or the build directory
                if (!data_dir.exists() || !data_dir.isDirectory())  {
                    data_dir = getDataDir(new File(curr_dir, "build"));

                    // then give up
                    if (!data_dir.exists() || !data_dir.isDirectory())  {
                        data_dir = null;
                    }
                }
            }
        }
        return data_dir;
    }

    /**
     * Get data dir relative to specified path
     * @param base_dir Base directory
     * @return data directory
     */

    protected static File getDataDir(File base_dir)  {
        return new File(new File(base_dir, "bulk"), "items-A");
    }

    /**
     * Loads test data
     * @param data_dir Directory w data files
     * @throws FileNotFoundException if {@link File} has a bug
     */
    protected static void loadTestData(File data_dir) throws FileNotFoundException
    {
        File[] data_files = data_dir.listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.endsWith(".ion") || name.endsWith(".10n");
            }
        });
        for (File data_file : data_files)  {
            Iterator<IonValue> i = ionSys.iterate(
                    new BufferedInputStream(
                            new FileInputStream(data_file)));
            while (i.hasNext())  {
                loadValue(i.next());
            }
        }
    }

    /**
     * Loads an IonValue into the test data set.
     * @param value
     */
    protected static void loadValue(IonValue value)
    {
        testData.add(value);
        if (value instanceof IonContainer)
        {
            IonContainer container = (IonContainer) value;
            for (IonValue v : container)  {
                loadValue(v);
            }
        }
    }

    /**
     * Duplicate protected method {@link HashMap#hash(int)}.  Formula from
     * decompiled class in JDK 1.6.0.
     * @param raw_hash Raw hash value
     * @return cooked hash value
     */
    protected int hashMapHash(int raw_hash)  {
        int cooked_hash = raw_hash;
        cooked_hash ^= (cooked_hash >>> 20) ^ (cooked_hash >>> 12);
        return cooked_hash ^ (cooked_hash >>> 7) ^ (cooked_hash >>> 4);
    }

    protected static final Set<IonValue> testData
            = new HashSet<IonValue>(4096);

    protected int dataSize;

    protected static final IonSystem ionSys = SystemFactory.newSystem();

    /** From http://www.itl.nist.gov/div898/handbook/eda/section3/eda3674.htm */
    protected static final double[] chiSquared95Table = {
        -1,  // nu = 0 is undefined
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
