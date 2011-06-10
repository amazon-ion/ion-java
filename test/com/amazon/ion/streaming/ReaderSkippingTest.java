// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import static com.amazon.ion.TestUtils.testdataFiles;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.TestUtils;
import com.amazon.ion.junit.Injected.Inject;
import com.amazon.ion.junit.IonAssert;
import java.io.File;
import java.io.FileInputStream;
import java.util.Random;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReaderSkippingTest
extends IonTestCase
{
    static final boolean _debug_flag = true;

    @Inject("testFile")
    public static final File[] FILES =
        testdataFiles(TestUtils.GLOBAL_SKIP_LIST,
                      "good", "equivs");

    private File myTestFile;
    private IonReader myFullReader;
    private IonReader mySkipReader;

    private static long SEED = 0;
    private static int SKIP_PERCENT = 50;
    private static int ITERATIONS_PER_FILE = 10;
    private Random myRandom;


    public void setTestFile(File file)
    {
        myTestFile = file;
    }


    @Override
    @Before
    public void setUp()
    throws Exception
    {
        super.setUp();
        myFullReader = system().newReader(new FileInputStream(myTestFile));
        mySkipReader = system().newReader(new FileInputStream(myTestFile));

        if (SEED == 0)
        {
            SEED = System.currentTimeMillis();
            System.err.println("ReaderSkippingTest.SEED=" + SEED + "L");
        }

        myRandom = new Random(SEED);
    }

    @Override
    @After
    public void tearDown()
    throws Exception
    {
        myFullReader.close();
        mySkipReader.close();
        myFullReader = null;
        mySkipReader = null;
        myRandom = null;
        super.tearDown();
    }


    private boolean skip()
    {
        int val = myRandom.nextInt(100);
        return (val < SKIP_PERCENT);
    }


    @Test
    public void testFile()
    throws Exception
    {
        for (int i = 0; i < ITERATIONS_PER_FILE; i++)
        {
            skipThroughFile();
        }
    }


    public void skipThroughFile()
    throws Exception
    {
        FileInputStream fullStream = new FileInputStream(myTestFile);
        myFullReader = system().newReader(fullStream);
        try
        {
            FileInputStream skipStream = new FileInputStream(myTestFile);
            mySkipReader = system().newReader(skipStream);
            try
            {
                skipThroughContainer();
            }
            finally
            {
                mySkipReader.close();
                skipStream.close();
            }
        }
        finally
        {
            myFullReader.close();
            fullStream.close();
        }
    }


    public void skipThroughContainer()
    {
        while (true)
        {
            IonType fullType = myFullReader.next();
            IonType skipType = mySkipReader.next();

            assertEquals("next type", fullType, skipType);
            if (fullType == null) return;

            skipThroughValue(fullType);
        }
    }

    public void skipThroughValue(IonType fullType)
    {
        // Read all data from myFullReader,
        // but only some data from mySkipReader

        myFullReader.getTypeAnnotations();
        if (!skip())
        {
            Assert.assertArrayEquals(myFullReader.getTypeAnnotations(),
                                     mySkipReader.getTypeAnnotations());
        }

        myFullReader.getTypeAnnotationIds();
        if (!skip())
        {
            Assert.assertArrayEquals(myFullReader.getTypeAnnotationIds(),
                                     mySkipReader.getTypeAnnotationIds());
        }

        myFullReader.getFieldName();
        if (!skip())
        {
            assertEquals(myFullReader.getFieldName(),
                         mySkipReader.getFieldName());
        }

        myFullReader.getFieldId();
        if (!skip())
        {
            assertEquals(myFullReader.getFieldId(),
                         mySkipReader.getFieldId());
        }

        if (skip())
        {
            system().newValue(myFullReader);
        }
        else
        {
            switch (fullType)
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
                {
                    IonValue fullValue = system().newValue(myFullReader);
                    IonValue skipValue = system().newValue(mySkipReader);
                    IonAssert.assertIonEquals(fullValue, skipValue);
                    break;
                }
                case STRUCT:
                case LIST:
                case SEXP:
                {
                    myFullReader.stepIn();
                    mySkipReader.stepIn();
                    skipThroughContainer();
                    myFullReader.stepOut();
                    mySkipReader.stepOut();
                    break;
                }
                default:
                    throw new IllegalStateException("unexpected type: " + fullType);
            }
        }
    }
}
