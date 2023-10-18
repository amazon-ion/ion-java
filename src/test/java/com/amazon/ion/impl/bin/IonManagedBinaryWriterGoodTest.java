package com.amazon.ion.impl.bin;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonValue;
import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.junit.Injected;
import com.amazon.ion.system.IonReaderBuilder;
import org.junit.Test;

import java.io.File;

import static com.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static com.amazon.ion.TestUtils.GOOD_IONTESTS_FILES;
import static com.amazon.ion.TestUtils.hexDump;
import static com.amazon.ion.TestUtils.testdataFiles;

/**
 * Re-writes and verifies all "good" ion-tests files using all combinations of IonManagedBinaryWriter options.
 */
public class IonManagedBinaryWriterGoodTest extends IonManagedBinaryWriterTestCase {

    @Injected.Inject("testFile")
    public static final File[] FILES =
        testdataFiles(GLOBAL_SKIP_LIST,
            GOOD_IONTESTS_FILES);


    private File myTestFile;

    public void setTestFile(File file)
    {
        myTestFile = file;
    }

    @Test
    public void allGoodFiles() throws Exception {
        byte[] testData = _Private_Utils.loadFileBytes(myTestFile);
        IonReader reader = IonReaderBuilder.standard().build(testData);
        writer.writeValues(reader);
        reader.close();

        writer.finish();
        final byte[] data = writer.getBytes();
        final IonValue actual;
        try {
            actual = system().getLoader().load(data);
        } catch (final Exception e) {
            throw new IonException("Bad generated data:\n" + hexDump(data), e);
        }
        final IonValue expected = system().getLoader().load(testData);
        assertEquals(expected, actual);

        additionalValueAssertions(actual);
    }
}
