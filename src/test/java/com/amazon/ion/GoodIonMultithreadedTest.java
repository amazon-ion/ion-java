package com.amazon.ion;

import com.amazon.ion.junit.Injected;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static com.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static com.amazon.ion.TestUtils.GOOD_IONTESTS_FILES;
import static com.amazon.ion.TestUtils.testdataFiles;

/**
 * Tests IonValue's multithreaded contract over the `good` ion-tests files.
 */
public class GoodIonMultithreadedTest extends IonTestCase {

    @Injected.Inject("testFile")
    public static final File[] FILES = testdataFiles(GLOBAL_SKIP_LIST, GOOD_IONTESTS_FILES);

    private File myTestFile;

    public void setTestFile(File file) {
        myTestFile = file;
    }

    /**
     * Wraps a byte array to provide an implementation of `equals` that compares the content of the array.
     */
    private static class ByteArrayWrapper {
        private final byte[] array;

        ByteArrayWrapper(byte[] array) {
            this.array = array;
        }

        @Override
        public boolean equals(Object that) {
            return that instanceof ByteArrayWrapper && Arrays.equals(array, ((ByteArrayWrapper) that).array);
        }
    }

    /**
     * Ensures all elements in the given list are equivalent to each other.
     * @param list the list to test.
     */
    private static void assertAllElementsEqual(List<?> list) {
        int i = 0; int j = 1;
        while (i < list.size() && j < list.size()) {
            assertEquals(list.get(i), list.get(j));
            i++;
            j++;
        }
    }

    /**
     * Makes the given IonValue read-only, then accesses its attributes concurrently from many threads. Verifies that
     * the results are the same from each thread.
     * @param value the value to test.
     */
    private void testMultithreadedAccess(IonValue value) {
        // Make read-only so that concurrent access is legal.
        value.makeReadOnly();
        // Concurrently access the value using many threads, recording the results.
        int numberOfTasks = 100;
        List<CompletableFuture<Void>> tasks = new ArrayList<>();
        List<IonValue> parentContainers = Collections.synchronizedList(new ArrayList<>());
        List<Integer> hashCodes = Collections.synchronizedList(new ArrayList<>());
        List<SymbolToken> fieldNames = Collections.synchronizedList(new ArrayList<>());
        List<List<SymbolToken>> annotations = Collections.synchronizedList(new ArrayList<>());
        List<List<Object>> values = Collections.synchronizedList(new ArrayList<>());
        for (int task = 0; task < numberOfTasks; task++) {
            tasks.add(CompletableFuture.runAsync(
                () -> {
                    hashCodes.add(value.hashCode());
                    parentContainers.add(value.getContainer());
                    fieldNames.add(value.getFieldNameSymbol());
                    annotations.add(Arrays.asList(value.getTypeAnnotationSymbols()));
                    if (value.isNullValue()) {
                        values.add(Collections.singletonList(value.getType()));
                    } else {
                        switch (value.getType()) {
                            case NULL:
                                values.add(Collections.singletonList(value.getType()));
                                break;
                            case BOOL:
                                values.add(Collections.singletonList(((IonBool) value).booleanValue()));
                                break;
                            case INT:
                                switch (((IonInt) value).getIntegerSize()) {
                                    case INT:
                                    case LONG:
                                        values.add(Collections.singletonList(((IonInt) value).longValue()));
                                        break;
                                    case BIG_INTEGER:
                                        values.add(Collections.singletonList(((IonInt) value).bigIntegerValue()));
                                        break;
                                }
                                break;
                            case FLOAT:
                                values.add(Collections.singletonList(((IonFloat) value).doubleValue()));
                                break;
                            case DECIMAL:
                                values.add(Collections.singletonList(((IonDecimal) value).decimalValue()));
                                break;
                            case TIMESTAMP:
                                values.add(Collections.singletonList(((IonTimestamp) value).timestampValue()));
                                break;
                            case SYMBOL:
                                values.add(Collections.singletonList(((IonSymbol) value).symbolValue()));
                                break;
                            case STRING:
                                values.add(Collections.singletonList(((IonString) value).stringValue()));
                                break;
                            case CLOB:
                            case BLOB:
                                values.add(Collections.singletonList(new ByteArrayWrapper(((IonLob) value).getBytes())));
                                break;
                            case LIST:
                            case SEXP:
                            case STRUCT:
                            case DATAGRAM:
                                IonContainer container = (IonContainer) value;
                                List<Object> children = new ArrayList<>();
                                container.forEach(children::add);
                                values.add(children);
                                break;
                        }
                    }
                }
            ));
        }
        tasks.forEach(CompletableFuture::join);
        assertAllElementsEqual(parentContainers);
        assertAllElementsEqual(hashCodes);
        assertAllElementsEqual(fieldNames);
        assertAllElementsEqual(annotations);
        assertAllElementsEqual(values);
    }

    /**
     * Selects a child value of the container at an arbitrary depth and index.
     * @param random source of randomness for the selection.
     * @param container the container.
     * @return a value.
     */
    private IonValue selectValueToTest(Random random, IonContainer container) {
        int numberOfValues = container.size();
        int valueToTarget = random.nextInt(numberOfValues + 1);
        IonValue value = null;
        if (valueToTarget == numberOfValues) {
            // Don't descend further.
            value = container;
        } else {
            Iterator<IonValue> iterator = container.iterator();
            for (int i = 0; i <= valueToTarget; i++) {
                value = iterator.next();
            }
            if (value instanceof IonContainer) {
                // Descend into the container and select one of its children.
                value = selectValueToTest(random, (IonContainer) value);
            }
        }
        return value;
    }

    @Test
    public void testRandomMultithreadedAccess() throws Exception {
        // Descend to a random child value, make that value read-only, then concurrently access that value.
        Random random = new Random();
        IonDatagram datagram = load(myTestFile);
        IonValue value = selectValueToTest(random, datagram);
        testMultithreadedAccess(value);
    }
}
