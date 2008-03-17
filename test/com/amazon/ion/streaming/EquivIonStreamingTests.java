package com.amazon.ion.streaming;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;

import junit.framework.TestSuite;
import com.amazon.ion.DirectoryTestSuite;
import com.amazon.ion.FileTestCase;
import com.amazon.ion.IonType;
import com.amazon.ion.impl.IonTokenReader.Type.timeinfo;

public class EquivIonStreamingTests extends DirectoryTestSuite {

	    private static class StreamingEquivsTest
	        extends FileTestCase
	    {
	        public StreamingEquivsTest(File ionText)
	        {
	            super(ionText);
	        }

	        public void runTest()
	            throws Exception
	        {
	        	IonIterator it1 = openIterator(myTestFile);
	        	IonIterator it2 = openIterator(myTestFile);

	        	iterateAndCompare(it1, it2);

	        	assert it1.hasNext() == false && it1.getDepth() == 0;
	        	assert it2.hasNext() == false && it2.getDepth() == 0;
	        }
	        
	        IonIterator openIterator(File f) {
	        	IonIterator it;
	        	int len = (int)myTestFile.length();
	        	byte[] buf = new byte[len];
	        	
	        	FileInputStream in;
	        	BufferedInputStream bin; 
				try {
					in = new FileInputStream(myTestFile);
					bin = new BufferedInputStream(in);
					bin.read(buf);
				} catch (FileNotFoundException e) {
					throw new RuntimeException(e);
		        } catch (IOException ioe) {
		        	throw new RuntimeException(ioe);
				} 
	        	it = IonIterator.makeIterator(buf);
	        	return it;
		    }
	        void compareFieldNames(IonIterator it1, IonIterator it2) {
	        	String f1 = it1.getFieldName();
	        	String f2 = it2.getFieldName();
	        	assert f1 != null && f2 != null;
	        	assert f1.equals(f2);
	        	return;
	        }
	        void compareAnnotations(IonIterator it1, IonIterator it2) {
	        	String[] a1 = it1.getAnnotations();
	        	String[] a2 = it2.getAnnotations();
	        	if (a1 == null) {
	        		assert a1 != null && a2 != null;
	        	}
	        	else {
	        		assert a1.length == a2.length;
	        		for (int ii=0; ii<a1.length; ii++) {
	    	        	String s1 = a1[ii];
	    	        	String s2 = a2[ii];
	    	        	assert s1 != null && s2 != null;
	    	        	assert s1.equals(s2);
	        		}
	        	}
	        	return;
	        }
	        void compareScalars(IonType t, IonIterator it1, IonIterator it2) {
        		switch (t) {
        		case BOOL:
        			assert it1.getBool() == it2.getBool();
        			break;
        		case INT:
        			assert it1.getLong() == it2.getLong();
        			break;
        		case FLOAT:
        			assert it1.getDouble() == it2.getDouble();
        			break;
        		case DECIMAL:
        			BigDecimal bd1 = it1.getBigDecimal();
        			BigDecimal bd2 = it2.getBigDecimal();
        			assert bd1.equals(bd2);
        			break;
        		case TIMESTAMP:
        			timeinfo t1 = it1.getTimestamp();
        			timeinfo t2 = it2.getTimestamp();
        			assert t1.d.equals(t2.d);
        			// if they're both null they're ==, otherwise the should be .equalse
        			assert t1.localOffset == t2.localOffset || t1.localOffset.equals(t2.localOffset);
        			break;
        		case STRING:
        		case SYMBOL:
        			String s1 = it1.getString();
        			String s2 = it2.getString();
        			assert s1.equals(s2);
        			break;
        		case BLOB:
        		case CLOB:
        			byte[] b1 = it1.getBytes();
        			byte[] b2 = it2.getBytes();
        			assert b1 != null && b2 != null;
        			assert b1.length == b2.length;
        			for (int ii=0; ii<b1.length; ii++) {
        				byte v1 = b1[ii];
        				byte v2 = b2[ii];
        				assert v1 == v2;
        			}
        			break;
        	    default:
        	    	throw new IllegalStateException("iterated to a type that's not expected");
        		}
	        }
	        void iterateAndCompare(IonIterator it1, IonIterator it2) {
	        	while (it1.hasNext() && it2.hasNext()) {
	        		IonType t1 = it1.next();
	        		IonType t2 = it2.next();
	        		if (it1.isInStruct()) {
	        			compareFieldNames(it1, it2);
	        		}
	        		compareAnnotations(it1, it2);
	        		assert t1.equals(t2);
		        	if (it1.isNull() || it2.isNull()) {
		        		// remember - anything can be a null value
		        		assert it1.isNull() && it2.isNull();
		        		continue;
		        	}

	        		switch (t1) {
	        		case NULL:
	        			assert it1.isNull() && it2.isNull();
	        			break;
	        		case BOOL:
	        		case INT:
	        		case FLOAT:
	        		case DECIMAL:
	        		case TIMESTAMP:
	        		case STRING:
	        		case SYMBOL:
	        		case BLOB:
	        		case CLOB:
	        	    	compareScalars(t1, it1, it2);
	        			break;
	        		case STRUCT:
	        		case LIST:
	        		case SEXP:
	        	    	it1.stepInto();
	        	    	it2.stepInto();
	        	    	iterateAndCompare(it1, it2);
	        	    	it1.stepOut();
	        	    	it2.stepOut();
	        	    	break;
	        	    	
	        	    default:
	        	    	throw new IllegalStateException("iterated to a type that's not expected");
	        		}
	        	}
	        	assert !it1.hasNext() && !it2.hasNext();
	        }
	    }

	    public static TestSuite suite()
	    {
	        return new EquivIonStreamingTests();
	    }


	    public EquivIonStreamingTests()
	    {
	        super("equivs");
	    }


	    @Override
	    protected StreamingEquivsTest makeTest(File ionFile)
	    {
	        return new StreamingEquivsTest(ionFile);
	    }
	
	
}
