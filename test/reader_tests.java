
// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

import com.amazon.ion.IonBinaryWriter;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.IonReaderFactoryX;
import com.amazon.ion.impl.IonReaderTextSystemX;
import com.amazon.ion.impl.UnifiedInputStreamX;
import com.amazon.ion.impl.IonScalarConversionsX.CantConvertException;
import com.amazon.ion.system.SystemFactory;
import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

public class reader_tests
{
    static final boolean verbose = false;
    static final int BUFFER_SIZE_LIMIT = 1000000;

    static IonSystem sys = SystemFactory.newSystem();
    abstract static class test {
        private String _name;
        test(String name) {
            _name = name;
        }
        abstract long doit() throws IOException;
        String getName() {
            return _name;
        }
        abstract IonReader getReader() throws IOException;
        abstract InputStream getInputStream() throws IOException;
    }
    abstract static class test_scan extends test
    {
        test_input _ti;
        test_scan(String name, test_input ti) {
            super(name);
            _ti = ti;
        }
        @Override
        InputStream getInputStream() throws IOException {
            InputStream in = _ti.getInputStream();
            return in;
        }
    }
    abstract static class test_scan_new extends test_scan
    {
        test_scan_new(String name, test_input ti) {
            super(name, ti);
        }
        @Override
        String getName() {
            return "new "+super.getName();
        }
        @Override
        IonReader getReader() throws IOException {
            IonReader r;
            r = new IonReaderTextSystemX(_ti.getByteStreamOverStream());
            return r;
        }
    }
    abstract static class test_scan_old extends test_scan
    {
        test_scan_old(String name, test_input ti) {
            super(name, ti);
        }
        @Override
        String getName() {
            return "OLD "+super.getName();
        }
        @Override
        IonReader getReader() throws IOException {
            IonReader r;
            InputStream in = new ByteArrayInputStream(_ti.getBytes());
            r = sys.newReader(in);
            return r;
        }
    }
    static class test_scan_materialize_new extends test_scan_new
    {
        test_scan_materialize_new(String name, test_input ti) {
            super(name, ti);
        }
        @Override
        String getName() {
            return "Scan Materialized "+super.getName();
        }
        @Override
        long doit() throws IOException {
            IonReader r = getReader();
            long count = scan_reader(r, "new", true);
            return count;
        }
    }
    static class test_scan_materialize_old extends test_scan_old
    {
        test_scan_materialize_old(String name, test_input ti) {
            super(name, ti);
        }
        @Override
        String getName() {
            return "Scan Materialized "+super.getName();
        }
        @Override
        long doit() throws IOException {
            IonReader r = getReader();
            long count = scan_reader(r, "new", true);
            return count;
        }
    }
    static class test_scan_only_new extends test_scan_new
    {
        public test_scan_only_new(String name, test_input ti) {
            super(name, ti);
        }
        @Override
        String getName() {
            return "Scan only "+super.getName();
        }
        @Override
        long doit() throws IOException {
            IonReader r = getReader();
            long count = scan_reader_quick(r);
            return count;
        }
    }
    static class test_scan_only_old extends test_scan_old
    {
        test_scan_only_old(String name, test_input ti) {
            super(name, ti);
        }
        @Override
        String getName() {
            return "Scan only "+super.getName();
        }
        @Override
        long doit() throws IOException {
            IonReader r = getReader();
            long count = scan_reader_quick(r);
            return count;
        }
    }
    static class test_scan_for_name_new extends test_scan_new
    {
        String _field;
        test_scan_for_name_new(String test_name, test_input ti, String field_name) {
            super(test_name, ti);
            _field = field_name;
        }
        @Override
        String getName() {
            return "Scan for '"+_field+"' "+super.getName();
        }
        @Override
        long doit() throws IOException {
            IonReader r = getReader();
            long count = scan_reader_field(r, _field);
            return count;
        }
    }
    static class test_scan_for_name_old extends test_scan_old
    {
        String _field;
        test_scan_for_name_old(String test_name, test_input ti, String field_name) {
            super(test_name, ti);
            _field = field_name;
        }
        @Override
        String getName() {
            return "Scan for '"+_field+"' "+super.getName();
        }
        @Override
        long doit() throws IOException {
            IonReader r = getReader();
            long count = scan_reader_field(r, _field);
            return count;
        }
    }
    static final int READ_BUFFER_SIZE = 32*1024;
    static byte[] read_buffer = new byte[READ_BUFFER_SIZE];
    static class test_scan_file_new extends test_scan_new
    {
        test_scan_file_new(String test_name, test_input ti) {
            super(test_name, ti);
        }
        @Override
        String getName() {
            return "open file "+super.getName();
        }
        @Override
        long doit() throws IOException {
            long count = 1;
            InputStream in = getInputStream();
            while (in.read(read_buffer, 0, READ_BUFFER_SIZE) > 0) {
                count++;
            }
            return count;
        }
    }
    static class test_scan_file_old extends test_scan_old
    {
        test_scan_file_old(String test_name, test_input ti) {
            super(test_name, ti);
        }
        @Override
        String getName() {
            return "open file "+super.getName();
        }
        @Override
        long doit() throws IOException {
            long count = 1;
            InputStream in = getInputStream();
            while (in.read(read_buffer, 0, READ_BUFFER_SIZE) > 0) {
                count++;
            }
            return count;
        }
    }


    public static long time_it(test t, int count) throws IOException {
        long start, finish;
        long scan_count = 0;

        start = System.currentTimeMillis();
        String time_unit = "ms";
        System.out.println(t.getName()+":");
        System.out.println("start: "+start);
        for (int ii=0; ii<count; ii++) {
            scan_count += t.doit();
        }
        finish = System.currentTimeMillis();
        long elapsed = (finish-start);
        System.out.println("scan count: "+scan_count);
        System.out.println("end: "+finish+", elapsed "+time_unit+": "+elapsed);
        System.out.println(time_unit+" per interation: "+((double)elapsed)/count);

        return scan_count;
    }

    public static void timing_tests() throws Exception
    {
        String name, path = "C:\\src\\brazil\\src\\shared\\platform\\IonTests\\mainline\\iontestdata\\good\\";
        long count1 = 0, count2 = 0;
        test_input  ti;

        int iterations;

        path = "C:\\src\\workspaces\\IonC_msc\\test\\";

        iterations = 100;
        name = "test5k.json";
        ti = new test_input(path+name);
        //count1 = time_it(new test_scan_for_name_new(name, ti, "id"), iterations);
        //count2 = time_it(new test_scan_for_name_old(name, ti, "id"), iterations);
        //count1 = time_it(new test_scan_materialize_new(name, ti), iterations);
        //count2 = time_it(new test_scan_materialize_old(name, ti), iterations);
        //count1 = time_it(new test_scan_only_new(name, ti), iterations);
        //count2 = time_it(new test_scan_only_old(name, ti), iterations);
        //count1 = time_it(new test_scan_file_new(name, ti), iterations);
        //count2 = time_it(new test_scan_file_old(name, ti), iterations);

        if (count1 > 0) {
            assert(count1 == count2);
            return;
        }

        iterations = 1;
        name = "test_data.json";
        ti = new test_input(path+name);
        //count1 = time_it(new test_scan_only_new(name, ti), iterations);
        count1 = time_it(new test_scan_for_name_new(name, ti, "id"), iterations);
        count1 = time_it(new test_scan_materialize_new(name, ti), iterations);
    }

    public static void main(String[] args) throws Exception
    {
        system_reader_test();

        Double d = Double.valueOf("1.23");
        BigDecimal bd = new BigDecimal(d);
        System.out.println(d);
        System.out.println(bd);

        String name, path;
        test_input  ti;

        System.out.println("reader tests started");

        path = "C:\\src\\brazil\\src\\shared\\platform\\IonTests\\mainline\\iontestdata\\good\\";
        test_comparison(path, "sexpAnnotationQuotedOperator.ion");

        test_comparisons(path, null);


        path = "C:\\src\\brazil\\src\\shared\\platform\\IonTests\\mainline\\iontestdata\\equivs\\";
        test_equivalent(path, "nullNulls.ion");

        test_equivalents(path, null);

        path = "C:\\src\\brazil\\src\\shared\\platform\\IonTests\\mainline\\iontestdata\\good\\";

        name = "fieldNameQuotedFalse.ion";
        ti = new test_input(path+name);
        ti.scan_all("file: "+name);

        File f = new File(path);
        File[] files = f.listFiles();
        for (File f1 : files) {
            String fname = f1.getName();
            if (!fname.endsWith(".ion")) {
                continue;
            }
            ti = new test_input(f1.getAbsolutePath());
            ti.scan_all("file: "+fname);
        }

        name = "fieldNameQuotedFalse.ion";
        ti = new test_input(path+name);
        ti.scan_all("file: "+name);

        char[] chars1 = make_text_chars_1(200);
        ti = new test_input(chars1);
        ti.scan_all("make_text_chars_1");

        byte[] bytes1 = make_text_bytes_1(200);
        ti = new test_input(bytes1);
        ti.scan_all("make_text_bytes_1");
        path = "C:\\src\\workspaces\\IonC_msc\\test\\";
        name = "test5k.json";
        ti = new test_input(path+name);
        test tt = new test_scan_only_new("quick test", ti);
        IonReader r = tt.getReader();
        r.next();     // this has stepped onto a struct
        r.hasNext();  // this will step over the struct (it should be to the next top level struct)
        r.hasNext();  // and again, to the same 2nd top level struct
        r.next();     // this returns 2nd top level struct

        test_failures();

//        timing_tests();
//        if (path.length() > 0) return;

        System.out.println("reader tests finished");

    }

    public static void test_equivalents(String path, String extension) throws IOException
    {
        File f = new File(path);
        File[] files = f.listFiles();
        for (File f1 : files) {
            String fname = f1.getName();
            if (extension != null && !fname.endsWith(extension)) {
                continue;
            }
            if (fname.endsWith(".swp")) {
                // extension of Vim temp files
                continue;
            }
            test_equivalent(path, fname);
        }

    }

    public static void test_equivalent(String path, String fname) throws IOException
    {
        test_input ti;

        File f = new File(path+fname);
        ti = new test_input(f.getAbsolutePath());
        IonReader r_old = sys.newReader(ti.getInputStream());
        IonReader r_new = IonReaderFactoryX.makeReader(sys, new FileInputStream(f));
        compare_equiv(r_old, r_new);

        IonValue v = sys.getLoader().load(ti.getInputStream());
        byte[] b = ((IonDatagram)v).getBytes();
        IonReader r_old2 = sys.newReader(v);
        IonReader r_new2 = IonReaderFactoryX.makeReader(sys, b);
        compare_equiv(r_old2, r_new2);

    }


    public static void test_comparisons(String path, String extension) throws IOException
    {
        File f = new File(path);
        File[] files = f.listFiles();
        for (File f1 : files) {
            String fname = f1.getName();
            if (extension != null && !fname.endsWith(extension)) {
                continue;
            }
            test_comparison(path, fname);
        }

    }

    public static void test_comparison(String path, String fname) throws IOException
    {
        test_input ti;

        File f = new File(path+fname);
        ti = new test_input(f.getAbsolutePath());
        IonReader r_old = sys.newReader(ti.getInputStream());
        IonReader r_new = IonReaderFactoryX.makeReader(sys, new FileInputStream(f));
        compare(r_old, r_new, 0);

        IonValue v = sys.getLoader().load(ti.getInputStream());
        byte[] b = ((IonDatagram)v).getBytes();
        // IonReader r_old2 = sys.newReader(v);
        IonReader r_old2 = sys.newReader(b, 0, b.length);
        IonReader r_new2 = IonReaderFactoryX.makeReader(sys, b);
        compare(r_old2, r_new2, 0);

    }
    public static void compare (IonReader r_old, IonReader r_new, int depth)
        throws IOException
    {
        long count = 0;

        while (compare_hasNext(r_old, r_new))
        {
            count++;
            if (count == 5 && depth == 1) {
                depth = (int)(count - 4);
            }
            IonType t = compare_next(r_old, r_new);

            compare_field_names(r_old, r_new);
            compare_annotations(r_old, r_new);

            if (compare_isNull(r_old, r_new)) {
                continue;
            }
            switch (t) {
            case NULL:
            case DATAGRAM:
                throw new IllegalStateException("we shouldn't ever get here");
            case BOOL:
                compare_bool(r_old, r_new);
                break;
            case INT:
                compare_int(r_old, r_new);
                break;
            case FLOAT:
                compare_float(r_old, r_new);
                break;
            case DECIMAL:
                compare_decimal(r_old, r_new);
                break;
            case TIMESTAMP:
                compare_timestamp(r_old, r_new);
                break;
            case SYMBOL:
                compare_symbol(r_old, r_new);
                break;
            case STRING:
                compare_string(r_old, r_new);
                break;
            case CLOB:
            case BLOB:
                compare_lob(r_old, r_new);
                break;
            case LIST:
            case SEXP:
            case STRUCT:
                r_old.stepIn();
                r_new.stepIn();
                compare(r_old, r_new, depth + 1);
                r_old.stepOut();
                r_new.stepOut();
                break;
            }
        }
    }
    static int has_next_counter = 0;
    public static boolean compare_hasNext(IonReader r_old, IonReader r_new)
        throws IOException
    {
        has_next_counter++;
        if (has_next_counter == 6) {
            has_next_counter = has_next_counter + 1 - 1;
        }
        boolean hn1 = r_old.hasNext();
        boolean hn2 = r_new.hasNext();
        if (hn1 != hn2) {
            throw new IOException("has next failure: old: "+hn1+" new: "+hn2);
        }
        return hn1;
    }
    public static IonType compare_next(IonReader r_old, IonReader r_new)
        throws IOException
    {
        IonType n1 = r_old.next();
        IonType n2 = r_new.next();
        if (n1 != n2) {
            String message = "next() failure: old: "+n1+" new: "+n2;
            throw new IOException(message);
        }
        return n1;
    }

    public static boolean compare_isNull(IonReader r_old, IonReader r_new)
        throws IOException
    {
        boolean n1 = r_old.isNullValue();
        boolean n2 = r_new.isNullValue();
        if (n1 != n2) {
            throw new IOException("isNullValue() failure: old: "+n1+" new: "+n2);
        }
        return n1;
    }
    public static void compare_bool(IonReader r_old, IonReader r_new)
        throws IOException
    {
        boolean n1 = r_old.booleanValue();
        boolean n2 = r_new.booleanValue();
        if (n1 != n2) {
            throw new IOException("boolean value failure: old: "+n1+" new: "+n2);
        }
        return;
    }
    public static void compare_int(IonReader r_old, IonReader r_new)
        throws IOException
    {
        long n1 = r_old.longValue();
        long n2 = r_new.longValue();
        if (n1 != n2) {
            throw new IOException("int value failure: old: "+n1+" new: "+n2);
        }
        return;
    }
    public static void compare_float(IonReader r_old, IonReader r_new)
        throws IOException
    {
        double n1 = r_old.doubleValue();
        double n2 = r_new.doubleValue();
        if (Double.valueOf(n1).compareTo(Double.valueOf(n2)) == 0) return;
        throw new IOException("float value failure: old: "+n1+" new: "+n2);
    }
    public static void compare_decimal(IonReader r_old, IonReader r_new)
        throws IOException
    {
        BigDecimal n1 = r_old.bigDecimalValue();
        BigDecimal n2 = r_new.bigDecimalValue();
        if (!n1.equals(n2)) {
            throw new IOException("decimal value failure: old: "+n1+" new: "+n2);
        }
        return;
    }
    public static void compare_timestamp(IonReader r_old, IonReader r_new)
        throws IOException
    {
        Timestamp n1 = r_old.timestampValue();
        Timestamp n2 = r_new.timestampValue();
        if (!n1.equals(n2)) {
            throw new IOException("timestamp value failure: old: "+n1+" new: "+n2);
        }
        return;
    }
    public static void compare_string(IonReader r_old, IonReader r_new)
        throws IOException
    {
        String n1 = r_old.stringValue();
        String n2 = r_new.stringValue();
        if (!n1.equals(n2)) {
            String message = r_old.getType().toString()
                           + " value failure, old string: \""
                           + n1
                           + "\", new string: \""
                           + n2
                           + "\"";
            throw new IOException(message);
        }
        return;
    }
    public static void compare_symbol(IonReader r_old, IonReader r_new)
        throws IOException
    {
        String n1 = r_old.stringValue();
        String n2 = r_new.stringValue();
        if (!n1.equals(n2)) {
            String message = r_old.getType().toString()
                           + " value failure, old symbol: '"
                           + n1
                           + "', new string: '"
                           + n2
                           + "'";
            throw new IOException(message);
        }
        return;
}
    public static void compare_lob(IonReader r_old, IonReader r_new)
        throws IOException
    {
        byte[] n1 = r_old.newBytes();
        byte[] n2 = r_new.newBytes();
        if (!compare_buffers_equal(n1, n2)) {
            throw new IOException("lob ("+r_old.getType()+") value failure: old: "+n1+" new: "+n2);
        }
        return;
    }
    public static boolean compare_buffers_equal(byte[] b1, byte[] b2)
    {
        if (b1 == null) {
            return (b2 == null);
        }
        if (b2 == null) {
            return false;
        }
        if (b1.length != b2.length) {
            return false;
        }
        for (int ii=0; ii<b1.length; ii++) {
            if (b1[ii] != b2[ii]) {
                return false;
            }
        }
        return true;
    }

    public static void compare_field_names(IonReader r_old, IonReader r_new)
        throws IOException
    {
        if (r_old.isInStruct()) {
            String a_old = r_old.getFieldName();
            String a_new = r_new.getFieldName();
            compare_field_names(a_old, a_new);
        }
    }
    public static void compare_field_names(String a_old, String a_new)
        throws IOException
    {
        if (a_old == null || a_new == null) {
            String message = "field names must be non-null";
            if (a_old == null) {
                message += " and the old field name is null";
            }
            if (a_new == null) {
                message += " and the new field name is null";
                if (a_old == null) {
                    message += " too!";
                }
            }
            throw new IOException(message);
        }

        if (!a_old.equals(a_new)) {
            String message = "mismatch field names old: \""
                           + a_old
                           + "\", new: \""
                           + a_new
                           + "\"";
            throw new IOException(message);
        }
    }

    public static void compare_annotations(IonReader r_old, IonReader r_new)
        throws IOException
    {
        String[] a_old = r_old.getTypeAnnotations();
        String[] a_new = r_new.getTypeAnnotations();
        compare_annotations(a_old, a_new);
    }
    public static void compare_annotations(String[] a_old, String[] a_new)
        throws IOException
    {
        int l_old = (a_old == null) ? 0 : a_old.length;
        int l_new = (a_new == null) ? 0 : a_new.length;
        if (l_old != l_new) {
            String message = "annotation lengths don't match old: "
                + l_old
                + ", new: "
                + l_new;
            throw new IOException(message);
        }
        if (l_old == 0) {
            return;
        }
        boolean[] used = new boolean[l_old];

        // compare all the strings - order doesn't matter
        // but a value can be used only once and there
        // may be duplicates.
        boolean found;
        for (int old=0; old<a_old.length; old++) {
            String  a = a_old[old];
            found = false;
            for (int n=0; n<a_new.length; n++) {
                if (used[n]) continue;
                found = a.equals(a_new[n]);
                if (found) {
                    used[n] = true;
                    break;
                }
            }
            if (!found) {
                String message = "old annotation '"
                               + a
                               + "' not found in new reader";
                throw new IOException(message);
            }
        }
        // at this point since we have the same number of
        // annotation in both arrays, and we only "used"
        // the value from the new array once, and we found
        // all the values that are present in the old array
        // in the new array (exactly once), the lists must
        // be equivalent
    }

    public static void test_failures() throws IOException
    {
        String path;

        path = "C:\\src\\brazil\\src\\shared\\platform\\IonTests\\mainline\\iontestdata\\bad\\";

        String name = "symbolEmptyWithCRLF.ion";
        File f = new File(path+name);
        test_on_bad_file(f);

        f = new File(path);
        File[] files = f.listFiles();
        for (File f1 : files) {
            test_on_bad_file(f1);
        }
    }

    static void test_on_bad_file(File f1) throws IOException
    {
        test_input  ti;
        int non_error_count;

        non_error_count = 0;
        String fname = f1.getName();
        if (!fname.endsWith(".ion")) {
            return;
        }
        ti = new test_input(f1.getAbsolutePath());
        try {
            ti.scan_bbuf(fname, true);
            non_error_count |= 1;
        }
        catch (Exception e) {

        }
        try {
            ti.scan_brdr(fname, true);
            non_error_count |= 2;
        }
        catch (Exception e) {

        }
        try {
            ti.scan_cbuf(fname, true);
            non_error_count |= 4;
        }
        catch (Exception e) {

        }
        try {
            ti.scan_crdr(fname, true);
            non_error_count |= 8;
        }
        catch (Exception e) {

        }
        if (non_error_count != 0) {
            System.out.println("error expected (and not encountered) in file "+f1.getName());
            if (non_error_count != 0xf) {
                System.out.println("error flags: "+Integer.toHexString(non_error_count));
            }
        }

    }

    static char[] make_text_chars_1(int len) {
        StringBuilder sb = new StringBuilder(len);
        while (sb.length() <= len) {
            sb.append(" "+sb.length());
        }
        return sb.toString().toCharArray();
    }

    static byte[] make_text_bytes_1(int len) {
        byte[] bytes = new byte[len];
        int dst = 0;
        while (dst < len) {
            if (dst != 0) {
                bytes[dst++] = ' ';
            }
            String s = "0x"+Integer.toHexString(dst);
            int src = 0;
            while (dst < len && src < s.length()) {
                bytes[dst++] = (byte)s.charAt(src++);
            }
        }
        return bytes;
    }

    static long scan_reader(IonReader r, String header, boolean materialize) {
        long count;
        if (verbose) {
            System.out.println("\n-----------------------"+header+":\n");
        }
        count = scan_reader_helper(r, 0, header, materialize);
        if (verbose) {
            System.out.println("\n---- token count: "+count);
        }
        return count;
    }
    static long scan_reader_helper(IonReader r, int depth, String header, boolean materialize)
    {
        long count = 0;

        IonType parent = r.getType();
        if (depth > 0) {
            parent = r.getType();
        }
        else {
            parent = IonType.DATAGRAM;
        }

        while (r.hasNext()) {
            IonType t = r.next();
            count++;
            if (verbose) {
                for (int ii=0; ii<depth; ii++) {
                    System.out.print("  ");
                }
                System.out.print(header);
                System.out.print(": type: "+t);
            }
            if (materialize) {
                String fieldname = r.getFieldName();
                if (verbose) {
                    if (fieldname != null) {
                        System.out.print(" "+fieldname+": ");
                    }
                }
                String [] as = r.getTypeAnnotations();
                if  (verbose) {
                    for (String a : as) {
                        System.out.print(" "+a+":: ");
                    }
                }
            }
            if (r.isNullValue()) {
                t = IonType.NULL;
            }
            switch (t) {
                case NULL:
                    if (verbose) System.out.print(" is null");
                    break;
                case BOOL:
                    if (materialize) {
                        if (r.booleanValue()) {
                            if (verbose) System.out.print(" true");
                        }
                        else {
                            if (verbose) System.out.print(" false");
                        }
                    }
                    break;
                case INT:
                    if (materialize) {
                        try {
                            int ival = r.intValue();
                            if (verbose) System.out.print(" "+ival);
                        }
                        catch (CantConvertException e) {
                            try {
                                long lival = r.longValue();
                                if (verbose) System.out.print(" "+lival);
                            }
                            catch (CantConvertException e2) {
                                if (r instanceof IonReaderTextSystemX) {
                                    IonReaderTextSystemX r2 = (IonReaderTextSystemX)r;
                                    BigInteger bival = r2.bigIntegerValue();
                                    if (verbose) System.out.print(" "+bival);
                                }
                            }
                        }
                    }
                    break;
                case FLOAT:
                    if (materialize) {
                        double f = r.doubleValue();
                        if (verbose) System.out.print(" "+f);
                    }
                    break;
                case DECIMAL:
                    if (materialize) {
                        BigDecimal d = r.bigDecimalValue();
                        if (verbose) System.out.print(" "+d.toString());
                    }
                    break;
                case STRING:
                case SYMBOL:
                    if (materialize) {
                        String s = r.stringValue();
                        if (verbose) System.out.print(" "+s);
                    }
                    break;
                 case TIMESTAMP:
                     if (materialize) {
                        Timestamp ti = r.timestampValue();
                        if (verbose) System.out.print(" "+ti.toString());
                     }
                    break;
                case BLOB:
                case CLOB:
                    if (materialize) {
                        byte[] bytes = r.newBytes();
                        if (verbose) System.out.print(" bytes["+bytes.length+"]");
                    }
                    break;
                case STRUCT:
                case LIST:
                case SEXP:
                    if (verbose) System.out.println(get_container_open(t));
                    r.stepIn();
                    count += scan_reader_helper(r, depth+1, header, materialize);
                    r.stepOut();
                    break;
                default:
                    break;
            }
            if (verbose) System.out.println();
        }
        if (verbose) {
            if (depth > 0) {
                System.out.println(get_container_close(parent));
            }
        }
        return count;
    }

    static String get_container_open(IonType t) {
        String start;
        switch(t) {
            case STRUCT:
                start = "{";
                break;
            case LIST:
                start = "[";
                break;
            case SEXP:
                start = "(";
                break;
            default:
                start = t.toString()+" is not a container!";
                break;
        }
        return start;
    }
    static String get_container_close(IonType t) {
        String start;
        switch(t) {
            case STRUCT:
                start = "}";
                break;
            case LIST:
                start = "]";
                break;
            case SEXP:
                start = ")";
                break;
            default:
                start = t.toString()+" is not a container!";
                break;
        }
        return start;
    }

    static long scan_reader_field(IonReader r, String field) {
        long count;
        /*
        IonTextUserReader u;
        if (r instanceof IonTextUserReader) {
            u = (IonTextUserReader)r;
            IonFieldList fl = new IonFieldList();
            fl.addField(field);
            count = scan_reader_field_helper(u, 0, fl);
        }
        else {
        */
            count = scan_reader_field_helper(r, 0, field);
        //}
        return count;
    }
    static long scan_reader_field_helper(IonReader r, int depth, String field)
    {
        long count = 0;
        while (r.hasNext()) {
            IonType t = r.next();
            count++;
            if (r.isNullValue()) {
                t = IonType.NULL;
            }
            switch (t) {
            default:
                break;
            case STRUCT:
                r.stepIn();
                count += scan_reader_field_helper_struct(r, depth+1, field);
                r.stepOut();
                break;
            case LIST:
            case SEXP:
                r.stepIn();
                count += scan_reader_field_helper(r, depth+1, field);
                r.stepOut();
                break;
            }
        }
        return count;
    }
    static long scan_reader_field_helper_struct(IonReader r, int depth, String field)
    {
        long count = 0;
        while (r.hasNext()) {
            IonType t = r.next();
            count++;
            if (count > 5000) {
                return count;
            }
            String fieldname = r.getFieldName();
            if (!field.equals(fieldname)) {
                continue;
            }
            if (r.isNullValue()) {
                t = IonType.NULL;
            }
            switch (t) {
            default:
                break;
            case STRUCT:
                r.stepIn();
                count += scan_reader_field_helper_struct(r, depth+1, field);
                r.stepOut();
                break;
            case LIST:
            case SEXP:
                r.stepIn();
                count += scan_reader_field_helper(r, depth+1, field);
                r.stepOut();
                break;
            }
        }
        return count;
    }


    static long scan_reader_quick(IonReader r) {
        long count;
        count = scan_reader_helper_quick(r, 0);
        return count;
    }
    static long scan_reader_helper_quick(IonReader r, int depth)
    {
        long count = 0;
        while (r.hasNext()) {
            IonType t = r.next();
            count++;
            if (r.isNullValue()) {
                t = IonType.NULL;
            }
            switch (t) {
            default:
                break;
            case STRUCT:
            case LIST:
            case SEXP:
                r.stepIn();
                count += scan_reader_helper_quick(r, depth+1);
                r.stepOut();
                break;
            }
        }
        return count;
    }

    static class test_input
    {
        enum source { CHARS, BYTES, FILE }

        source _source;
        char[] _chars;
        byte[] _bytes;
        String _file_name;
        File   _file;

        test_input(char[] c) throws IOException
        {
            _source = source.CHARS;
            _chars = c;
        }
        test_input(byte[] bytes) throws IOException
        {
            _source = source.BYTES;
            _bytes = bytes;
        }
        test_input(String fileName) throws IOException
        {
            _source = source.FILE;
            _file_name = fileName;
            _file = new File(_file_name);
        }

        byte[] getBytes() throws IOException {
            if (_bytes == null) {
                switch(_source) {
                case CHARS:
                    assert( _chars != null );
                    CharsetEncoder encoder = Charset.forName("utf-8").newEncoder();
                    CharBuffer cb = CharBuffer.wrap(_chars);
                    ByteBuffer bb = encoder.encode(cb);
                    _bytes = new byte[bb.limit()];
                    byte[] temp = bb.array();
                    System.arraycopy(temp, 0, _bytes, 0, bb.limit());
                    break;
                case FILE:
                    assert( _file_name != null && _file != null );
                    InputStream in = new FileInputStream(_file);

                    long len = _file.length();
                    if (len > BUFFER_SIZE_LIMIT) {
                        System.out.println("ERROR: the file "+_file_name+" is too long ("+len+") to load into a buffer.");
                    }
                    else {
                        _bytes = new byte[(int)len];
                        int len2 = in.read(_bytes);
                        assert(len2 == len);
                    }
                    break;
                case BYTES:
                default:
                    throw new IllegalStateException("you can't ask for bytes until there's data available");
                }
            }
            return _bytes;
        }
        char[] getChars() throws IOException {
            if (_chars == null) {
                switch(_source) {
                case BYTES:
                    assert( _bytes != null );
                    CharsetDecoder decoder = Charset.forName("utf-8").newDecoder();
                    ByteBuffer bb = ByteBuffer.wrap(_bytes);
                    CharBuffer cc = decoder.decode(bb);
                    _chars = new char[cc.limit()];
                    System.arraycopy(cc.array(), 0, _chars, 0, cc.limit());
                    break;
                case FILE:
                    assert( _file_name != null && _file != null );
                    Reader r = new FileReader(_file);

                    long len = _file.length();
                    if (len > BUFFER_SIZE_LIMIT) {
                        System.out.println("ERROR: the file "+_file_name+" is too long ("+len+") to load into a buffer.");
                    }
                    else {
                        _chars = new char[(int)len];
                        int len2 = r.read(_chars);
                        assert(len2 == len);
                    }
                    break;
                case CHARS:
                default:
                    throw new IllegalStateException("you can't ask for chars until there's data available");
                }
            }
            return _chars;
        }
        UnifiedInputStreamX getByteStreamOverBuffer() throws IOException
        {
            byte[] bytes = getBytes();
            UnifiedInputStreamX uis = UnifiedInputStreamX.makeStream(bytes, 0, bytes.length);
            return uis;
        }
        UnifiedInputStreamX getByteStreamOverStream() throws IOException {
            UnifiedInputStreamX uis;
            switch(_source) {
            case BYTES:
                uis = UnifiedInputStreamX.makeStream(new ByteArrayInputStream(_bytes, 0, _bytes.length));
                break;
            case FILE:
                assert( _file_name != null && _file != null );
                InputStream in = new FileInputStream(_file);
                uis = UnifiedInputStreamX.makeStream(in);
                break;
            case CHARS:
                byte[] bytes = getBytes();
                uis = UnifiedInputStreamX.makeStream(new ByteArrayInputStream(bytes, 0, bytes.length));
                break;
            default:
                throw new IllegalStateException("source of data is unknown");
            }
            return uis;
        }
        InputStream getInputStream() throws IOException {
            InputStream is;
            switch(_source) {
            case BYTES:
                is = new ByteArrayInputStream(_bytes, 0, _bytes.length);
                break;
            case FILE:
                assert( _file_name != null && _file != null );
                is = new FileInputStream(_file);
                break;
            case CHARS:
                byte[] bytes = getBytes();
                is = new ByteArrayInputStream(bytes, 0, bytes.length);
                break;
            default:
                throw new IllegalStateException("source of data is unknown");
            }
            return is;
        }
        UnifiedInputStreamX getCharStreamOverBuffer() throws IOException {
            char[] chars = getChars();
            UnifiedInputStreamX uis = UnifiedInputStreamX.makeStream(chars);
            return uis;
        }
        UnifiedInputStreamX getCharStreamOverStream() throws IOException {
            UnifiedInputStreamX uis;
            switch(_source) {
            case BYTES:
                char[] chars = getChars();
                uis = UnifiedInputStreamX.makeStream(new CharArrayReader(chars, 0, chars.length));
                break;
            case FILE:
                assert( _file_name != null && _file != null );
                Reader r = new FileReader(_file);
                uis = UnifiedInputStreamX.makeStream(r);
                break;
            case CHARS:
                uis = UnifiedInputStreamX.makeStream(new CharArrayReader(_chars, 0, _chars.length));
                break;
            default:
                throw new IllegalStateException("source of data is unknown");
            }
            return uis;
        }

        void scan_all(String testName) throws IOException
        {
            scan_all(testName, true);
        }
        long scan_all(String testName, boolean materialize) throws IOException
        {
            long count1 = -1, count2 = -1, count3 = -1, count4 = -1;
            count1 = scan_cbuf(testName, materialize);
            count2 = scan_bbuf(testName, materialize);
            count3 = scan_crdr(testName, materialize);
            count4 = scan_brdr(testName, materialize);

            long count = count1;
            if (count2 != -1) count = count2;
            if (count3 != -1) count = count3;
            if (count4 != -1) count = count4;

            return count;
        }

        long scan_cbuf(String testName, boolean materialize) throws IOException
        {
            UnifiedInputStreamX uis;
            IonReaderTextSystemX sysr;

            uis = getCharStreamOverBuffer();
            sysr = new IonReaderTextSystemX(uis);
            long count = scan_reader(sysr, testName+" scan_cbuf", materialize);

            return count;
        }

        long scan_bbuf(String testName, boolean materialize) throws IOException
        {
            UnifiedInputStreamX uis;
            IonReaderTextSystemX sysr;

            uis = getByteStreamOverBuffer();
            sysr = new IonReaderTextSystemX(uis);
            long count = scan_reader(sysr, testName+" scan_bbuf", materialize);

            return count;
        }
        long scan_crdr(String testName, boolean materialize) throws IOException
        {
            UnifiedInputStreamX uis;
            IonReaderTextSystemX sysr;

            uis = getCharStreamOverStream();
            sysr = new IonReaderTextSystemX(uis);
            long count = scan_reader(sysr, testName+" scan_crdr", materialize);

            return count;
        }
        long scan_brdr(String testName, boolean materialize) throws IOException
        {
            UnifiedInputStreamX uis;
            IonReaderTextSystemX sysr;

            uis = getByteStreamOverStream();
            sysr = new IonReaderTextSystemX(uis);
            long count = scan_reader(sysr, testName+" scan_brdr", materialize);

            return count;
        }

    }


    /**
     * compare readers over equiv files.  Equiv files have lists
     * (either LIST or SEXP) of values that should be the same.
     * The same type, annotations, and values.
     *
     * @param r_old
     * @param r_new
     * @throws IOException
     */
    public static void compare_equiv(IonReader r_old, IonReader r_new)
        throws IOException
    {
        long count = 0;

        while (compare_hasNext(r_old, r_new))
        {
            count++;
            IonType t = compare_next(r_old, r_new);

            if (compare_isNull(r_old, r_new)) {
                continue;
            }

            switch (t) {
            default:
                throw new IllegalStateException("we shouldn't ever get here");
            case LIST:
            case SEXP:
                r_old.stepIn();
                r_new.stepIn();
                compare_equiv_values(r_old, r_new, false);
                r_old.stepOut();
                r_new.stepOut();
                break;
            }
        }
    }

    public static void compare_equiv_values(IonReader r_old, IonReader r_new, boolean inStruct)
        throws IOException
    {
        int count = 0;
        String fname = null;

        while (compare_hasNext(r_old, r_new))
        {
            count++;
            IonType t = compare_next(r_old, r_new);

            boolean in_old = r_old.isInStruct();
            boolean in_new = r_new.isInStruct();
            if (in_old != inStruct) {
                in_old = r_old.isInStruct();
                String message = "is_in_struct mismatch local: "+inStruct+", old: "+in_old;
                System.out.println(message);
                throw new IOException(message);
            }
            if (in_new != inStruct) {
                in_new = r_new.isInStruct();
                String message = "is_in_struct mismatch local: "+inStruct+", new: "+in_new;
                System.out.println(message);
                throw new IOException(message);
            }
            if (inStruct) {
                in_old = r_old.isInStruct();
                in_new = r_new.isInStruct();
                assert(in_old == in_new);

                if (fname == null) {
                    fname = r_old.getFieldName();
                }
                String fn_old = r_old.getFieldName();
                String fn_new = r_new.getFieldName();

                if (!fname.equals(fn_old)) {
                    fn_old = r_old.getFieldName();
                    String message = "old field name \""
                                    + fn_old
                                    + "\" doesn't match orig \""
                                    + fname
                                    + "\"";
                    System.out.println(message);
                }
                if (!fname.equals(fn_new)) {
                    fn_new = r_new.getFieldName();
                    String message = "new field name \""
                                    + fn_new
                                    + "\" doesn't match orig \""
                                    + fname
                                    + "\"";
                    fn_new = r_new.getFieldName();
                    System.out.println(message);
                }
            }
            String[] a_old = r_old.getTypeAnnotations();
            String[] a_new = r_new.getTypeAnnotations();
            compare_annotations(a_old, a_new);

            if (compare_isNull(r_old, r_new)) {
                compare_equiv_nulls(r_old, r_new, a_old);
                continue;
            }
            switch (t) {
                case BOOL:
                    compare_equiv_bool(r_old, r_new);
                    break;
                case INT:
                    compare_equiv_int(r_old, r_new);
                    break;
                case FLOAT:
                    compare_equiv_float(r_old, r_new);
                    break;
                case DECIMAL:
                    compare_equiv_decimal(r_old, r_new);
                    break;
                case TIMESTAMP:
                    compare_equiv_timestamp(r_old, r_new);
                    break;
                case SYMBOL:
                    compare_equiv_symbol(r_old, r_new);
                    break;
                case STRING:
                    compare_equiv_string(r_old, r_new);
                    break;
                case CLOB:
                case BLOB:
                    compare_equiv_lob(r_old, r_new);
                    break;
                case STRUCT:
                    r_old.stepIn();
                    r_new.stepIn();
                    compare(r_old, r_new, 2);
                    r_old.stepOut();
                    r_new.stepOut();
                    break;
                case SEXP:
                    r_old.stepIn();
                    r_new.stepIn();
                    compare(r_old, r_new, 2);
                    r_old.stepOut();
                    r_new.stepOut();
                    break;
                default:
                    throw new IllegalStateException("we shouldn't ever get here");
            }
        }

        if (count < 1) {
            String message = "equiv lists should have SOME values";
            throw new IOException(message);
        }

    }
    public static void compare_equiv_nulls(IonReader r_old, IonReader r_new, String[] a_old)
        throws IOException
    {
        int count = 0;

        IonType t_orig = r_old.getType();  // which has to be the same for r_new since that was test in compare_next()
        assert(t_orig.equals(r_new.getType()));

        String[] a_orig = r_new.getTypeAnnotations();

        while (compare_hasNext(r_old, r_new))
        {
            count++;

            IonType t = compare_next(r_old, r_new);
            if (!t_orig.equals(t)) {
                String message = "type of value["
                    + count
                    + "] "
                    + t
                    +" doesn't match first type "
                    + t_orig;
                throw new IOException(message);
            }
            if (!compare_isNull(r_old, r_new)) {
                String message = "value ["
                    + count
                    + "] "
                    +" should be null";
                throw new IOException(message);
            }
            compare_annotations(a_orig, r_old.getTypeAnnotations());
            compare_annotations(a_orig, r_new.getTypeAnnotations());
        }
        return;
    }
    public static void compare_equiv_bool(IonReader r_old, IonReader r_new)
        throws IOException
    {
        boolean n1 = r_old.booleanValue();
        boolean n2 = r_new.booleanValue();

        String[] a_orig = r_new.getTypeAnnotations();
        IonType t_orig = r_old.getType();
        if (n1 != n2) {
            String message = t_orig.toString()
                            + " value differs on first instance: old: "
                            + n1
                            + " new: "
                            + n2;
            throw new IOException(message);
        }

        int count = 0;
        while (compare_hasNext(r_old, r_new))
        {
            count++;

            IonType t = compare_next(r_old, r_new);
            if (!t_orig.equals(t)) {
                String message = "type of value["
                    + count
                    + "] "
                    + t
                    +" doesn't match first type "
                    + t_orig;
                throw new IOException(message);
            }

            compare_annotations(a_orig, r_old.getTypeAnnotations());
            compare_annotations(a_orig, r_new.getTypeAnnotations());

            if (compare_isNull(r_old, r_new)) {
                String message = t_orig.toString()
                                + " value ["
                                + count
                                + "] "
                                +" should be NOT null";
                throw new IOException(message);
            }
            n2 = r_old.booleanValue();
            if (n1 != n2) {
                String message = t_orig.toString()
                                + " value differs on instance ["
                                + count
                                + "]: orig: "
                                + n1
                                + " old: "
                                + n2;
                throw new IOException(message);
            }
            n2 = r_new.booleanValue();
            if (n1 != n2) {
                String message = t_orig.toString()
                                + " value differs on instance ["
                                + count
                                + "]: orig: "
                                + n1
                                + " new: "
                                + n2;
                throw new IOException(message);
            }
        }
        return;
    }
public static void compare_equiv_int(IonReader r_old, IonReader r_new)
    throws IOException
{
    long n1 = r_old.longValue();
    long n2 = r_new.longValue();

    String[] a_orig = r_new.getTypeAnnotations();
    IonType t_orig = r_old.getType();
    if (n1 != n2) {
        String message = t_orig.toString()
                        + " value differs on first instance: old: "
                        + n1
                        + " new: "
                        + n2;
        throw new IOException(message);
    }

    int count = 0;
    while (compare_hasNext(r_old, r_new))
    {
        count++;

        IonType t = compare_next(r_old, r_new);
        if (!t_orig.equals(t)) {
            String message = "type of value["
                + count
                + "] "
                + t
                +" doesn't match first type "
                + t_orig;
            throw new IOException(message);
        }

        compare_annotations(a_orig, r_old.getTypeAnnotations());
        compare_annotations(a_orig, r_new.getTypeAnnotations());

        if (compare_isNull(r_old, r_new)) {
            String message = t_orig.toString()
                            + " value ["
                            + count
                            + "] "
                            +" should be NOT null";
            throw new IOException(message);
        }
        n2 = r_old.longValue();
        if (n1 != n2) {
            String message = t_orig.toString()
                            + " value differs on instance ["
                            + count
                            + "]: orig: "
                            + n1
                            + " old: "
                            + n2;
            throw new IOException(message);
        }
        n2 = r_new.longValue();
        if (n1 != n2) {
            String message = t_orig.toString()
                            + " value differs on instance ["
                            + count
                            + "]: orig: "
                            + n1
                            + " new: "
                            + n2;
            throw new IOException(message);
        }
    }
    return;
}
public static void compare_equiv_float(IonReader r_old, IonReader r_new)
    throws IOException
{
    double n1 = r_old.doubleValue();
    double n2 = r_new.doubleValue();
    String[] a_orig = r_new.getTypeAnnotations();
    IonType t_orig = r_old.getType();
    if (n1 != n2) {
        String message = t_orig.toString()
                        + " value differs on first instance: old: "
                        + n1
                        + " new: "
                        + n2;
        throw new IOException(message);
    }

    int count = 0;
    while (compare_hasNext(r_old, r_new))
    {
        count++;

        IonType t = compare_next(r_old, r_new);
        if (!t_orig.equals(t)) {
            String message = "type of value["
                + count
                + "] "
                + t
                +" doesn't match first type "
                + t_orig;
            throw new IOException(message);
        }

        compare_annotations(a_orig, r_old.getTypeAnnotations());
        compare_annotations(a_orig, r_new.getTypeAnnotations());

        if (compare_isNull(r_old, r_new)) {
            String message = t_orig.toString()
                            + " value ["
                            + count
                            + "] "
                            +" should be NOT null";
            throw new IOException(message);
        }
        n2 = r_old.doubleValue();
        if (n1 != n2) {
            String message = t_orig.toString()
                            + " value differs on instance ["
                            + count
                            + "]: orig: "
                            + n1
                            + " old: "
                            + n2;
            throw new IOException(message);
        }
        n2 = r_new.doubleValue();
        if (n1 != n2) {
            String message = t_orig.toString()
                            + " value differs on instance ["
                            + count
                            + "]: orig: "
                            + n1
                            + " new: "
                            + n2;
            throw new IOException(message);
        }
    }
    return;
}
public static void compare_equiv_decimal(IonReader r_old, IonReader r_new)
    throws IOException
{
    BigDecimal n1 = r_old.bigDecimalValue();
    BigDecimal n2 = r_new.bigDecimalValue();

    String[] a_orig = r_new.getTypeAnnotations();
    IonType t_orig = r_old.getType();
    if (!n1.equals(n2)) {
        String message = t_orig.toString()
                        + " value differs on first instance: old: "
                        + n1
                        + " new: "
                        + n2;
        throw new IOException(message);
    }

    int count = 0;
    while (compare_hasNext(r_old, r_new))
    {
        count++;

        IonType t = compare_next(r_old, r_new);
        if (!t_orig.equals(t)) {
            String message = "type of value["
                + count
                + "] "
                + t
                +" doesn't match first type "
                + t_orig;
            throw new IOException(message);
        }

        compare_annotations(a_orig, r_old.getTypeAnnotations());
        compare_annotations(a_orig, r_new.getTypeAnnotations());

        if (compare_isNull(r_old, r_new)) {
            String message = t_orig.toString()
                            + " value ["
                            + count
                            + "] "
                            +" should be NOT null";
            throw new IOException(message);
        }
        n2 = r_old.bigDecimalValue();
        if (!n1.equals(n2)) {
            String message = t_orig.toString()
                            + " value differs on instance ["
                            + count
                            + "]: orig: "
                            + n1
                            + " old: "
                            + n2;
            throw new IOException(message);
        }
        n2 = r_new.bigDecimalValue();
        if (!n1.equals(n2)) {
            String message = t_orig.toString()
                            + " value differs on instance ["
                            + count
                            + "]: orig: "
                            + n1
                            + " new: "
                            + n2;
            throw new IOException(message);
        }
    }
    return;
}
public static void compare_equiv_timestamp(IonReader r_old, IonReader r_new)
    throws IOException
{
    Timestamp n1 = r_old.timestampValue();
    Timestamp n2 = r_new.timestampValue();
    Timestamp n3;

    String[] a_orig = r_new.getTypeAnnotations();
    IonType t_orig = r_old.getType();
    if (!n1.equals(n2)) {
        String message = t_orig.toString()
                        + " value differs on first instance: old: "
                        + n1
                        + " new: "
                        + n2;
        throw new IOException(message);
    }

    int count = 0;
    while (compare_hasNext(r_old, r_new))
    {
        count++;

        IonType t = compare_next(r_old, r_new);
        if (!t_orig.equals(t)) {
            String message = "type of value["
                + count
                + "] "
                + t
                +" doesn't match first type "
                + t_orig;
            throw new IOException(message);
        }

        compare_annotations(a_orig, r_old.getTypeAnnotations());
        compare_annotations(a_orig, r_new.getTypeAnnotations());

        if (compare_isNull(r_old, r_new)) {
            String message = t_orig.toString()
                            + " value ["
                            + count
                            + "] "
                            +" should be NOT null";
            throw new IOException(message);
        }
        n2 = r_old.timestampValue();
        n3 = r_new.timestampValue();
        if (n1.compareTo(n2) != 0) {
            n1.compareTo(n2);
            String message = t_orig.toString()
                            + " value differs on instance ["
                            + count
                            + "]: orig: "
                            + n1
                            + " old: "
                            + n2;
            System.out.println(message);
            throw new IOException(message);
        }

        if (n1.compareTo(n3) != 0) {
            n1.compareTo(n3);
            String message = t_orig.toString()
                            + " value differs on instance ["
                            + count
                            + "]: orig: "
                            + n1
                            + " new: "
                            + n3;
            System.out.println(message);
            throw new IOException(message);
        }
    }
    return;
}
static int compare_equiv_string_count = 0;
public static void compare_equiv_string(IonReader r_old, IonReader r_new)
    throws IOException
{
    String n1 = r_old.stringValue();
    String n2 = r_new.stringValue();

    String[] a_orig = r_new.getTypeAnnotations();
    IonType t_orig = r_old.getType();
    if (!n1.equals(n2)) {
        String message = t_orig.toString()
                        + " value differs on first instance: old: "
                        + n1
                        + " new: "
                        + n2;
        throw new IOException(message);
    }


    while (compare_hasNext(r_old, r_new))
    {
        compare_equiv_string_count++;
        if (compare_equiv_string_count == 57) {
            compare_equiv_string_count += 0;
        }

        IonType t = compare_next(r_old, r_new);
        if (!t_orig.equals(t)) {
            String message = "type of value["
                + compare_equiv_string_count
                + "] "
                + t
                +" doesn't match first type "
                + t_orig;
            throw new IOException(message);
        }

        compare_annotations(a_orig, r_old.getTypeAnnotations());
        compare_annotations(a_orig, r_new.getTypeAnnotations());

        if (compare_isNull(r_old, r_new)) {
            String message = t_orig.toString()
                            + " value ["
                            + compare_equiv_string_count
                            + "] "
                            +" should be NOT null";
            throw new IOException(message);
        }
        n2 = r_old.stringValue();
        if (!n1.equals(n2)) {
            String message = t_orig.toString()
                            + " value differs on instance ["
                            + compare_equiv_string_count
                            + "]: orig: "
                            + n1
                            + " old: "
                            + n2;
            System.out.println("string from old reader error: "+message);
            throw new IOException(message);
        }
        n2 = r_new.stringValue();
        if (!n1.equals(n2)) {
            String message = t_orig.toString()
                            + " value differs on instance ["
                            + compare_equiv_string_count
                            + "]: orig: "
                            + n1
                            + " new: "
                            + n2;
            throw new IOException(message);
        }
    }
    return;
}
public static void compare_equiv_symbol(IonReader r_old, IonReader r_new)
    throws IOException
{
    String n1 = r_old.stringValue();
    String n2 = r_new.stringValue();

    String[] a_orig = r_new.getTypeAnnotations();
    IonType t_orig = r_old.getType();
    if (!n1.equals(n2)) {
        String message = t_orig.toString()
                        + " value differs on first instance: old: "
                        + n1
                        + " new: "
                        + n2;
        throw new IOException(message);
    }

    int count = 0;
    while (compare_hasNext(r_old, r_new))
    {
        count++;

        IonType t = compare_next(r_old, r_new);
        if (!t_orig.equals(t)) {
            String message = "type of value["
                + count
                + "] "
                + t
                +" doesn't match first type "
                + t_orig;
            throw new IOException(message);
        }

        compare_annotations(a_orig, r_old.getTypeAnnotations());
        compare_annotations(a_orig, r_new.getTypeAnnotations());

        if (compare_isNull(r_old, r_new)) {
            String message = t_orig.toString()
                            + " value ["
                            + count
                            + "] "
                            +" should be NOT null";
            throw new IOException(message);
        }
        n2 = r_old.stringValue();
        if (!n1.equals(n2)) {
            String message = t_orig.toString()
                            + " value differs on instance ["
                            + count
                            + "]: orig: "
                            + n1
                            + " old: "
                            + n2;
            throw new IOException(message);
        }
        n2 = r_new.stringValue();
        if (!n1.equals(n2)) {
            String message = t_orig.toString()
                            + " value differs on instance ["
                            + count
                            + "]: orig: "
                            + n1
                            + " new: "
                            + n2;
            throw new IOException(message);
        }
    }
    return;
}
public static void compare_equiv_lob(IonReader r_old, IonReader r_new)
    throws IOException
{
    byte[] n1 = r_old.newBytes();
    byte[] n2 = r_new.newBytes();

    String[] a_orig = r_new.getTypeAnnotations();
    IonType t_orig = r_old.getType();
    if (!compare_buffers_equal(n1, n2)) {
        String message = t_orig.toString()
                        + " value differs on first instance: old: "
                        + n1
                        + " new: "
                        + n2;
        throw new IOException(message);
    }

    int count = 0;
    while (compare_hasNext(r_old, r_new))
    {
        count++;

        IonType t = compare_next(r_old, r_new);
        if (!t_orig.equals(t)) {
            String message = "type of value["
                + count
                + "] "
                + t
                +" doesn't match first type "
                + t_orig;
            throw new IOException(message);
        }

        compare_annotations(a_orig, r_old.getTypeAnnotations());
        compare_annotations(a_orig, r_new.getTypeAnnotations());

        if (compare_isNull(r_old, r_new)) {
            String message = t_orig.toString()
                            + " value ["
                            + count
                            + "] "
                            +" should be NOT null";
            throw new IOException(message);
        }
        n2 = r_old.newBytes();
        if (!compare_buffers_equal(n1, n2)) {
            String message = t_orig.toString()
                            + " value differs on instance ["
                            + count
                            + "]: orig: "
                            + n1
                            + " old: "
                            + n2;
            throw new IOException(message);
        }
        n2 = r_new.newBytes();
        if (!compare_buffers_equal(n1, n2)) {
            String message = t_orig.toString()
                            + " value differs on instance ["
                            + count
                            + "]: orig: "
                            + n1
                            + " new: "
                            + n2;
            throw new IOException(message);
        }
    }
    return;
}

static void traverse(IonReader reader) {
    if (reader.hasNext()) {
        IonType t = reader.next();
        switch (t) {
        case STRUCT:
        case LIST:
        case SEXP:
            reader.stepIn();
            traverse(reader);
            reader.stepOut();
            break;
        default:
            break;
        }
    }
}

static void open_and_traverse(String text) throws IOException {
    IonBinaryWriter w = sys.newBinaryWriter();
    IonReader r = sys.newReader(text);
    w.writeValues(r);
    byte[] b = w.getBytes();
    IonReader reader = sys.newReader(b);

    traverse(reader);
}

static void system_reader_test() throws IOException {
    String text =
        "A::{data:B::{items:[C::{itemPromos:[D::{f4:['''12.5''']}]}]}}";
    //startIteration(text);

    text = "D::{f4:['''12.5''']}";
    open_and_traverse(text);

    text = "[D::{f4:['''12.5''']}]";
    open_and_traverse(text);

    text = "[C::{itemPromos:[D::{f4:['''12.5''']}]}]";
    open_and_traverse(text);

    text = "B::{items:[C::{itemPromos:[D::{f4:['''12.5''']}]}]}";
    open_and_traverse(text);

    text = "{data:B::{items:[C::{itemPromos:[D::{f4:['''12.5''']}]}]}}";
    open_and_traverse(text);

    text = "A::{data:B::{items:[C::{itemPromos:[  D::{f4:['''12.5''']}]  }]}}";
    open_and_traverse(text);

}

static void assertFalse(boolean t) {
    assert t == false;
}

static void assertEquals(String s1, String s2) {
    if (s1 == null && s2 == null) return;
    assert s1.equals(s2);
}

}
