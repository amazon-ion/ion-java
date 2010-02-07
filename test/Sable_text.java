// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.



import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.IonReaderFactoryX;
import com.amazon.ion.impl.IonTextWriter;
import com.amazon.ion.system.SimpleCatalog;
import com.amazon.ion.system.SystemFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;


/**
 *
 * @author krystia
 */
public class Sable_text
{
    static final int binary_count = 1;
    static final int text_count   = 1;

    static final boolean x_reader = false;
    static final boolean id_lookup = false;

    static String title = "x_reader: "+x_reader+", id_lookup: "+id_lookup;


    private static final String SERVER             = "http://sable-http.integ.amazon.com:18700";
    private static final String HAS_CONDITION_NOTE = "/contribution/v1/1/0439785960/527693540/9R-J2DA-CXV6";
    private static final String NO_CONDITION_NOTE  = "/contribution/v1/1/0439785960/521559250/521559250-1179267400822";
    private static final String BINARY             = "application/x-amzn-ion";
    private static final String TEXT               = "text/x-amzn-ion";

    static IonSystem system = buildSystem();
    static IonReaderFactoryX factory = new IonReaderFactoryX();


    /**
    * @param args
    */
    public static void main(String[] args) throws Exception {

        Sable_text test = new Sable_text();

        for (int ii=0; ii<binary_count; ii++) {
            test.getConditionNote(SERVER + HAS_CONDITION_NOTE, BINARY);
            test.getConditionNote(SERVER + NO_CONDITION_NOTE,  BINARY);
        }
        for (int ii=0; ii<text_count; ii++) {
            test.getConditionNote(SERVER + HAS_CONDITION_NOTE, TEXT);
            test.getConditionNote(SERVER + NO_CONDITION_NOTE,  TEXT);
        }
    }

    /**
    * @throws Exception
    */
    public Sable_text() throws Exception {
        //system = buildSystem();
    }

    /**
    * @return
    * @throws FileNotFoundException
    * @throws IOException
    */
    private static IonSystem buildSystem()
    {
        SimpleCatalog catalog = new SimpleCatalog();
        IonSystem system = SystemFactory.newSystem(catalog);

System.out.println("loading symbol table catalog");
int count = 0;

        // Symbol tables copped from the IOPMetadataBucket package
        InputStream is = null;
        try {
            is = new FileInputStream(new File("c:\\data\\samples\\sable\\symbol_tables.ion"));
            //is = new FileInputStream(new File("c:\\data\\samples\\sable\\symbol_tables.10n"));

            IonReader reader = null;  // = system.newReader(is);
            if (x_reader) {
                reader = IonReaderFactoryX.makeReader(system, is);
            }
            else {
                reader = system.newReader(is);
            }


            while (reader.hasNext()) {
                /*
                * The symbol tables are wrapped in some custom boilerplate;
                * need to strip it off here...
                */
                IonType t = reader.next();
                if (t != IonType.STRUCT) {
System.out.println("WARNING: non-struct ("+t.toString()+") found in catalog");
                    continue;
                }
                reader.stepIn();
                IonType t1 = null;
                while (reader.hasNext()) {
                    t1 = reader.next();
                    if (t1 != IonType.STRUCT) {
                        continue;
                    }
                    if ("symbol_table".equals(reader.getFieldName())) {
                        break;
                    }
                    t1 = null;
                }
                if (t1 != IonType.STRUCT) {
System.out.println("WARNING: no symbol_table field found in struct of symbol table definition");
                    continue;
                }
                //while (!"symbol_table_version".equals(reader.getFieldName()) && reader.hasNext()) {
                //    reader.next();
                //}

                SymbolTable symtab = system.newSharedSymbolTable(reader, true);

System.out.println("loaded: "+symtab.getName()+", ver: "+symtab.getVersion()+", max_id: "+symtab.getMaxId());
count++;

                reader.stepOut();
                catalog.putTable(symtab);
            }
        }
        catch (Exception ie) {
            System.out.println("ERROR: exception encountered loading catalog: "+ie.getStackTrace().toString());
            throw new RuntimeException(ie);
        }
        finally {
            if (is != null) {
                try {
                    is.close();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

System.out.println("catalog loaded "+count+" symbol tables");

        return system;
    }


    public void getConditionNote(String url, String encoding) throws Exception
    {
        System.out.println(url + " (" + encoding + "):");

        URL u = new URL(url);
        HttpURLConnection c= (HttpURLConnection) u.openConnection();
        c.addRequestProperty("authorization", "SableBasic guest");
        c.addRequestProperty("accept", encoding);

        byte[] buffer = new byte[5000];
        InputStream in = c.getInputStream();
        int len = in.read(buffer, 0, buffer.length);
        IonReader reader = null;


        try {
            long start, finish, used = 0, count = 0;
            String s = "no-op";

            do {
                if (x_reader) {
                    reader = IonReaderFactoryX.makeReader(system, buffer, 0, len);
                }
                else {
                    reader = system.newReader(buffer, 0, len);
                }

                start = System.currentTimeMillis();
                // s = makeString(reader);
                s = readOfferConditionNote(reader);
                count++;
                finish = System.currentTimeMillis();
                used += finish - start;
            } while(used < 100);

            long average = (used *1000) / count;
            System.out.println(((double)average)/1000+ " ms over "+count+" calls, using "+title);
            System.out.println(s);
        }
        catch (RuntimeException e) {
            e.printStackTrace(System.out);
        }
        System.out.print("reader class: ");
        if (reader == null) {
            System.out.print("<reader is null>");
        }
        else {
            System.out.print(reader.getClass().getSimpleName());
        }
        System.out.println();
        System.out.println();
    }

    /**
    * @param r reader positioned before the com.amazon.item_master.Contribution@0.1 struct
    * @return datagram[0].product.condition_note[0].value
    */
    public static final String readOfferConditionNote(IonReader r)
    {
        if (!r.hasNext()) {
            return "ERROR: empty datagram";
        }
        if (IonType.STRUCT != r.next()) {
            return "ERROR: datagram is not a struct";
        }
        r.stepIn(); // step into the Contribution struct

        int product = 0;
        int condition_note = 0;
        int value = 0;

        if (id_lookup) {
            SymbolTable symtab = r.getSymbolTable();
            product        = symtab.findSymbol("product");
            condition_note = symtab.findSymbol("condition_note");
            value          = symtab.findSymbol("value");
            if (product < 1 || condition_note < 1 || value < 1) {
                return "ERROR: symbol not in symbol table";
            }
        }

        // find the "product" field in the Contribution
        IonType found;
        if (id_lookup) {
            found = skipToMember(r, product);
        }
        else {
            found = skipToMember(r, "product");
        }
        if (found == null) {
            return "ERROR: product field not found";
        }
        r.stepIn(); // step into the product

        // find the condition_note field in the product
        if (id_lookup) {
            found = skipToMember(r, condition_note);
        }
        else {
            found = skipToMember(r, "condition_note");
        }
        if (found == null) {
            return "ERROR: condition_note field not found";
        }
        r.stepIn(); // step into the product

        // and step into the first element of the condition note list
        if (!r.hasNext()) {
            return "ERROR: no elements in condition note";
        }
        if (IonType.STRUCT != r.next()) {
            return "ERROR: condition note element is not a struct";
        }


        // step into the 0th condition note (a struct)
        r.stepIn();

        // get the value member of the condition_note[0] struct
        if (id_lookup) {
            found = skipToMember(r, value);
        }
        else {
            found = skipToMember(r, "value");
        }
        if (found == null) {
            return "ERROR: value field not found";
        }

        // return the value as a string
        switch (found) {
            case STRING:
            case SYMBOL:
                return r.stringValue();
            default:
                return null;
        }
    }
    static final IonType skipToMember(IonReader r, String fieldname)
    {
        while (r.hasNext()) {
            IonType t = r.next();
            String f = r.getFieldName();
            if (fieldname.equals(f)) {
                return t;
            }
        }
        return null;
    }
    static final IonType skipToMember(IonReader r, int fieldid)
    {
        while (r.hasNext()) {
            IonType t = r.next();
            if (fieldid == r.getFieldId()) {
                return t;
            }
        }
        return null;
    }

    public static final String makeString(IonReader r) throws Exception
    {
        StringBuilder container = new StringBuilder(1000);
        //IonDatagram container = system.newDatagram();

        IonWriter w = new IonTextWriter(container, true /* prettyPrint */, false /* printAscii */);
        w.writeValues(r);

        return container.toString();
    }
}
