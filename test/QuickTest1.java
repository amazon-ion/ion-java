import com.amazon.ion.IonBinaryWriter;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.UnifiedSymbolTable;
import com.amazon.ion.system.SimpleCatalog;
import com.amazon.ion.system.SystemFactory;
import com.amazon.ion.system.SystemFactory.SystemCapabilities;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;


// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

/**
 *
 */
public class QuickTest1
{

    static void testIonV3Regression() throws Exception
    {
        // C:\src\brazil\src\shared\platform\IonTests\mainline\iontestdata\failed
        //   iopg.ion is the symbol table

        IonSystem sys = SystemFactory.newSystem(SystemCapabilities.ORIGINAL);
        IonLoader loader = sys.getLoader();

        String    path = "C:\\src\\brazil\\src\\shared\\platform\\IonTests\\mainline\\iontestdata\\failed\\";
        String    symbolTableName = path + "iopg.ion";
        File      symbolTableFile = new File(symbolTableName);

        IonDatagram dg = loader.load(symbolTableFile);
        IonValue    symbolTableStruct = dg.get(0);
        assert(symbolTableStruct instanceof IonStruct && symbolTableStruct.hasTypeAnnotation(UnifiedSymbolTable.ION_SHARED_SYMBOL_TABLE));

        SymbolTable   symbolTable = UnifiedSymbolTable.makeNewSharedSymbolTable((IonStruct)symbolTableStruct);
        SimpleCatalog cat = new SimpleCatalog();
        cat.putTable(symbolTable);

        IonSystem sysWithCatalog = SystemFactory.newSystem(cat);
        IonLoader loaderWithCatalog = sysWithCatalog.getLoader();

        File dir = new File(path);
        for (String filename : dir.list()) {
            if (filename.contains(".10n") == false) {
                continue;
            }
            File testFile = new File(path+filename);

            System.out.println((testFile.canRead() ? "can read" : "NO READ ACCESS") +": "+ filename);

            IonDatagram value = loaderWithCatalog.load(testFile);
            if (value != null) System.out.println("ok");
        }

    }

    public static void test128()
    {
        String value = "-1.28";

        IonSystem sys = SystemFactory.newSystem(SystemCapabilities.ORIGINAL);
        IonValue v = sys.getLoader().load(value);
        IonValue currency = sys.singleValue(value);
    }

    public static void ivsv3test2() {
        String value = "MarketplaceConfig::{schema:\"marketplaceConfig@1.0\",marketplace_id:12345678,marketplace_name:\"test marketplace\","
        +"state:\"open\",datacenter_groups:[\"IAD\"],publishing_realms:[\"USAmazon\"],landlord_customer_id:12340000,"
        +"domain_id:555,default_currency_code:USD,default_language_tag:en_US,version:1}";

        IonSystem sys = SystemFactory.newSystem(SystemCapabilities.LITE);
        IonStruct s = (IonStruct)sys.singleValue(value);

        IonValue currency = s.get("default_currency_code");
        assert(currency != null);
        assert(currency instanceof IonSymbol);
        assert(((IonSymbol)currency).stringValue().equals("USD"));

    }
    public static void testStruct() {
        IonSystem sys = SystemFactory.newSystem(SystemCapabilities.LITE);
        IonStruct daoConfig = (IonStruct)sys.singleValue(
            "{"
            + "  JdbcDaoConfig:{"
            + "    name:\"configuration\","
            + "    description:\"DAO config for configuration store.\","
            + "    version:1.0,"
            + "    dataColumn:\"data\","
            + "    optimisticCheck:true,"
            + "    keyDefs:["
            + "      {name:\"key\", type:\"string\", column:\"key\"}"
            + "    ],"
            + "    versionDef:{name:\"version\", column:\"version\",},"
            + "    numVersionsToKeep:999,"
            + "  }"
            + "}"
        );

        IonStruct jdbcDaoConfig = (IonStruct) daoConfig.get("JdbcDaoConfig");
        assert(jdbcDaoConfig != null && jdbcDaoConfig.isNullValue() == false);
        IonValue val = jdbcDaoConfig.get("numVersionsToKeep");
        assert(val != null && val.isNullValue() == false);
    }

    static void ioncpputf8() throws IOException
    {
        IonSystem is = SystemFactory.newSystem();

        IonString s1 = is.newString("\\xc3\\xa9");
        System.out.println(s1.toString());

        IonString s2 = is.newString("\u00c3\u00a9");
        System.out.println(s2.toString());

        IonReader ir = is.newReader("\"\\xc3\\xa9\"".getBytes());
        ir.next();
        String s3 = ir.stringValue();
        System.out.println(s3);

        byte[] b = new byte[4];
        b[0] = '\"';
        b[1] = (byte)(0xc3 & 0xff);
        b[2] = (byte)(0xa9 & 0xff);
        b[3] = '\"';
        ir = is.newReader(b);
        ir.next();
        String s4 = ir.stringValue();
        System.out.println(s4);

        return;
    }


    static void chrisss2() throws IOException
    {
        IonSystem is = SystemFactory.newSystem();

        IonReader ir = is.newReader("{a:{b:10}}");
        IonBinaryWriter iw = is.newBinaryWriter();
        ir.next();
        ir.stepIn();
        ir.hasNext();
        iw.stepIn(IonType.STRUCT);
        iw.writeValue(ir);
        System.out.println(is.singleValue(iw.getBytes()));
        // invalid: {a:{a:{},b:10}}
        // expected: {a:{b:10}}
        return;
    }

    static void chrisss() throws IOException
    {
        IonSystem is = SystemFactory.newSystem();

        IonReader ir = is.newReader("{a:{b:10}}");
        IonBinaryWriter iw = is.newBinaryWriter();
        ir.next();
        iw.stepIn(IonType.STRUCT);
        iw.setFieldName("fee");
        iw.writeValue(ir);
        System.out.println(is.singleValue(iw.getBytes()));
    }
    public static void main(String[] args) throws Exception
    {

        testIonV3Regression();

        if (true) return;

        test128();
        ivsv3test2();
        testStruct();
        test_doubles();
        ioncpputf8();

        String x = UnifiedSymbolTable.IMPORTS;
        IonSystem sys3 = SystemFactory.newSystem();


        chrisss2();

        if (true) return;

        IonSystem sys = SystemFactory.newSystem();
        String op = "read"; // args[0];

        if ("read".equals(op))
        {
            String file = "C:\\src\\workspaces\\Ion\\ols.ion"; // args[1];
            BufferedReader br = null;

            if(file.endsWith(".gz")) {
                br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)), "UTF-8"));
            }
            else {
                br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
            }

            Iterator<IonValue> it = sys.iterate(br);

            long start = System.currentTimeMillis();
            System.out.println("start time = "+(start/1000.0));
            long count = 0;
            try {
                while(it.hasNext()) {
                    it.next();
                    count++;
                    if ((count % 10000) == 0) {
                        System.out.print("count = "+count);
                        System.out.println("\t time = "+((System.currentTimeMillis() - start)/1000.0));
//if (count > 400010) // -agentlib:hprof=heap=sites  3 382 212
//    break;
                    }
                }
            }
            catch(Throwable t) {
                t.printStackTrace();
            }
            System.out.println("read "+count);
        }
    }

    static void test_doubles()
    {
        double d_minus = Double.valueOf("-0.0");
        double d_plus = Double.valueOf("0.0");

        if (Double.compare(d_minus, d_plus) == 0) {
            System.out.println("They're EQUAL! (that's bad)");
        }
        else {
            System.out.println("They're not equal! (that's a good thing)");
        }
        return;
    }
}
