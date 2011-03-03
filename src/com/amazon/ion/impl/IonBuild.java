// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonSystemBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class IonBuild
{
    static final int argid_VERSION = 2;
    static final int argid_HELP    = 3;
    static final int argid_INVALID = -1;

    static boolean printHelp    = false;
    static boolean printVersion = false;
    static String  errorMessage = null;


    /**
     * This main simply prints the version information to
     * allow users to identify the build version of the Jar.
     *
     * @param args user command line flags
     */
    public static void main(String[] args) throws IOException
    {
        process_command_line(args);

        if (printVersion) {
            doPrintVersion();
        }
        if (printHelp) {
            doPrintHelp();
        }
    }

    private static void process_command_line(String[] args)
    {
        if (args.length == 0) printHelp = true;

        for (int ii=0; ii<args.length; ii++) {
            String arg = args[ii];
            if (arg == null || arg.length() < 1) continue;
            int argid = getArgumentId(arg);
            switch (argid) {
            case argid_VERSION:
                printVersion = true;
                break;
            case argid_HELP:
                printHelp = true;
                break;
            default:
                invalid_arg(ii, arg);
                break;
            }
        }
        return;
    }
    private static int getArgumentId(String arg)
    {
        if (arg.startsWith("-") && arg.length() == 2) {
            switch (arg.charAt(1)) {
            case 'h': case '?':
                return argid_HELP;
            case 'v':
                return argid_VERSION;
            default:
                return argid_INVALID;
            }
        }
        if (arg.startsWith("--") && arg.length() > 2) {
            if (arg.equals("help")) {
                return argid_HELP;
            }
            if (arg.equals("version")) {
                return argid_VERSION;
            }
        }
        return argid_INVALID;
    }
    private static void invalid_arg(int ii, String arg)
    {
        errorMessage += "\narg["+ii+"] \""+arg+"\" is unrecognized or invalid.";
        printHelp = true;
    }

    private static void doPrintHelp() {
        System.out.println("IonJava -- Copyright (c) 2010-2011 Amazon.com");
        System.out.println("usage: java -jar IonJava.jar <options>");
        System.out.println("options:");
        System.out.println("-v (--version) prints current version entry");
        System.out.println("-h (--help)    prints this helpful message");
        if (errorMessage != null) {
            System.out.println();
            System.out.println(errorMessage);
        }
    }

    private static Properties loadBuildProperties() throws IOException
    {
        Properties props = new Properties();

        InputStream in = IonBuild.class.getResourceAsStream("build.properties");
        try
        {
            props.load(in);
        }
        finally
        {
            in.close();
        }

        return props;
    }


    private static void copyProperty(IonStruct struct, Properties props,
                                     String name)
    {
        String value = props.getProperty(name, "");
        if (value.length() != 0)
        {
            struct.add(name).newString(value);
        }
    }

    private static void doPrintVersion() throws IOException
    {
        Properties props = loadBuildProperties();

        IonSystem sys = IonSystemBuilder.defaultBuilder().build();
        IonStruct v = sys.newEmptyStruct();

        if (printVersion) {
            copyProperty(v, props, "release_label");
            copyProperty(v, props, "brazil_package_version");
            copyProperty(v, props, "build_time");
        }

        IonWriterUserText.TextOptions options = new IonWriterUserText.TextOptions(
                 true  // boolean prettyPrint
                ,true  // boolean printAscii
                ,true  // boolean filterOutSymbolTables
                ,false // boolean suppressIonVersionMarker
        );

        IonWriter w = IonWriterFactory.makeWriter(sys, (Appendable)System.out, options);
        IonReader r = sys.newReader(v);
        w.writeValues(r);
        w.finish();
        System.out.println();
    }
}
