package com.amazon.ion.impl;

import java.io.IOException;

import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.SystemFactory;

public class IonBuild 
{
    static final int Major = 1;
    static final int Minor = 8;
    static final int Patch = 3;
    
    static final String CheckInDate    = "2010-06-24T09:56+07:00";
    static final String CheckInComment = "fixed binary stepOut bug Jira 133 - csuver";

    static final String[] History = {
        "{major_version:1,minor_version:8,patch:3,check_in_date:2010-06-24T09:56+07:00,check_in_comment:'''fixed binary stepOut bug Jira 133 - csuver'''}"
       ,"{major_version:1,minor_version:8,patch:2,check_in_date:2010-06-07T08:10+07:00,check_in_comment:'''Initial identified JAR - with lite and fix for reading local symbol table - csuver'''}"
    };
    
    static final String BrazilBuild = "@BRAZIL_VERSION@"; // someday we'll make this work

    static final int argid_HISTORY = 1;
    static final int argid_VERSION = 2;
    static final int argid_HELP    = 3;
    static final int argid_INVALID = -1;
   
    static boolean printHelp    = false;
    static boolean printHistory = false;
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
        
        if (printVersion || printHistory) {
            doPrintVersion();
        }
        if (printHelp) {
            doPrintHelp();
        }
    }
    
    private static void process_command_line(String[] args)
    {        
        for (int ii=0; ii<args.length; ii++) {
            String arg = args[ii];
            if (arg == null || arg.length() < 1) continue;
            int argid = getArgumentId(arg);
            switch (argid) {
            case argid_HISTORY:
                printHistory = true;
                break;
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
            case 'a':
                return argid_HISTORY;
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
            if (arg.equals("history")) {
                return argid_HISTORY;    
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
        System.out.println("IonJava JAR - CSuver");
        System.out.println("Copyright (c) 2010 Amazon.com");
        System.out.println("usage: java -jar IonJava.jar <options>");
        System.out.println("options:");
        System.out.println("-v (--version) - prints current version entry");
        System.out.println("-a (--all) - prints all the history entries");
        System.out.println("-h )--help) - prints this helpful message");
        if (errorMessage != null) {
            System.out.println();
            System.out.println(errorMessage);
        }
    }

    private static void doPrintVersion() throws IOException
    {
        IonSystem sys = SystemFactory.newSystem();
        IonStruct v = sys.newEmptyStruct();
        
        if (printVersion) {
            v.add("major_version", sys.newInt(Major));
            v.add("minor_version", sys.newInt(Minor));
            v.add("patch",         sys.newInt(Patch));
            IonValue t = sys.singleValue(CheckInDate);
            v.add("check_in_date", t);
            v.add("check_in_comment", sys.newString(CheckInComment));
        }
        
        if (printHistory) {
            IonList h = sys.newEmptyList();
            for (int ii=0; ii<History.length; ii++) {
                h.add(sys.singleValue(History[ii]));
            }
            v.add("history", h);
        }

        IonWriterUserText.TextOptions options = new IonWriterUserText.TextOptions(
                 true  // boolean prettyPrint
                ,true  // boolean printAscii
                ,true  // boolean filterOutSymbolTables
                ,false // boolean suppressIonVersionMarker
        );
        v.addTypeAnnotation("IonJava");
        
        IonWriter w = IonWriterFactory.makeWriter(sys, (Appendable)System.out, options);
        IonReader r = sys.newReader(v);
        w.writeValues(r);
        w.flush();
        System.out.println();
    }
    
}
