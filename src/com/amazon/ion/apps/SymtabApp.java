// Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.apps;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 */
public class SymtabApp
    extends BaseApp
{
    private ArrayList<SymbolTable> myImports = new ArrayList<SymbolTable>();
    private ArrayList<String>      mySymbols = new ArrayList<String>();

    private String mySymtabName;
    private int    mySymtabVersion;


    //=========================================================================
    // Static methods

    public static void main(String[] args)
    {
        if (args.length < 1)
        {
            System.err.println("Need one file to build symtab");
            return;
        }

        SymtabApp app = new SymtabApp();
        app.doMain(args);
    }


    //=========================================================================
    // Construction and Configuration

    public SymtabApp()
    {
    }


    //=========================================================================

    public void doMain(String[] args)
    {
        int firstFileIndex = processOptions(args);

        if (mySymtabName == null)
        {
            throw new RuntimeException("Must provide --name");
        }
        // TODO verify that we don't import the same name.

        if (mySymtabVersion == 0)
        {
            mySymtabVersion = 1;
        }


        if (firstFileIndex == args.length)
        {
            System.err.println("Must provide list of files to provide symbols");
        }
        else
        {
            processFiles(args, firstFileIndex);
        }


        SymbolTable[] importArray = new SymbolTable[myImports.size()];
        myImports.toArray(importArray);

        SymbolTable mySymtab =
            mySystem.newSharedSymbolTable(mySymtabName,
                                          mySymtabVersion,
                                          mySymbols.iterator(),
                                          importArray);

        IonWriter w = mySystem.newTextWriter(System.out);
        try
        {
            // TODO ensure IVM is printed
            mySymtab.writeTo(w);
            System.out.println();
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private int processOptions(String[] args)
    {
        for (int i = 0; i < args.length; i++)
        {
            String arg = args[i];
            if ("--catalog".equals(arg))
            {
                String symtabPath = args[++i];
                loadCatalog(symtabPath);
            }
            else if ("--import".equals(arg))
            {
                // We'll use the latest version available.
                String name = args[++i];
                IonCatalog catalog = mySystem.getCatalog();
                SymbolTable table = catalog.getTable(name);
                if (table == null)
                {
                    String message =
                        "There's no symbol table in the catalog named " +
                        name;
                    throw new RuntimeException(message);
                }
                myImports.add(table);
                logDebug("Imported symbol table " + name
                           + "@" + table.getVersion());
            }
            else if ("--name".equals(arg))
            {
                if (mySymtabName != null)
                {
                    throw new RuntimeException("Multiple names");
                }
                mySymtabName = args[++i];
                if (mySymtabName.length() == 0)
                {
                    throw new RuntimeException("Name must not be empty");
                }
            }
            else if ("--version".equals(arg))
            {
                if (mySymtabVersion != 0)
                {
                    throw new RuntimeException("Multiple versions");
                }
                int version = Integer.parseInt(arg);
                if (version < 1)
                {
                    throw new RuntimeException("Version must be at least 1");
                }
                if (version != 1)
                {
                    String message = "Symtab extension not implemented";
                    throw new UnsupportedOperationException(message);
                }
                mySymtabVersion = version;
            }
            else
            {
                // this arg is not an option, we're done here
                return i;
            }
        }

        return args.length;
    }


    @Override
    protected void process(IonReader reader)
        throws IonException
    {
        while (reader.hasNext())
        {
            IonType type = reader.next();

//            System.err.println("Next: " + type);
//            System.err.println("isInStruct=" + reader.isInStruct());

//            if (reader.isInStruct())
            {
                String fieldName = reader.getFieldName();

                if (fieldName != null)
                {
//                    System.err.println("Adding field name: " + fieldName);
                    mySymbols.add(fieldName);
                }
            }

            internAnnotations(reader);

            switch (type) {
                case SYMBOL:
                {
                    String text = reader.stringValue();
                    if (text != null)
                    {
                        mySymbols.add(text);
                    }
                    break;
                }
                case LIST:
                case SEXP:
                case STRUCT:
                {
//                    System.err.println("stepping in");
                    reader.stepIn();
                    break;
                }
                default:
                {
                    // do nothing
                    break;
                }
            }

            while (! reader.hasNext() && reader.getDepth() > 0)
            {
//                System.err.println("stepping out");
                reader.stepOut();
            }
        }
    }

    private void internAnnotations(IonReader reader)
    {
        Iterator<String> i = reader.iterateTypeAnnotations();
        assert i != null;
        while (i.hasNext())
        {
            String ann = i.next();
            mySymbols.add(ann);
        }
    }
}
