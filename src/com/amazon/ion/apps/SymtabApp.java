/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.apps;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.UnifiedSymbolTable;
import com.amazon.ion.util.Printer;
import java.io.IOException;
import java.util.Iterator;

/**
 *
 */
public class SymtabApp
    extends BaseApp
{
    private SymbolTable mySystemSymtab;
    private UnifiedSymbolTable mySymtab;


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
        mySystemSymtab = mySystem.getSystemSymbolTable();
        mySymtab = (UnifiedSymbolTable)
            mySystem.newLocalSymbolTable(mySystemSymtab);
    }


    //=========================================================================

    public void doMain(String[] args)
    {
        int firstFileIndex = processOptions(args);

        if (mySymtab.getName() == null)
        {
            throw new RuntimeException("Must provide --name");
        }
        // TODO verify that we don't import the same name.

        if (mySymtab.getVersion() == 0)
        {
            mySymtab.setVersion(1);
        }


        if (firstFileIndex == args.length)
        {
            System.err.println("Must provide list of files to provide symbols");
        }
        else
        {
            processFiles(args, firstFileIndex);
        }

        // TODO mySymtab should have new symbols relative to last version

        mySymtab.lock();

        IonStruct symtabIon = mySymtab.getIonRepresentation(mySystem);
        Printer p = new Printer();
        try
        {
            // TODO ensure IVM is printed
            p.print(symtabIon, System.out);
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
                importLatestVersion(mySymtab, name);
            }
            else if ("--name".equals(arg))
            {
                if (mySymtab.getName() != null)
                {
                    throw new RuntimeException("Multiple names");
                }
                String name = args[++i];
                mySymtab.setName(name);
            }
            else if ("--version".equals(arg))
            {
                if (mySymtab.getVersion() != 0)
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
                mySymtab.setVersion(version);
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
                    mySymtab.addSymbol(fieldName);
                }
            }

            internAnnotations(reader);

            switch (type) {
                case SYMBOL:
                {
                    String text = reader.stringValue();
                    if (text != null)
                    {
                        mySymtab.addSymbol(text);
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
            mySymtab.addSymbol(ann);
        }
    }
}
