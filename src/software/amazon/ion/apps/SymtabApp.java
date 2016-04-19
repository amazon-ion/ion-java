/*
 * Copyright 2008-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.apps;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import software.amazon.ion.IonCatalog;
import software.amazon.ion.IonException;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolTable;

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

    @Override
    protected int processOptions(String[] args)
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
    protected boolean optionsAreValid(String[] filePaths)
    {
        if (mySymtabName == null)
        {
            throw new RuntimeException("Must provide --name");
        }
        // TODO verify that we don't import the same name.

        if (mySymtabVersion == 0)
        {
            mySymtabVersion = 1;
        }

        if (filePaths.length == 0)
        {
            System.err.println("Must provide list of files to provide symbols");
            return false;
        }

        return true;
    }


    @Override
    public void processFiles(String[] filePaths)
    {
        super.processFiles(filePaths);

        SymbolTable[] importArray = new SymbolTable[myImports.size()];
        myImports.toArray(importArray);

        SymbolTable mySymtab =
            mySystem.newSharedSymbolTable(mySymtabName,
                                          mySymtabVersion,
                                          mySymbols.iterator(),
                                          importArray);

        IonWriter w = mySystem.newTextWriter((OutputStream)System.out);
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


    @Override
    protected void process(IonReader reader)
        throws IonException
    {
        IonType type;
        while ((type = reader.next()) != null)
        {
            String fieldName = reader.getFieldName();
            intern(fieldName);

            internAnnotations(reader);

            switch (type) {
                case SYMBOL:
                {
                    String text = reader.stringValue();
                    intern(text);
                    break;
                }
                case LIST:
                case SEXP:
                case STRUCT:
                {
                    reader.stepIn();
                    break;
                }
                default:
                {
                    // do nothing
                    break;
                }
            }

            while (reader.next() != null && reader.getDepth() > 0)
            {
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
            intern(ann);
        }
    }

    private void intern(String text)
    {
        if (text != null)
        {
            if (text.equals("$ion") || text.startsWith("$ion_")) return;
            mySymbols.add(text);
        }
    }
}
