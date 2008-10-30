/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.apps;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonWriter;
import com.amazon.ion.impl.IonBinaryWriter;
import com.amazon.ion.impl.UnifiedSymbolTable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 *
 */
public class EncodeApp
    extends BaseApp
{
    private UnifiedSymbolTable[] myImports;
    private File myOutputDir;


    //=========================================================================
    // Static methods

    public static void main(String[] args)
    {
        EncodeApp app = new EncodeApp();
        app.doMain(args);
    }


    //=========================================================================
    // Construction and Configuration

    public EncodeApp()
    {
    }


    //=========================================================================


    public void doMain(String[] args)
    {
        int firstFileIndex = processOptions(args);

        if (firstFileIndex == args.length)
        {
            System.err.println("Must provide list of files to encode");
        }
        else
        {
            processFiles(args, firstFileIndex);
        }
    }


    /**
     *
     * @param args
     * @return the next index to process
     */
    private int processOptions(String[] args)
    {
        ArrayList<UnifiedSymbolTable> imports =
            new ArrayList<UnifiedSymbolTable>();

        int i;
        for (i = 0; i < args.length; i++)
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
                UnifiedSymbolTable symtab = getLatestSharedSymtab(name);
                imports.add(symtab);
            }
            else if ("--output-dir".equals(arg))
            {
                String path = args[++i];
                myOutputDir = new File(path);
                if (! myOutputDir.isDirectory() || ! myOutputDir.canWrite())
                {
                    throw new RuntimeException("Not a writeable directory: "
                                               + path);
                }
            }
            else
            {
                // this arg is not an option, we're done here
                break;
            }
        }

        myImports = imports.toArray(new UnifiedSymbolTable[0]);

        return i;
    }


    @Override
    protected void process(File inputFile, IonReader reader)
        throws IOException, IonException
    {
        IonWriter writer = new IonBinaryWriter(myImports);
        writer.writeIonEvents(reader);

        byte[] binaryBytes = writer.getBytes();

        if (myOutputDir != null)
        {
            String fileName = inputFile.getName();
            File outputFile = new File(myOutputDir, fileName);
            FileOutputStream out = new FileOutputStream(outputFile);
            try
            {
                out.write(binaryBytes);
            }
            finally
            {
                out.close();
            }
        }
        else
        {
            System.out.write(binaryBytes);
        }
    }
}
