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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import software.amazon.ion.IonCatalog;
import software.amazon.ion.IonException;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonSystem;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.system.IonSystemBuilder;
import software.amazon.ion.system.SimpleCatalog;

/**
 *   ion_encode  ion_print
 */
abstract class BaseApp
{
    protected SimpleCatalog myCatalog = new SimpleCatalog();
    protected IonSystem mySystem = IonSystemBuilder.standard()
                                                   .withCatalog(myCatalog)
                                                   .build();


    //=========================================================================
    // Static methods

    /**
     * @param in doesn't need to be buffered; this method will read in bulk.
     */
    protected static byte[] loadAsByteArray(InputStream in)
        throws IOException
    {
        byte[] buf = new byte[4096];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int cnt;

        while ((cnt = in.read(buf)) != -1) {
            bos.write(buf, 0, cnt);
        }
        return bos.toByteArray();
    }


    protected static byte[] loadAsByteArray(File file)
        throws FileNotFoundException, IOException
    {
        long len = file.length();
        if (len < 0 || len > Integer.MAX_VALUE)
        {
            throw new IllegalArgumentException("File too long: " + file);
        }

        byte[] buffer = new byte[(int) len];

        FileInputStream in = new FileInputStream(file);
        try
        {
            int readBytesCount = in.read(buffer);
            if (readBytesCount != len || in.read() != -1)
            {
                System.err.println("Read the wrong number of bytes from "
                                       + file);
                return null;
            }
        }
        finally
        {
            in.close();
        }

        return buffer;
    }


    //=========================================================================


    public void doMain(String[] args)
    {
        int firstFileIndex = processOptions(args);

        int fileCount = args.length - firstFileIndex;
        String[] files = new String[fileCount];
        System.arraycopy(args, firstFileIndex, files, 0, fileCount);

        if (optionsAreValid(files))
        {
            processFiles(files);
        }
    }


    protected int processOptions(String[] args)
    {
        return 0;
    }

    protected boolean optionsAreValid(String[] filePaths)
    {
        return true;
    }

    protected void processFiles(String[] filePaths)
    {
        if (filePaths.length == 0)
        {
            processStdIn();
        }
        else
        {
            for (int i = 0; i < filePaths.length; i++)
            {
                String filePath = filePaths[i];
                processFile(filePath);
            }
        }
    }

    protected boolean processFile(String path)
    {
        File file = new File(path);
        if (file.canRead() && file.isFile())
        {
            try
            {
                process(file);
                return true;
            }
            catch (IonException e)
            {
                System.err.println("An error occurred while processing "
                                   + path);
                System.err.println(e.getMessage());
            }
            catch (IOException e)
            {
                System.err.println("An error occurred while processing "
                                   + path);
                System.err.println(e.getMessage());
            }
        }
        else
        {
            System.err.println("Skipping unreadable file: " + path);
        }
        return false;
    }

    protected void processStdIn() {
    try
        {
        byte[] buffer = loadAsByteArray(System.in);
        IonReader reader = mySystem.newReader(buffer);
        process(reader);
            }
    catch (IonException e)
            {
                System.err.println("An error occurred while processing stdin");
                System.err.println(e.getMessage());
            }
    catch (IOException e)
            {
                System.err.println("An error occurred while processing stdin");
        System.err.println(e.getMessage());
            }
    }

    protected void process(File file)
        throws IOException, IonException
    {
        byte[] buffer = loadAsByteArray(file);

        IonReader reader = mySystem.newReader(buffer);

        process(file, reader);
    }

    protected void process(File inputFile, IonReader reader)
        throws IOException, IonException
    {
        process(reader);
    }

    protected void process(IonReader reader)
        throws IOException, IonException
    {
        // Do nothing...
    }


    protected void loadCatalog(String catalogPath)
    {
        System.err.println("Loading catalog from " + catalogPath);
        File catalogFile = new File(catalogPath);
        try
        {
            InputStream in =
                new BufferedInputStream(new FileInputStream(catalogFile));
            try
            {
                IonReader reader = mySystem.newReader(in);
                while (reader.next() != null)
                {
                    SymbolTable symtab =
                        mySystem.newSharedSymbolTable(reader, true);
                    myCatalog.putTable(symtab);
                }
            }
            finally
            {
                in.close();
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error loading catalog from "
                                         + catalogPath + ": " + e.getMessage(),
                                       e);
        }

        IonCatalog catalog = mySystem.getCatalog();
        assert myCatalog == catalog;
//        logDebug("----Catalog content:");
//        for (Iterator<StaticSymbolTable> i = catalog.iterator(); i.hasNext(); )
//        {
//            StaticSymbolTable table = i.next();
//            logDebug(table.getName() + "@" + table.getVersion());
//        }
//        logDebug("----");
    }


    protected SymbolTable getLatestSharedSymtab(String name)
    {
        IonCatalog catalog = mySystem.getCatalog();
        SymbolTable table = catalog.getTable(name);
        if (table == null)
        {
            String message =
                "There's no symbol table in the catalog named " + name;
            throw new RuntimeException(message);
        }
        logDebug("Found shared symbol table " + name
                   + "@" + table.getVersion());
        return table;
    }


    protected void logDebug(String message)
    {
        System.err.println(message);
    }
}
