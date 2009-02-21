// Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.apps;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.system.SimpleCatalog;
import com.amazon.ion.system.SystemFactory;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 *   ion_encode  ion_print
 */
abstract class BaseApp
{
    protected IonCatalog myCatalog = new SimpleCatalog();
    protected IonSystem  mySystem  = SystemFactory.newSystem(myCatalog);


    //=========================================================================
    // Static methods

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

    // TODO get rid of this
    public void doMain(String action, String[] args)
    {
        if (args.length == 0)
        {
            processStdIn();
        }
        else
        {
            processFiles(args);
        }
    }


    protected void processFiles(String[] filePaths)
    {
        processFiles(filePaths, 0);
    }

    protected void processFiles(String[] filePaths, int startingIndex)
    {
        if (startingIndex == 0)
        {
            processStdIn();
        }
        else
        {
            for (int i = startingIndex; i < filePaths.length; i++)
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
        IonLoader loader = mySystem.newLoader();
        File catalogFile = new File(catalogPath);
        try
        {
            // The loader will automatically add all shared symtabs into the
            // catalog
            IonDatagram dg = loader.load(catalogFile);

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
