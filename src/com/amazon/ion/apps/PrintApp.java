/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.apps;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonWriter;
import com.amazon.ion.impl.IonTextWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * TODO pretty-print on/off and configure
 * TODO show system view (eg symbol tables)
 */
public class PrintApp
    extends BaseApp
{
    private File myOutputDir;


    //=========================================================================
    // Static methods

    public static void main(String[] args)
    {
        PrintApp app = new PrintApp();
        app.doMain(args);
    }


    //=========================================================================

    public void doMain(String[] args)
    {
        int firstFileIndex = processOptions(args);

        if (firstFileIndex == args.length)
        {
            System.err.println("Must provide list of files to print");
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
        for (int i = 0; i < args.length; i++)
        {
            String arg = args[i];
            if ("--catalog".equals(arg))
            {
                String symtabPath = args[++i];
                loadCatalog(symtabPath);
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
                return i;
            }
        }

        return args.length;
    }


    @Override
    protected void process(File inputFile, IonReader reader)
        throws IOException, IonException
    {
        if (myOutputDir == null)
        {
            process(reader, System.out);
        }
        else
        {
            String fileName = inputFile.getName();
            File outputFile = new File(myOutputDir, fileName);
            FileOutputStream out = new FileOutputStream(outputFile);
            try
            {
                process(reader, out);
            }
            finally
            {
                out.close();
            }
        }
    }

    protected void process(IonReader reader, OutputStream out)
        throws IOException, IonException
    {
        IonWriter writer = new IonTextWriter(out, true);
        writer.writeIonEvents(reader);

        // Ensure there's a newline at the end and flush the buffer.
        out.write('\n');
        out.flush();
    }
}
