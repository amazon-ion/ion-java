/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ion.util.JarInfo;
import java.io.IOException;

/**
 * NOT FOR APPLICATION USE!
 */
public final class _Private_CommandLine
{
    static final int argid_VERSION = 2;
    static final int argid_HELP    = 3;
    static final int argid_INVALID = -1;

    static JarInfo info = null;
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

        info = new JarInfo();

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
            if (arg.equals("--help")) {
                return argid_HELP;
            }
            if (arg.equals("--version")) {
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
        System.out.println("ion-java -- Copyright (c) 2007-" + info.getBuildTime().getYear() + " Amazon.com");
        System.out.println("usage: java -jar <jar> <options>");
        System.out.println("options:");
        System.out.println("version\t\tprints current version entry");
        System.out.println("help\t\tprints this helpful message");
        if (errorMessage != null) {
            System.out.println();
            System.out.println(errorMessage);
        }
    }


    private static void doPrintVersion() throws IOException
    {
        IonTextWriterBuilder b = IonTextWriterBuilder.pretty();
        b.setCharset(IonTextWriterBuilder.ASCII);

        IonWriter w = b.build((Appendable)System.out);
        w.stepIn(IonType.STRUCT);
        {
            w.setFieldName("version");
            w.writeString(info.getProjectVersion());

            w.setFieldName("build_time");
            w.writeTimestamp(info.getBuildTime());
        }
        w.stepOut();
        w.finish();

        System.out.println();
    }
}
