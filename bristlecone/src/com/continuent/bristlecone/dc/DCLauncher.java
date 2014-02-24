/**
 * Bristlecone Test Tools for Databases
 * Copyright (C) 2011-2014 Continuent Inc.
 * Contact: bristlecone@lists.forge.continuent.org
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 *
 * Initial developer(s): Robert Hodges
 * Contributor(s): Linas Virbalas
 */

package com.continuent.bristlecone.dc;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Launcher for data comparison runs.
 */
public class DCLauncher
{
    private static Logger logger = Logger.getLogger(DCLauncher.class);

    /** Creates a new Benchmark instance. */
    public DCLauncher()
    {
    }

    /** Main method to permit external invocation. */
    public static void main(String argv[]) throws Exception
    {
        String masterUrl = null;
        String masterUser = null;
        String masterPassword = null;
        String slaveUrl = null;
        String slaveUser = null;
        String slavePassword = null;
        String user = null;
        String password = null;
        String schema = null;
        String table = null;
        List<String> keys = null;
        String driver = null;
        boolean verbose = false;

        // Parse arguments.
        int argc = 0;
        while (argc < argv.length)
        {
            String nextArg = argv[argc];
            argc++;

            if ("-url1".equals(nextArg))
            {
                masterUrl = argv[argc++];
            }
            else if ("-user1".equals(nextArg))
            {
                masterUser = argv[argc++];
            }
            else if ("-password1".equals(nextArg))
            {
                masterPassword = argv[argc++];
            }
            else if ("-url2".equals(nextArg))
            {
                slaveUrl = argv[argc++];
            }
            else if ("-user2".equals(nextArg))
            {
                slaveUser = argv[argc++];
            }
            else if ("-password2".equals(nextArg))
            {
                slavePassword = argv[argc++];
            }
            else if ("-user".equals(nextArg))
            {
                user = argv[argc++];
            }
            else if ("-password".equals(nextArg))
            {
                password = argv[argc++];
            }
            else if ("-schema".equals(nextArg))
            {
                schema = argv[argc++];
            }
            else if ("-table".equals(nextArg))
            {
                table = argv[argc++];
            }
            else if ("-keys".equals(nextArg))
            {
                String keyList = argv[argc++];
                keys = new LinkedList<String>();
                for (String key : keyList.split(","))
                {
                    String cleanKey = key.trim();
                    if (cleanKey.length() > 0)
                        keys.add(cleanKey);
                }
            }
            else if ("-driver".equals(nextArg))
            {
                driver = argv[argc++];
            }
            else if ("-verbose".equals(nextArg))
            {
                verbose = true;
            }
            else if ("-help".equals(nextArg))
            {
                usage();
                return;
            }
            else
            {
                String msg = "Unrecognized flag (try -help for usage): "
                        + nextArg;
                println(msg);
                exitWithFailure();
            }
        }

        // Ensure we have table names.
        if (table == null)
        {
            String msg = "You must specify a table using the -table option";
            println(msg);
            exitWithFailure();
        }

        // Run the test.
        try
        {
            logger.info("DC - Data comparison utility");
            logger.info("Initiating...");
            DC dc = new DC();
            dc.setMasterUrl(masterUrl);
            if (masterUser != null)
                dc.setMasterUser(masterUser);
            if (masterPassword != null)
                dc.setMasterPassword(masterPassword);
            dc.setSlaveUrl(slaveUrl);
            if (slaveUser != null)
                dc.setSlaveUser(slaveUser);
            if (slavePassword != null)
                dc.setSlavePassword(slavePassword);
            if (user != null)
                dc.setUser(user);
            if (password != null)
                dc.setPassword(password);
            if (schema != null)
                dc.setSchema(schema);
            if (table != null)
                dc.setTable(table);
            if (keys != null && keys.size() > 0)
                dc.setKeys(keys);
            if (driver != null)
                dc.setDriver(driver);
            dc.setVerbose(verbose);

            if (!dc.compare())
                exitWithFailure();
        }
        catch (DCRuntimeException e)
        {
            if (verbose)
                logger.fatal("ERROR: " + e.getMessage(), e);
            else
                logger.fatal("ERROR: " + e.getMessage());
            exitWithFailure();
        }
        catch (Throwable t)
        {
            logger.fatal("Execution failed due to unexpected exception", t);

            // Catch and print the error that caused benchmark failure.
            println("Execution failed...See log for detailed stack trace(s)");
            println("EXCEPTION: " + t.getMessage());

            // Print out sub-exceptions as well.
            Throwable cause = t;
            while ((cause = cause.getCause()) != null)
            {
                println("SUB-EXCEPTION: " + cause.getMessage());
            }
            exitWithFailure();
        }

        // If we get here we succeeded.
        exitWithSuccess1();
    }

    /** Print to standard out. */
    protected static void println(String message)
    {
        System.out.println(message);
    }

    /** Print usage. */
    protected static void usage()
    {
        println("Data comparison utility (\"dc\")");
        println("Usage: dc -url1 u1 -url2 u2 -table tab [options]");
        println("Standard Options:");
        println("  -url1 url      Master db url");
        println("  -url2 url      Slave db url");
        println("  -user user     Db user (defaults to tungsten)");
        println("  -password pw   Db password (defaults to secret)");
        println("  -schema s      Schema name (defaults to url schema)");
        println("  -table t       Table name");
        println("  -keys k1,k2,.. Comma-separated list of keys (defaults to no keys)");
        println("Extended Options:");
        println("  -user1 user    Url1 db user");
        println("  -password1 pw  Url1 db password");
        println("  -user2 user    Url2 db user");
        println("  -password2 pw  Url2 db password");
        println("  -driver        JDBC driver name");
        println("  -verbose       Print verbose error output");
        println("  -help          Print usage and exit");
    }

    // Fail gloriously.
    protected static void exitWithFailure()
    {
        System.exit(1);
    }

    // Exit with a success code.
    protected static void exitWithSuccess1()
    {
        System.exit(0);
    }
}