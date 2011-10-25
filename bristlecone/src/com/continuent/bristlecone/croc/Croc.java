/**
 * Bristlecone Test Tools for Databases
 * Copyright (C) 2011 Continuent Inc.
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

package com.continuent.bristlecone.croc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.bristlecone.benchmark.db.Table;
import com.continuent.bristlecone.benchmark.db.TableHelper;

public class Croc implements CrocContext
{
    private static Logger   logger           = Logger.getLogger(Croc.class);

    // Properties for croc runs.
    private String          masterUrl        = null;
    private String          slaveUrl         = null;
    private String          user             = "tungsten";
    private String          password         = "secret";
    private boolean         ddlReplication   = true;
    private boolean         stageTables      = true;
    private boolean         compare          = true;
    private int             timeout          = 60;
    private String          testList         = null;

    // Runtime parameters.
    private List<Loader>    tests            = new ArrayList<Loader>();
    private LivenessChecker checker;
    private TableComparator comparator;

    // Results.
    int                     tried            = 0;
    int                     failed           = 0;

    /** Create a new Croc instance. */
    public Croc()
    {
    }

    public synchronized String getMasterUrl()
    {
        return masterUrl;
    }

    public synchronized void setMasterUrl(String masterUrl)
    {
        this.masterUrl = masterUrl;
    }

    public synchronized String getSlaveUrl()
    {
        return slaveUrl;
    }

    public synchronized void setSlaveUrl(String slaveUrl)
    {
        this.slaveUrl = slaveUrl;
    }

    public synchronized String getUser()
    {
        return user;
    }

    public synchronized void setUser(String user)
    {
        this.user = user;
    }

    public synchronized String getPassword()
    {
        return password;
    }

    public synchronized void setPassword(String password)
    {
        this.password = password;
    }

    public synchronized boolean isDdlReplication()
    {
        return ddlReplication;
    }

    public synchronized void setDdlReplication(boolean ddlReplication)
    {
        this.ddlReplication = ddlReplication;
    }
    
    public synchronized void setStageTables(boolean stageTables)
    {
        this.stageTables = stageTables;
    }

    public synchronized boolean isCompare()
    {
        return compare;
    }

    public synchronized void setCompare(boolean compare)
    {
        this.compare = compare;
    }

    public synchronized int getTimeout()
    {
        return timeout;
    }

    public synchronized void setTimeout(int timeout)
    {
        this.timeout = timeout;
    }

    public synchronized String getTestList()
    {
        return testList;
    }

    public synchronized void setTestList(String testList)
    {
        this.testList = testList;
    }

    /**
     * Execute a croc test run.
     */
    public void run()
    {
        // Vet options.
        assertPropertyNotNull("masterUrl", masterUrl);
        assertPropertyNotNull("slaveUrl", slaveUrl);
        assertPropertyNotNull("user", user);
        assertPropertyNotNull("password", password);
        assertPropertyNotNull("testList", testList);

        // Load and instantiate tests.
        logger.info("Loading croc run list");
        File testListFile = new File(testList);
        FileReader fr = null;
        String className = null;
        try
        {
            fr = new FileReader(testListFile);
            BufferedReader reader = new BufferedReader(fr);
            String line;
            while ((line = reader.readLine()) != null)
            {
                line = line.trim();
                if (line.length() > 0 && !line.startsWith("#"))
                {
                    className = line;
                    Class<?> clazz = Class.forName(className);
                    Loader run = (Loader) clazz.newInstance();
                    this.tests.add(run);
                }
            }
        }
        catch (FileNotFoundException e)
        {
            throw new CrocException("Unable to read test list: "
                    + testListFile.getAbsolutePath(), e);
        }
        catch (IOException e)
        {
            throw new CrocException("Unable to read test list: "
                    + testListFile.getAbsolutePath(), e);
        }
        catch (ClassNotFoundException e)
        {
            throw new CrocException("Unable to instantiate test class: "
                    + className, e);
        }
        catch (IllegalAccessException e)
        {
            throw new CrocException("Unable to instantiate test class: "
                    + className, e);
        }
        catch (InstantiationException e)
        {
            throw new CrocException("Unable to instantiate test class: "
                    + className, e);
        }

        // Check database liveness.
        Connection m = getJdbcConnection(masterUrl);
        this.releaseJdbcConnection(m);
        Connection s = getJdbcConnection(slaveUrl);
        this.releaseJdbcConnection(s);

        // Create a replication liveness checker.
        this.checker = new LivenessChecker();
        checker.setMasterUrl(masterUrl);
        checker.setSlaveUrl(slaveUrl);
        checker.setUser(user);
        checker.setPassword(password);
        checker.setDdlReplication(ddlReplication);
        checker.setStageTables(stageTables);
        try
        {
            checker.prepare();
        }
        catch (Exception e)
        {
            throw new CrocException("Unable to set up liveness checker: "
                    + e.getMessage(), e);
        }

        // Create a table comparator.
        this.comparator = new TableComparator();
        comparator.setMasterUrl(masterUrl);
        comparator.setSlaveUrl(slaveUrl);
        comparator.setUser(user);
        comparator.setPassword(password);
        try
        {
            comparator.prepare();
        }
        catch (Exception e)
        {
            throw new CrocException("Unable to set up comparator: "
                    + e.getMessage(), e);
        }

        // Run each test in succession.
        for (Loader run : tests)
        {
            String name = run.getClass().getName();
            if (logger.isDebugEnabled())
                logger.debug("Starting test: " + name);
            tried++;
            try
            {
                long start = System.currentTimeMillis();
                boolean result = doRun(run);
                long end = System.currentTimeMillis();
                double duration = (end - start) / 1000.0;
                if (result)
                {
                    logger.info("RUN (" + duration + ") " + name + " OK");
                }
                else
                {
                    logger.info("RUN (" + duration + ") " + name + " FAIL");
                    failed++;
                }
            }
            catch (CrocException e)
            {
                logger.error("Case failed due to exception: " + name);
                throw e;
            }
        }

        // Print results.
        logger.info(String.format("TRIED: %d  FAILED: %d", tried, failed));

        // Release resources.
        checker.cleanup();
        comparator.cleanup();
    }

    // Execute a single croc run.
    private boolean doRun(Loader crocRun) throws CrocException
    {
        // Create master tables
        List<Table> tables = crocRun.getTables();
        for (Table table : tables)
        {
            // Do not create staging tables on master.
            createTable(masterUrl, table, false);
        }

        // Create slave tables if desired.
        if (!ddlReplication)
        {
            for (Table table : tables)
            {
                // If Replicator is using BatchLoader with stage method, create
                // the staging tables too.
                createTable(slaveUrl, table, stageTables);
            }
        }

        // Test liveness.
        if (checker.flush(timeout))
        {
            logger.debug("Replication is live after table creation");
        }
        else
        {
            throw new CrocException(
                    "Replication is not live after table creation");
        }

        // Call croc load() method.
        crocRun.load(this);

        // Test liveness again.
        if (checker.flush(timeout))
        {
            logger.debug("Replication is live after data load");
        }
        else
        {
            throw new CrocException(
                    "Replication is not live after table creation");
        }

        // Compare tables.
        boolean ok = true;
        if (compare)
        {
            for (Table table : tables)
            {
                if (comparator.compare(table))
                {
                    logger.debug("Table compares OK: " + table.getName());
                }
                else
                {
                    ok = false;
                    logger.error("Table does not compare OK: "
                            + table.getName());
                }
            }
        }
        else
        {
            logger.debug("Skipping compare");
        }

        // Write result.
        return ok;
    }

    // Ensure String property is not null.
    private void assertPropertyNotNull(String name, String value)
            throws CrocException
    {
        if (value == null)
        {
            throw new CrocException("Property may not be null: " + name);
        }
    }

    // Get a database connection.
    public Connection getJdbcConnection(String url)
    {
        try
        {
            return DriverManager.getConnection(url, user, password);
        }
        catch (SQLException e)
        {
            throw new CrocException("Unable to connect to database: " + url, e);
        }
    }

    // Get a database connection.
    public void releaseJdbcConnection(Connection conn)
    {
        try
        {
            conn.close();
        }
        catch (SQLException e)
        {
            logger.warn("Unable to close database connection");
        }
    }

    // Create a test table.
    public void createTable(String url, Table table, boolean stageTables)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Creating test table: " + table.getName());
            logger.debug("Table details: " + table);
        }
        TableHelper helper = new TableHelper(url, user, password);
        try
        {
            helper.create(table, true, stageTables);
        }
        catch (SQLException e)
        {
            throw new CrocException("Unable to create table: "
                    + table.getName(), e);
        }
    }
}