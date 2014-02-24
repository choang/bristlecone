/**
 * Bristlecone Test Tools for Databases
 * Copyright (C) 2014 Continuent Inc.
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

import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.bristlecone.benchmark.db.SqlDialect;
import com.continuent.bristlecone.benchmark.db.SqlDialectFactory;

public class DC
{
    private static Logger logger         = Logger.getLogger(DC.class);

    // Properties for data comparison runs.
    private String        masterUrl      = null;
    private String        masterUser     = "tungsten";
    private String        masterPassword = "secret";
    private String        slaveUrl       = null;
    private String        slaveUser      = "tungsten";
    private String        slavePassword  = "secret";
    private String        schema         = null;
    private String        table          = null;
    private List<String>  keys           = null;
    private String        driver         = null;
    private boolean       verbose        = false;

    /** Create a new data comparator instance. */
    public DC()
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

    public synchronized void setUser(String user)
    {
        this.masterUser = user;
        this.slaveUser = user;
    }

    public synchronized void setPassword(String password)
    {
        this.masterPassword = password;
        this.slavePassword = password;
    }

    public synchronized String getMasterUser()
    {
        return masterUser;
    }

    public synchronized void setMasterUser(String user)
    {
        this.masterUser = user;
    }

    public synchronized String getMasterPassword()
    {
        return masterPassword;
    }

    public synchronized void setMasterPassword(String password)
    {
        this.masterPassword = password;
    }

    public synchronized String getSlaveUser()
    {
        return slaveUser;
    }

    public synchronized void setSlaveUser(String slaveUser)
    {
        this.slaveUser = slaveUser;
    }

    public synchronized String getSlavePassword()
    {
        return slavePassword;
    }

    public synchronized void setSlavePassword(String slavePassword)
    {
        this.slavePassword = slavePassword;
    }

    public String getSchema()
    {
        return schema;
    }

    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    public String getTable()
    {
        return table;
    }

    public void setTable(String table)
    {
        this.table = table;
    }

    public void setKeys(List<String> keys)
    {
        this.keys = keys;
    }

    public String getDriver()
    {
        return driver;
    }

    public void setDriver(String driver)
    {
        this.driver = driver;
    }

    public synchronized boolean isVerbose()
    {
        return verbose;
    }

    public synchronized void setVerbose(boolean verbose)
    {
        this.verbose = verbose;
    }

    /**
     * Compare tables;
     */
    public boolean compare() throws DCRuntimeException
    {
        // Vet options.
        assertPropertyNotNull("masterUrl", masterUrl);
        assertPropertyNotNull("masterUser", masterUser);
        assertPropertyNotNull("masterPassword", masterPassword);
        assertPropertyNotNull("slaveUrl", slaveUrl);
        assertPropertyNotNull("slaveUser", slaveUser);
        assertPropertyNotNull("slavePassword", slavePassword);
        assertPropertyNotNull("table", table);

        // Load JDBC driver(s).
        loadJdbcDriver(masterUrl);
        loadJdbcDriver(slaveUrl);

        SqlRowFetcher f1 = null;
        SqlRowFetcher f2 = null;
        try
        {
            // Create row fetchers.
            f1 = new SqlRowFetcher();
            f1.setUrl(masterUrl);
            f1.setUser(masterUser);
            f1.setPassword(masterPassword);
            f1.setSchema(schema);
            f1.setTable(table);
            f1.setKeys(keys);
            f1.prepare();

            f2 = new SqlRowFetcher();
            f2.setUrl(slaveUrl);
            f2.setUser(slaveUser);
            f2.setPassword(slavePassword);
            f2.setSchema(schema);
            f2.setTable(table);
            f2.setKeys(keys);
            f2.prepare();

            // Compare tables.
            RowComparator comparator = new RowComparator();
            if (comparator.diff(f1, f2))
            {
                logger.info("Tables compare OK");
                return true;
            }
            else
            {
                logger.error("Tables do not compare OK");
                return false;
            }
        }
        catch (Exception e)
        {
            throw new DCRuntimeException("Table compare failed: message="
                    + e.getMessage(), e);
        }
        finally
        {
            if (f1 != null)
                f1.cleanup();
            if (f2 != null)
                f2.cleanup();
        }
    }

    // Ensure String property is not null.
    private void assertPropertyNotNull(String name, String value) throws Error
    {
        if (value == null)
        {
            throw new DCRuntimeException("Property may not be null: " + name);
        }
    }

    // Load driver corresponding to a particular URL type.
    public void loadJdbcDriver(String url)
    {
        // If we don't have a driver, get the proper dialect, which should know
        // the driver.
        if (driver == null)
        {
            SqlDialectFactory dialectFactory = SqlDialectFactory.getInstance();
            SqlDialect dialect = dialectFactory.getDialect(url);
            if (dialect == null)
            {
                logger.warn("Unable to find driver for url: " + url);
                return;
            }

            // Find the driver name from the dialect class.
            driver = dialect.getDriver();
            if (driver == null)
            {
                logger.warn("Sql dialect for URL does not specify a driver: "
                        + url);
                return;
            }
        }

        // Now load the driver.
        try
        {
            Class.forName(driver);
        }
        catch (ClassNotFoundException e)
        {
            throw new DCRuntimeException("Unable to load driver: " + driver, e);
        }
    }
}