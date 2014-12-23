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
 * Contributor(s):
 */

package com.continuent.bristlecone.dc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Fetches rows from a SQL table.
 */
public class SqlRowFetcher
{
    private static final Logger logger = Logger.getLogger(SqlRowFetcher.class);

    // Connection properties.
    private String              url;
    private String              user;
    private String              password;
    private String              schema;
    private String              table;
    private List<String>        keys;

    private Connection          connection;
    private Statement           statement;
    private ResultSet           resultSet;
    private List<String>        names;

    /** Create a new row fetcher instance. */
    public SqlRowFetcher()
    {
    }

    public String getUrl()
    {
        return url;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public String getUser()
    {
        return user;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
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

    public List<String> getKeys()
    {
        return keys;
    }

    public void setKeys(List<String> sortColumns)
    {
        this.keys = sortColumns;
    }

    /**
     * Perform basic initialization.
     */
    public void prepare() throws Exception
    {
        // Open connection to master.
        connection = DriverManager.getConnection(url, user, password);
        statement = connection.createStatement();

        // Design a query to fetch rows in orcer.
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT * from ");
        if (schema != null)
        {
            sb.append(schema).append(".");
        }
        sb.append(table);
        if (keys != null && keys.size() > 0)
        {
            sb.append(" ORDER BY ");
            for (int i = 0; i < keys.size(); i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(keys.get(i));
            }
        }
        String select = sb.toString();
        if (logger.isDebugEnabled())
            logger.debug("Issuing query: " + select);

        // Execute the query and fetch column names.
        resultSet = statement.executeQuery(select);
        ResultSetMetaData metadata = resultSet.getMetaData();
        int colCount = metadata.getColumnCount();
        names = new ArrayList<String>(colCount);
        for (int i = 0; i < colCount; i++)
        {
            names.add(i, metadata.getColumnName(i + 1));
        }
    }

    /**
     * Returns true if there are more rows.
     */
    public boolean next() throws Exception
    {
        return resultSet.next();
    }

    /**
     * Fetches the next row.
     */
    public Row fetch() throws Exception
    {
        Row nextRow = new Row(names);
        for (int i = 0; i < names.size(); i++)
        {
            nextRow.setValue(i, resultSet.getObject(i + 1));
        }
        return nextRow;
    }

    /**
     * Cleans up JDBC resources.
     */
    public void cleanup()
    {
        closeResultSet(resultSet);
        closeStatement(statement);
        closeConnection(connection);
    }

    // Private routine to close a JDBC result set.
    private void closeResultSet(ResultSet rs)
    {
        if (rs != null)
        {
            try
            {
                rs.close();
            }
            catch (SQLException e)
            {
            }
        }
    }

    // Private routine to close a JDBC statement.
    private void closeStatement(Statement s)
    {
        if (s != null)
        {
            try
            {
                s.close();
            }
            catch (SQLException e)
            {
            }
        }
    }

    // Private routine to close a JDBC connection.
    private void closeConnection(Connection c)
    {
        if (c != null)
        {
            try
            {
                c.close();
            }
            catch (SQLException e)
            {
            }
        }
    }
}