/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2010 Continuent Inc.
 * Contact: tungsten@continuent.org
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
 * Initial developer(s): Scott Martin
 * Contributor(s): 
 */

package com.continuent.bristlecone.benchmark.tpcb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log.Logger;

import com.continuent.bristlecone.benchmark.db.SqlDialect;
import com.continuent.bristlecone.benchmark.db.SqlDialectFactory;

/**
 * Manages connection to the database.  This class currently does little more than the
 * underlying Java class - Connection.
 */
public class DatabaseConnection {
	private Connection connection;
	private String dbUri;
    private Logger logger = null;
    private SqlDialect dialect;
	
	enum ConnectionType {MYSQL, ORACLE};
	
    DatabaseConnection(Logger logger, String url)
    {
        this.logger = logger;
        
        dbUri = url;

        dialect = SqlDialectFactory.getInstance().getDialect(dbUri);
    }
    
	public void connect()
	{
		initDbConnection();
	}
	
	public Connection getConnection()
	{
		return connection;
	}
	
	public SqlDialect getDialect()
	{
		return dialect;
	}
	
	public void close()
	{
	    try
	    {
	        connection.close();
	    }
	    catch (Exception e)
	    {
	    }
	}
	
    private void initDbConnection()
    {
        try
        {
            logger.info("Connecting to database via:" + dbUri, null);
            Class.forName(dialect.getDriver()).newInstance();
            connection = DriverManager.getConnection(dbUri);
            connection.setAutoCommit(false);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
	
	public Statement createStatement() throws SQLException
	{
		return connection.createStatement();
	}

	public PreparedStatement prepareStatement(String SQL) throws SQLException
	{
		return connection.prepareStatement(SQL);
	}

	public void commit() throws SQLException
	{
		connection.commit();
	}
	
	public void execute(String SQL) throws SQLException
	{
		PreparedStatement statement = prepareStatement(SQL);
		statement.execute();
	}
}
