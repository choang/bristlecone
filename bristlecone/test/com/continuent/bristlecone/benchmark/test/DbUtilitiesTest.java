/**
 * Bristlecone Test Tools for Databases
 * Copyright (C) 2006-2007 Continuent Inc.
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
 * Initial developer(s): Robert Hodges and Ralph Hannus.
 * Contributor(s):
 */

package com.continuent.bristlecone.benchmark.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import junit.framework.TestCase;

import com.continuent.bristlecone.benchmark.db.Column;
import com.continuent.bristlecone.benchmark.db.SqlDialect;
import com.continuent.bristlecone.benchmark.db.SqlDialectFactory;
import com.continuent.bristlecone.benchmark.db.Table;
import com.continuent.bristlecone.benchmark.db.TableSet;
import com.continuent.bristlecone.benchmark.db.TableSetHelper;

/**
 * Implements a unit test designed to check database utility functions, 
 * specifically, creating, loading with data, and deleting tables using
 * TableSet definitions.  
 * 
 * @author rhodges
 *
 */
public class DbUtilitiesTest extends TestCase
{

  // Does nothing for now
  protected void setUp() throws Exception
  {
    super.setUp();
  }

  protected void tearDown() throws Exception
  {
    super.tearDown();
  }

  /** 
   * Instantiate a column and confirm that values are correctly returned. 
   */
  public void testColumn()
  {
    Column c = new Column("name", Types.BIGINT, 2, 1, true, false);
    assertEquals("name", c.getName());
    assertEquals(Types.BIGINT, c.getType());
    assertEquals(2, c.getLength());
    assertEquals(1, c.getPrecision());
    assertEquals(true, c.isPrimaryKey());
    assertEquals(false, c.isAutoIncrement());
  }

  /**
   * Generate a table and check that values are correctly returned. 
   */
  public void testTable()
  {
    Table t = allTypesTable("testTable1");
    assertNotNull(t);
    assertEquals("Checking table name", "testTable1", t.getName());
    
    Column key = t.getPrimaryKey(); 
    assertNotNull("Checking that key is not null", key);
    assertEquals("Checking key name", "t_integer", key.getName());
    
    Column[] cols = t.getColumns();
    assertNotNull("Checking columns", cols);
    assertEquals("Checking column number", 8, cols.length);
  }
  
  /**
   * Generate a table with all types and confirm that all non-prepared 
   * statements work properly. 
   */
  public void testSqlDialect1() throws Exception
  {
    // Get data for test. 
    String url = "jdbc:hsqldb:file:build/testdb/testdb;shutdown=true";
    Table t = allTypesTable("testSqlDialect");
    SqlDialect dialect = SqlDialectFactory.getInstance().getDialect(url);
    Connection conn = getHsqlConnection(url);
    
    assertNotNull("Checking table", t);
    assertNotNull("Checking dialect", dialect); 
    assertNotNull("Checking connection", conn);

    // Drop table in case it already exists 
    Statement stmt = null;
    try
    {
      stmt = conn.createStatement();
      stmt.executeUpdate(dialect.getDropTable(t));
    }
    catch (SQLException e)
    {
    }
    finally
    {
      if (stmt != null)
      {
        stmt.close();
        stmt = null;
      }
    }
    
    // Test table creation.   
    stmt = conn.createStatement();
    String createTable = dialect.getCreateTable(t);
    stmt.execute(createTable);
    
    // Select from the table. 
    String selectAll = dialect.getSelectAll(t);
    stmt.execute(selectAll);
    
    // Delete everything from the table. 
    String deleteAll = dialect.getDeleteAll(t);
    stmt.execute(deleteAll);
    
    // Drop the table. 
    String deleteTable = dialect.getDropTable(t);
    stmt.execute(deleteTable);
    
    // Clean up. 
    stmt.close();
    conn.close();
  }
    
  /**
   * Generate a table with all types and confirm that all non-prepared 
   * statements work properly. 
   */
  public void testSqlDialect2() throws Exception
  {
    // Get data for test. 
    String url = "jdbc:hsqldb:file:build/testdb/testdb;shutdown=true";
    Table t = allTypesTable("testSqlDialect");
    SqlDialect dialect = SqlDialectFactory.getInstance().getDialect(url);
    Connection conn = getHsqlConnection(url);
    
    assertNotNull("Checking table", t);
    assertNotNull("Checking dialect", dialect); 
    assertNotNull("Checking connection", conn);

    // Drop table in case it already exists 
    Statement stmt = null;
    try
    {
      stmt = conn.createStatement();
      stmt.executeUpdate(dialect.getDropTable(t));
    }
    catch (SQLException e)
    {
    }
    finally
    {
      if (stmt != null)
      {
        stmt.close();
        stmt = null;
      }
    }
    
    // Test table creation.   
    stmt = conn.createStatement();
    String createTable = dialect.getCreateTable(t);
    stmt.execute(createTable);
    
    // Select from the table. 
    String selectAll = dialect.getSelectAll(t);
    stmt.execute(selectAll);
    
    // Delete everything from the table. 
    String deleteAll = dialect.getDeleteAll(t);
    stmt.execute(deleteAll);
    
    // Drop the table. 
    String deleteTable = dialect.getDropTable(t);
    stmt.execute(deleteTable);
    
    // Clean up. 
    stmt.close();
    conn.close();
  }

  /**
   * Shows that we can create and populate tables for a TableSet.  
   */
  public void testDataGeneration1() throws Exception
  {
    // Get data for test. 
    String url = "jdbc:hsqldb:file:build/testdb/testdb;shutdown=true";
    TableSet ts = allTypesTableSet("testDG1_", 10, 100);
    TableSetHelper tsHelper = new TableSetHelper(url, "sa", "");
    
    assertNotNull("Checking table", ts);

    // Create new tables. 
    tsHelper.createAll(ts);
    
    // Populate data. 
    tsHelper.populateAll(ts);
    
    // Drop tables and go home. 
    tsHelper.dropAll(ts, false);
  }
    
  // Create column defintions for all supported types. 
  private Column[] allTypes()
  {
    Column[] cols = new Column[] {
        new Column("t_integer", Types.INTEGER, 0, 0, true, false), 
        new Column("t_blob", Types.BLOB, 100), 
        new Column("t_char", Types.CHAR, 10), 
        new Column("t_clob", Types.CLOB, 100), 
        new Column("t_double", Types.DOUBLE), 
        new Column("t_float", Types.FLOAT),
        new Column("t_smallint", Types.SMALLINT),
        new Column("t_varchar", Types.VARCHAR, 10)
    };
    return cols;
  }

  // Create table definition containing all supported types.  
  private Table allTypesTable(String name)
  {
    Column[] cols = allTypes();
    Table t = new Table(name, cols);
    return t;
  }
  
  // Create table set definition containing all supported types.
  private TableSet allTypesTableSet(String prefix, int count, int rows)
  {
    Column[] cols = allTypes();
    return new TableSet(prefix, count, rows, cols);
  }
  
  // Returns an HSQLDB connection instance. 
  private Connection getHsqlConnection(String url) throws Exception
  {
    Connection c;
    Class.forName("org.hsqldb.jdbcDriver");
    c = DriverManager.getConnection(url, "sa", null);
    return c;
  }
}

