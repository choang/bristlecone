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

package com.continuent.bristlecone.benchmark.scenarios;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.continuent.bristlecone.benchmark.BenchmarkException;
import com.continuent.bristlecone.benchmark.db.Column;
import com.continuent.bristlecone.benchmark.db.SqlDialect;
import com.continuent.bristlecone.benchmark.db.Table;
import com.continuent.bristlecone.benchmark.db.TableSet;
import com.continuent.bristlecone.benchmark.db.TableSetHelper;

/**
 * Implements a scenario that repeatedly inserts into one or more tables.  
 * Inserts are non-conflicting (i.e., should never deadlock).  <p>
 * 
 * This scenario suppors a special parameter called replicaUrl that contains
 * the URL of a slave replica.  If set, the code used it to confirm that 
 * data have arrived in the slave database by looking up a key generated as 
 * part of the insert.<p>
 * 
 * This scenario is useful for testing raw insert speed, as rows are just
 * added to the end of the table.  It can be parameterized by the usual options
 * such as tables, datarows, etc. 
 * 
 * @author rhodges
 */
public class WriteSimpleScenario extends ScenarioBase
{
  private static final Logger logger = Logger.getLogger(WriteSimpleScenario.class);
  
  // Counter used to help with generating unique keys across multiple runs.  
  // This value must be incremented in a synchronized block to avoid generating
  // identical keys across multiple scenarios. 
  protected static long key = 0;
  protected long localKey; 
  
  // URL of replica for testing master/slave configurations. 
  protected String replicaUrl; 

  // Prepared insert statements. 
  protected PreparedStatement[] pstmtArray;

  // Variables used for managing master/slave replication.  
  protected Connection replicaConn;
  TableSetHelper replicaTableSetHelper; 
  protected PreparedStatement[] replicaSelectArray;

  /** 
   * Sets the replica URL used to check whether updates have arrived
   * on a replica.  If unset, the check is ignored. 
   */
  public void setReplicaUrl(String replicaUrl)
  {
    this.replicaUrl = replicaUrl;
  }

  /** Add additional initialization to take care of replica databases. */
  public void initialize(Properties properties) throws Exception
  {
    // Define table layout. 
    Column[] columns = new Column[3]; 
    columns[0] = new Column("mykey", Types.INTEGER, -1, -1, true, true); 
    columns[1] = new Column("mythread", Types.VARCHAR, 50); 
    columns[1].setIndexed(true);
    columns[2] = new Column("mypayload", Types.VARCHAR, (int) datawidth); 
    
    // Compute the local key value for use in this test case. 
    synchronized (WriteSimpleScenario.class)
    {
      localKey = key++;
    }
    
    // Set up helper classes. 
    tableSet = new TableSet("benchmark_scenario_", tables, 
        datarows, columns);
    helper = new TableSetHelper(url, user, password); 
    conn = helper.getConnection();

    if (replicaUrl != null)
    {
      logger.debug("Connecting to replica database: url=" + url + " user=" + user);
      replicaTableSetHelper = new TableSetHelper(url, user, password); 
      replicaConn = replicaTableSetHelper.getConnection();
    }
  }

  /** Create test tables. */
  public void globalPrepare() throws Exception
  {
    // Create and populate tables. 
    if (reusedata)
    {
      logger.info("Reusing existing test tables...");
    }
    else
    {
      logger.info("Creating and populating test tables...");
      helper.createAll(tableSet);
      helper.populateAll(tableSet);
    }
    
    // Run analyze command if supplied. 
    if (analyzeCmd != null)
    {
      logger.info("Running analyze command: " + analyzeCmd);
      helper.execute(analyzeCmd);
    }
  }

  /** Create a prepared statement array. */
  public void prepare() throws Exception
  {
    // Prepare insert statements for main database. 
    SqlDialect dialect = helper.getSqlDialect(); 
    Table tables[] = tableSet.getTables();
    pstmtArray = new PreparedStatement[tables.length];
    for (int i = 0; i < tables.length; i++)
    {
      String sql = dialect.getInsert(tables[i]);
      pstmtArray[i] = conn.prepareStatement(sql);
    }
    
    // Prepare select statements if we have a replica database. 
    if (replicaConn != null)
    {
      replicaSelectArray = new PreparedStatement[tables.length];
      for (int i = 0; i < tables.length; i++)
      {
        String sql = dialect.getSelectByColumn(tables[i], tables[i].getColumn("mythread"));
        replicaSelectArray[i] = replicaConn.prepareStatement(sql);
      }
    }
  }

  /** Execute an interation. */
  public void iterate(long iterationCount) throws Exception
  {
    // Pick a table at random on which to operate.
    int index = (int) (Math.random() * pstmtArray.length);
    PreparedStatement pstmt = pstmtArray[index];
    
    // Do the insert.  
    String value = localKey + "_" + Thread.currentThread().getName() + "_"
        + iterationCount;
    helper.generateParameters(tableSet, pstmt);
    pstmt.setString(1, value);
    pstmt.executeUpdate();
    
    // If we have a replica database, ensure the insert made it over. 
    // If there is a replica, connect to it and find our last record.
    if (replicaConn != null)
    {
      // Find the inserted row. 
      boolean found = replicaTableSetHelper
          .testRowExistence(this.replicaSelectArray[index], value, true, 120000, 5000); 
      if (! found)
        throw new BenchmarkException("Unable to find last row on replica: mythread=" + value); 
    }
  }

  /** Clean up resources used by scenario. */
  public void cleanup() throws Exception
  {
    // Clean up connections. 
    for (int i = 0; i < pstmtArray.length; i++)
      pstmtArray[i].close();
    if (conn != null)
      conn.close();
  }
}