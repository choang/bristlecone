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

import java.sql.PreparedStatement;

import com.continuent.bristlecone.benchmark.db.SqlDialect;
import com.continuent.bristlecone.benchmark.db.Table;

/**
 * Implements a scenario that selects rows from a table with varying column 
 * width and number of rows. 
 * 
 * @author rhodges
 */
public class QueryScenario extends ScenarioBase
{
  /** Create a prepared statement array. */
  public void prepare() throws Exception
  {
    SqlDialect dialect = helper.getSqlDialect(); 
    Table tables[] = tableSet.getTables();
    pstmtArray = new PreparedStatement[tables.length];
    for (int i = 0; i < tables.length; i++)
    {
      String sql = dialect.getSelectAll(tables[i]);
      pstmtArray[i] = conn.prepareStatement(sql);
    }
  }

  /** Execute an interation. */
  public void iterate(long iterationCount) throws Exception
  {
    // Pick a table at random on which to operate.
    int index = (int) (Math.random() * pstmtArray.length);
    PreparedStatement pstmt = pstmtArray[index];
    
    // Do the query.
    pstmt.executeQuery();
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