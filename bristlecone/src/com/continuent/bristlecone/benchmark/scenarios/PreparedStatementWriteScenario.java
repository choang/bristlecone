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

import com.continuent.bristlecone.benchmark.Scenario;
import com.continuent.bristlecone.benchmark.db.Column;
import com.continuent.bristlecone.benchmark.db.SqlDialect;
import com.continuent.bristlecone.benchmark.db.Table;
import com.continuent.bristlecone.benchmark.db.TableSet;
import com.continuent.bristlecone.benchmark.db.TableSetHelper;

/**
 * Implements a scenario that repeatedly inserts into one or more tables using
 * prepared statements. This scenario helps test the following:
 * <p/>
 * <ul>
 * <li>Effect of reusing prepared statements vs. recreating for each write</li>
 * <li>Effect of batched vs. non-batched prepared statements</li>
 * </ul>
 * Inserts are non-conflicting (i.e., should never deadlock).
 * <p/>
 * This scenario can be parameterized by the usual options such as tables,
 * datarows, etc.
 * 
 * @author rhodges
 */
public class PreparedStatementWriteScenario implements Scenario
{
    private static final Logger logger        = Logger
                                                      .getLogger(PreparedStatementWriteScenario.class);

    // Scenario properties.
    /** Url of the database on which we are running the test. */
    protected String            url;

    /** Database user name. */
    protected String            user;

    /** Database password (leaving it null equates to empty password). */
    protected String            password      = "";

    /** Datatype of the payload column in the benchmark table. */
    protected String            datatype      = "varchar";

    /**
     * Column width of the payload column, e.g., 10 for varchar equates to
     * varchar(10).
     */
    protected int               datawidth     = 10;

    /** Number of inserts per batch. */
    protected int               writesPerXact = 1;

    /** If true, use JDBC batching within each transaction. */
    protected boolean           jdbcBatching  = false;

    // Implementation data for scenario
    protected TableSet          tableSet;
    protected TableSetHelper    helper;
    protected Connection        conn          = null;

    public void setPassword(String password)
    {
        this.password = password;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public void setDataWidth(int datawidth)
    {
        this.datawidth = datawidth;
    }

    public void setDatatype(String datatype)
    {
        this.datatype = datatype;
    }

    public void setWritesPerXact(int writesPerXact)
    {
        this.writesPerXact = writesPerXact;
    }

    public void setJdbcBatching(boolean jdbcBatching)
    {
        this.jdbcBatching = jdbcBatching;
    }

    public void initialize(Properties properties) throws Exception
    {
        // Define table layout.
        Column[] columns = new Column[3];
        columns[0] = new Column("mykey", Types.INTEGER, -1, -1, false, false);
        columns[1] = new Column("mythread", Types.VARCHAR, 50);
        columns[1].setIndexed(true);
        columns[2] = new Column("mypayload", Types.VARCHAR, (int) datawidth);

        // Set up helper classes.
        tableSet = new TableSet("benchmark_scenario_", 1, 0, columns);
        helper = new TableSetHelper(url, user, password);
        conn = helper.getConnection();
    }

    /** Create test tables. */
    public void globalPrepare() throws Exception
    {
        logger.info("Creating test table...");
        helper.createAll(tableSet);
    }

    /**
     * Empty.
     */
    public void prepare() throws Exception
    {
        // Does nothing.
    }

    /** Execute an interation. */
    public void iterate(long iterationCount) throws Exception
    {
        // Prepare insert statement.
        SqlDialect dialect = helper.getSqlDialect();
        Table tables[] = tableSet.getTables();
        String sql = dialect.getInsert(tables[0]);
        PreparedStatement pstmt = conn.prepareStatement(sql);

        // Begin transaction. 
        conn.setAutoCommit(false);
        
        // Loop through writes.
        for (int i = 0; i < this.writesPerXact; i++)
        {
            // Add data.
            String value = "pstmt_" + Thread.currentThread().getName() + "_"
                    + iterationCount;
            helper.generateParameters(tableSet, pstmt);
            pstmt.setString(2, value);

            // Either batch or execute immediately.
            if (this.jdbcBatching)
                pstmt.addBatch();
            else
                pstmt.executeUpdate();
        }

        // If we are batching, submit now.
        if (this.jdbcBatching)
            pstmt.executeBatch();
        
        // Commit transaction. 
        conn.commit();
        
        // Close the statement. 
        pstmt.close();
    }

    /** Clean up resources used by scenario. */
    public void cleanup() throws Exception
    {
        // Clean up connection.
        if (conn != null)
            conn.close();
    }

    public void globalCleanup() throws Exception
    {
        // Does nothing. 
    }
}