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
 * Contributor(s):
 */

package com.continuent.bristlecone.dc;

import org.apache.log4j.Logger;

/**
 * Implements a comparison function that can compare sets of tables.
 */
public class RowComparator
{
    private static final Logger logger         = Logger.getLogger(RowComparator.class);

    // Operational parameters.
    private boolean             ignoreNames    = false;
    private boolean             ignoreNameCase = false;

    /** Create a new table comparator. */
    public RowComparator()
    {
    }

    public boolean isIgnoreNames()
    {
        return ignoreNames;
    }

    public void setIgnoreNames(boolean ignoreNames)
    {
        this.ignoreNames = ignoreNames;
    }

    public boolean isIgnoreNameCase()
    {
        return ignoreNameCase;
    }

    public void setIgnoreNameCase(boolean ignoreNameCase)
    {
        this.ignoreNameCase = ignoreNameCase;
    }

    /**
     * Compare rows using a match-merge approach.
     * 
     * @param t1 First table row fetcher
     * @param t2 Second table row fetcher
     * @return If true tables are identical, otherwise not identical
     */
    public boolean diff(SqlRowFetcher t1, SqlRowFetcher t2) throws Exception
    {
        int row = 0;
        logger.debug("Comparing tables:  t1=" + t1.getTable() + " t2="
                + t2.getTable());

        // Loop through master results.
        while (t1.next())
        {
            row++;
            Row t1Row = t1.fetch();
            if (t2.next())
            {
                Row t2Row = t2.fetch();
                if (compare(t1Row, t2Row))
                {
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("ROW: " + row);
                        logger.debug("T1: " + t1Row.toString());
                        logger.debug("T2 : " + t2Row.toString());
                    }
                }
                else
                {
                    logger.error("T1 and T2 differ: t1=" + t1.getTable()
                            + " row=" + row);
                    logger.info("T1: " + t1Row.toString());
                    logger.info("T2: " + t2Row.toString());
                    return false;
                }
            }
            else
            {
                logger.error("T1 is larger than T2: table=" + t1.getTable()
                        + " row=" + row);
                logger.info("T1: " + t1.toString());
                return false;
            }
        }

        // If there are any remaining T2 rows, it has more rows than
        // T1.
        if (t2.next())
        {
            row++;
            logger.error("T2 is larger than master: table=" + t2.getTable()
                    + " row=" + row);
            logger.info("T2: " + t2.toString());
            return false;
        }

        // If there are no rows at all that is bad.
        if (row == 0)
        {
            logger.error("Table is empty on T1 and T2: table=" + t1.getTable());
            return false;
        }

        // If we get this far, the tables are the same.
        logger.debug("Rows compared: " + row);
        return true;
    }

    /** Execute comparison between rows. */
    private boolean compare(Row r1, Row r2)
    {
        // Simple string comparison for now...
        return (r1.toString().equals(r2.toString()));
    }
}