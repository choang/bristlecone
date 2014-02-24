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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/**
 * Stores row data in a normalized form to permit ready comparison.
 */
public class Row
{
    private final List<String> names;
    private final String[]     values;

    /** Create a new row fetcher instance. */
    public Row(List<String> names)
    {
        this.names = names;
        this.values = new String[names.size()];
    }

    /**
     * Sets the value of a column in the row.
     * 
     * @param index Index of the column, starting at 0
     * @param value Value of the column
     */
    public void setValue(int index, Object value)
    {
        String normalValue;
        if (value == null)
        {
            normalValue = "NULL";
        }
        else if (value instanceof Float)
        {
            // Floats have precision issues going cross DBMS type.
            // Reduce decimal places.
            float dval = (Float) value;
            BigDecimal bd = new BigDecimal(dval);
            bd = bd.setScale(17, BigDecimal.ROUND_HALF_UP);
            normalValue = bd.toString();
        }
        else if (value instanceof Double)
        {
            // Doubles have precision issues. Reduce to 17 places.
            double dval = (Double) value;
            BigDecimal bd = new BigDecimal(dval);
            bd = bd.setScale(17, BigDecimal.ROUND_HALF_UP);
            normalValue = bd.toString();
        }
        else
        {
            normalValue = value.toString();
        }

        values[index] = normalValue;
    }

    /**
     * Returns the column names.
     */
    public List<String> getNames()
    {
        return names;
    }

    /**
     * Returns all values.
     */
    public List<String> getValues()
    {
        return Arrays.asList(values);
    }

    /** Returns a single value. */
    public String getValue(int index)
    {
        return values[index];
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer("[");
        for (int i = 0; i < names.size(); i++)
        {
            if (i > 0)
                sb.append("|");
            sb.append(names.get(i));
            sb.append("=");
            sb.append(values[i]);
        }
        sb.append("]");
        return sb.toString();
    }
}