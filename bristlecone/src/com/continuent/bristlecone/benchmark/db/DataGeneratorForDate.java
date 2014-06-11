/**
 * Bristlecone Test Tools for Databases
 * Copyright (C) 2006-2014 Continuent Inc.
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
 * Contributor(s): Hannu Alamäki, Linas Virbalas
 */

package com.continuent.bristlecone.benchmark.db;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Generates date values
 * 
 * @author alamäki
 */
public class DataGeneratorForDate implements DataGenerator
{
    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat(
        "yyyy-MM-dd");

    private long max;

    DataGeneratorForDate()
    {
        dateFormatter.setLenient(false);
      
        long maxVal = (8099 - 1970);
        maxVal *= 365L;
        maxVal *= 24L;
        maxVal *= 3600L;
        maxVal *= 1000L;
        this.max = maxVal;
    }

    /** Create a new instance with an upper bound. */
    DataGeneratorForDate(long maxValue)
    {
        dateFormatter.setLenient(false);
      
        this.max = maxValue;
    }

    /** Generate next date. */
    public Object generate()
    {
        long sign = (Math.random() >= 0.5) ? -1 : 1;
        long absvalue = (long) (Math.random() * max);

        long dateValue = sign * absvalue;
        
        Date date = new Date(dateValue);
     
        // REP-131 - catch invalid dates.
        // 1. Make a string out of this date - this doesn't fix invalid dates.
        String sDate = dateFormatter.format((Date) date);
        // 2. Now try to parse it - ParseException thrown if date is invalid.
        try
        {
            dateFormatter.parse(sDate);
        }
        catch (ParseException e)
        {
            // Re-generate date until it's valid. 
            date = (Date) generate(); 
        }
        
        return date;
    }
}