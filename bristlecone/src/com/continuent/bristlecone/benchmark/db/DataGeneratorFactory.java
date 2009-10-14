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

package com.continuent.bristlecone.benchmark.db;


/**
 * Returns a data generator appropriate for a particular data type. 
 * 
 * @author rhodges
 *
 */
public class DataGeneratorFactory
{
  private static DataGeneratorFactory instance = new DataGeneratorFactory();

  // Not used outside this class. 
  private DataGeneratorFactory()
  {
  }

  /** 
   * Returns factory instance. 
   */
  public static DataGeneratorFactory getInstance()
  {
    return instance;
  }
  
  /**
   * Return a data type generator for a particular column type.  
   */
  public DataGenerator getGenerator(Column c)
  {
    switch (c.getType())
    {
      case java.sql.Types.BLOB: 
        return new DataGeneratorForBlob(c.getLength(), 10);
      case java.sql.Types.CHAR:
        return new DataGeneratorForString(c.getLength(), 10);
      case java.sql.Types.CLOB:
        return new DataGeneratorForString(c.getLength(), 10);
      case java.sql.Types.DOUBLE:
        return new DataGeneratorForDouble();
      case java.sql.Types.FLOAT:
        return new DataGeneratorForFloat();
      case java.sql.Types.INTEGER:
          return new DataGeneratorForLong(Integer.MAX_VALUE);
      case java.sql.Types.DECIMAL:
          return new DataGeneratorForDecimal(c.getLength(), c.getPrecision());
      case java.sql.Types.SMALLINT:
        return new DataGeneratorForLong(32767); 
      case java.sql.Types.VARCHAR:
        return new DataGeneratorForString(c.getLength(), 10);
      case java.sql.Types.BOOLEAN:
        return new DataGeneratorForBoolean();
      case java.sql.Types.DATE:
        return new DataGeneratorForDate();
      case java.sql.Types.TIME:
        return new DataGeneratorForTime();
      case java.sql.Types.TIMESTAMP:
        return new DataGeneratorForTimestamp();
      
      case java.sql.Types.BIT:
      default:
        throw new IllegalArgumentException("Unsupported JDBC type value: " + c.getType());
    }
  }
}
