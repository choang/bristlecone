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
 * Definition of a SQL column. 
 * 
 * @author rhodges
 */
public class Column
{
  private String name;
  private int type;
  private int length;
  private int precision;
  private boolean isPrimaryKey;
  private boolean isAutoIncrement;
  
  /** Short form to generate definition. */
  public Column(String name, int type)
  {
    this.name = name;
    this.type = type;
  }
  
  /** Longer form for character types. */
  public Column(String name, int type, int length)
  {
    this.name = name;
    this.type = type;
    this.length = length;
  }
  
  /** Full form for definitions. */
  public Column(String name, int type, int length, int precision,
      boolean isPrimaryKey, boolean isAutoIncrement)
  {
    this.name = name;
    this.type = type;
    this.length = length;
    this.precision = precision;
    this.isPrimaryKey = isPrimaryKey;
    this.isAutoIncrement = isAutoIncrement;
  }

  /** Returns true if this is an autoincrement column. */
  public boolean isAutoIncrement()
  {
    return isAutoIncrement;
  }

  /** Returns true if this column is the primary key. */
  public boolean isPrimaryKey()
  {
    return isPrimaryKey;
  }

  /** Returns the length of this column or -1 if not used. */
  public int getLength()
  {
    return length;
  }

  /** Returns the name of this column. */
  public String getName()
  {
    return name;
  }

  /** Returns the precision of this column or -1 if not used. */
  public int getPrecision()
  {
    return precision;
  }

  /** Returns the column type, which must be a value from java.sql.Type. */
  public int getType()
  {
    return type;
  } 
}