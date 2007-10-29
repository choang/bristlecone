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

package com.continuent.bristlecone.evaluator;

public class Statistics
{
  private int queries;
  private int rowsRead;
  private int updates;
  private int deletes;
  private int inserts;
  private float interval;
  private long responseTime;
  private int threads;
  
  public Statistics()
  {
    
  }
  
  public Statistics(Statistics old)
  {
    this.queries = old.queries;
    this.rowsRead = old.rowsRead;
    this.updates = old.updates;
    this.deletes = old.deletes;
    this.inserts = old.inserts;
    this.responseTime = old.responseTime;
    this.threads = old.threads;
  }
  
  public int getDeletes()
  {
    return deletes;
  }

  public void addDelete()
  {
    this.deletes++;
  }
  public int getInserts()
  {
    return inserts;
  }
  public void addInsert()
  {
    this.inserts++;
  }
  public int getQueries()
  {
    return queries;
  }
  public void addQuery()
  {
    this.queries++;
  }
  public int getRowsRead()
  {
    return rowsRead;
  }
  public void addRowsRead(int rowsRead)
  {
    this.rowsRead += rowsRead;
  }
  public int getUpdates()
  {
    return updates;
  }
  public void addUpdate()
  {
    this.updates++;
  }

  public void add(Statistics stats)
  {
    this.deletes += stats.deletes;
    this.inserts += stats.inserts;
    this.queries += stats.queries;
    this.rowsRead += stats.rowsRead;
    this.updates += stats.updates;
    this.responseTime += stats.responseTime;
    this.threads += stats.threads;
  }

  public Statistics diff(Statistics prev)
  {
    Statistics result = new Statistics();
    result.deletes = this.deletes - prev.deletes;
    result.inserts = this.inserts - prev.inserts;
    result.queries = this.queries - prev.queries;
    result.rowsRead = this.rowsRead - prev.rowsRead;
    result.updates = this.updates - prev.updates;
    result.responseTime = this.responseTime - prev.responseTime;
    result.threads = this.threads;
    
    return result;
  }

  public void setInterval(float f)
  {
    interval = f;
  }

  public float getInterval()
  {
    return interval;
  }

  public void addResponseTime(long responseTime)
  {
    this.responseTime += responseTime;
  }

  public long getResponseTime()
  {
    return responseTime;
  }

  public void addThread()
  {
    threads++;
  }

  public int getThreads()
  {
    return threads;
  }

  public int getAverageResponseTime()
  {
    return queries == 0 ? 0 :(int)responseTime / queries;
  }
}
