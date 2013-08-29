/**
 * Bristlecone Test Tools for Databases
 * Copyright (C) 2006-2013 Continuent Inc.
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
 * Initial developer(s): Linas Virbalas.
 * Contributor(s):
 */

package com.continuent.bristlecone.benchmark.test;

import java.math.BigDecimal;
import com.continuent.bristlecone.benchmark.db.Column;
import com.continuent.bristlecone.benchmark.db.DataGenerator;
import com.continuent.bristlecone.benchmark.db.DataGeneratorFactory;
import junit.framework.TestCase;

public class DataGeneratorTest extends TestCase
{
  private BigDecimal getBigDecimal(double doubleValue)
  {
    return new BigDecimal(Double.toString(doubleValue)).stripTrailingZeros();
  }

  /**
   * Test that double generator generates at least a few numbers with decimal
   * digits.
   */
  public void testDoubleDistribution() throws Exception
  {
    Column c = new Column("d", java.sql.Types.DOUBLE);
    DataGenerator dg = DataGeneratorFactory.getInstance().getGenerator(c);

    int decimals = 0;
    int runs = 100;
    for (int i = 0; i < runs; i++)
    {
      Double obj = (Double) dg.generate();
      double d = obj.doubleValue();
      BigDecimal bd = getBigDecimal(d);

      if (bd.scale() > 0)
      {
        System.out.println(d + " = " + bd);
        decimals++;
      }
    }

    System.out.println("Numbers with decimals: " + decimals + " out of " + runs
        + " runs");

    assertTrue("No double values with decimal numbers generated", decimals > 0);
  }
}
