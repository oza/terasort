/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.library.common.comparator;

import java.nio.charset.Charset;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestTezTextComparator {
  private static final Logger LOG = LoggerFactory.getLogger(TestTezTextComparator.class);

  final static String[] keys = {
      "",
      "A", "B",
      "AA", "BB", "BA", "CB",
  };


  final static byte[][] keys2 = {
      {(byte)0x8d, (byte)0xae, (byte)0xc4, (byte)0xaa, (byte)0x0b, (byte)0x7c, (byte)0x33, (byte)0x3c, (byte)0x10, (byte)0x83},
      {(byte)0x8d, (byte)0xae, (byte)0x96, (byte)0x72, (byte)0x06, (byte)0xc9, (byte)0xbf, (byte)0x18, (byte)0xe2, (byte)0xcd}
  };

  private static final void set(Text bw, String s) {
    byte[] b = s.getBytes(Charset.forName("utf-8"));
    bw.set(b, 0, b.length);
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void cleanup() throws Exception {
  }

  @Test(timeout = 5000)
  public void testProxyComparator() {
    final ProxyComparator<Text> comparator = new TezTextComparator();
    Text lhs = new Text();
    Text rhs = new Text();
    for (String l : keys) {
      for (String r : keys) {
        set(lhs, l);
        set(rhs, r);
        final int lproxy = comparator.getProxy(lhs);
        final int rproxy = comparator.getProxy(rhs);
        if (lproxy < rproxy) {
          assertTrue(String.format("(%s) %d < (%s) %d", l, lproxy, r, rproxy),
              comparator.compare(lhs, rhs) < 0);
        }
        if (lproxy > rproxy) {
          assertTrue(String.format("(%s) %d > (%s) %d", l, lproxy, r, rproxy),
              comparator.compare(lhs, rhs) > 0);
        }
      }
    }
  }

  @Test(timeout = 5000)
  public void testProxyComparator2() {
    final ProxyComparator<Text> comparator = new TezTextComparator();
    Text lhs = new Text();
    Text rhs = new Text();
    for (byte[] l : keys2) {
      for (byte[] r : keys2) {
        lhs.set(l);
        rhs.set(r);
        final int lproxy = comparator.getProxy(lhs);
        final int rproxy = comparator.getProxy(rhs);
        if (lproxy < rproxy) {
          assertTrue(String.format("(%s) %d < (%s) %d", l, lproxy, r, rproxy),
              comparator.compare(lhs, rhs) < 0);
        }
        if (lproxy > rproxy) {
          assertTrue(String.format("(%s) %d > (%s) %d", l, lproxy, r, rproxy),
              comparator.compare(lhs, rhs) > 0);
        }
      }
    }
  }
}

