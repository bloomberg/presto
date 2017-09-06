/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.accumulo.iterators;

import com.facebook.presto.accumulo.AccumuloQueryRunner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCardinalityAssignmentIterator
{
    private static final String TABLE_NAME = "testcardinalityassignmentiterator";
    private Connector connector = null;

    @BeforeClass
    public void setupClass()
            throws Exception
    {
        connector = AccumuloQueryRunner.getAccumuloConnector();
    }

    @BeforeMethod
    public void setup()
            throws Exception
    {
        IteratorSetting wholeRowSetting = new IteratorSetting(21, WholeRowIterator.class);
        IteratorSetting setting = new IteratorSetting(22, CardinalityAssignmentIterator.class);
        CardinalityAssignmentIterator.setIndexColumnFamily(setting, "test_test");
        connector.tableOperations().create(TABLE_NAME);

        connector.tableOperations().attachIterator(TABLE_NAME, wholeRowSetting, EnumSet.of(IteratorScope.majc));
        connector.tableOperations().attachIterator(TABLE_NAME, setting, EnumSet.of(IteratorScope.majc));

        connector.securityOperations().changeUserAuthorizations("root", new Authorizations("private", "moreprivate"));
    }

    @AfterMethod
    public void cleanupClass()
            throws Exception
    {
        if (connector.tableOperations().exists(TABLE_NAME)) {
            connector.tableOperations().delete(TABLE_NAME);
        }
    }

    @Test
    public void testNoData()
            throws Exception
    {
        connector.tableOperations().compact(TABLE_NAME, null, null, true, true);

        Scanner scanner = connector.createScanner(TABLE_NAME, new Authorizations());
        assertFalse(scanner.iterator().hasNext());
        scanner.close();
    }

    @Test
    public void testNoMatchingData()
            throws Exception
    {
        Mutation m1 = new Mutation("value1");
        m1.put("no_match", "row1", "");

        Mutation m2 = new Mutation("value2");
        m2.put("no_match", "row2", "");

        BatchWriter writer = connector.createBatchWriter(TABLE_NAME, new BatchWriterConfig());
        writer.addMutation(m1);
        writer.addMutation(m2);
        writer.close();

        connector.tableOperations().compact(TABLE_NAME, null, null, true, true);

        Scanner scanner = connector.createScanner(TABLE_NAME, new Authorizations());
        Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        assertTrue(iterator.hasNext());
        assertKeyValuePair(iterator.next(), "value1", "no_match", "row1", "");
        assertKeyValuePair(iterator.next(), "value2", "no_match", "row2", "");
        assertFalse(iterator.hasNext());
        scanner.close();
    }

    @Test
    public void testCorrectValue()
            throws Exception
    {
        Mutation m1 = new Mutation("value1");
        m1.put("test_test", "row1", "");
        m1.put("test_test", "row2", "");
        m1.put("test_test", "row3", "");
        m1.put("test_test", "row4", "");
        m1.put("test_test_car", "car", "4");

        Mutation m2 = new Mutation("value2");
        m2.put("test_test", "row1", "");
        m2.put("test_test", "row2", "");
        m2.put("test_test", "row3", "");
        m2.put("test_test_car", "car", "3");

        BatchWriter writer = connector.createBatchWriter(TABLE_NAME, new BatchWriterConfig());
        writer.addMutation(m1);
        writer.addMutation(m2);
        writer.close();

        Scanner scanner = connector.createScanner(TABLE_NAME, new Authorizations());
        Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        assertTrue(iterator.hasNext());

        assertKeyValuePair(iterator.next(), "value1", "test_test", "row1", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row2", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row3", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row4", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test_car", "car", "4");
        assertKeyValuePair(iterator.next(), "value2", "test_test", "row1", "");
        assertKeyValuePair(iterator.next(), "value2", "test_test", "row2", "");
        assertKeyValuePair(iterator.next(), "value2", "test_test", "row3", "");
        assertKeyValuePair(iterator.next(), "value2", "test_test_car", "car", "3");

        assertFalse(iterator.hasNext());
        scanner.close();
    }

    @Test
    public void testWrongValueNoCompaction()
            throws Exception
    {
        Mutation m1 = new Mutation("value1");
        m1.put("test_test", "row1", "");
        m1.put("test_test", "row2", "");
        m1.put("test_test", "row3", "");
        m1.put("test_test", "row4", "");
        m1.put("test_test_car", "car", "2");

        Mutation m2 = new Mutation("value2");
        m2.put("test_test", "row1", "");
        m2.put("test_test", "row2", "");
        m2.put("test_test", "row3", "");
        m2.put("test_test_car", "car", "1");

        BatchWriter writer = connector.createBatchWriter(TABLE_NAME, new BatchWriterConfig());
        writer.addMutation(m1);
        writer.addMutation(m2);
        writer.close();

        Scanner scanner = connector.createScanner(TABLE_NAME, new Authorizations());
        Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        assertTrue(iterator.hasNext());

        assertKeyValuePair(iterator.next(), "value1", "test_test", "row1", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row2", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row3", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row4", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test_car", "car", "2");
        assertKeyValuePair(iterator.next(), "value2", "test_test", "row1", "");
        assertKeyValuePair(iterator.next(), "value2", "test_test", "row2", "");
        assertKeyValuePair(iterator.next(), "value2", "test_test", "row3", "");
        assertKeyValuePair(iterator.next(), "value2", "test_test_car", "car", "1");

        assertFalse(iterator.hasNext());
        scanner.close();
    }

    @Test
    public void testWrongValueWithCompaction()
            throws Exception
    {
        Mutation m1 = new Mutation("value1");
        m1.put("test_test", "row1", "");
        m1.put("test_test", "row2", "");
        m1.put("test_test", "row3", "");
        m1.put("test_test", "row4", "");
        m1.put("test_test_car", "car", "2");

        Mutation m2 = new Mutation("value2");
        m2.put("test_test", "row1", "");
        m2.put("test_test", "row2", "");
        m2.put("test_test", "row3", "");
        m2.put("test_test_car", "car", "1");

        BatchWriter writer = connector.createBatchWriter(TABLE_NAME, new BatchWriterConfig());
        writer.addMutation(m1);
        writer.addMutation(m2);
        writer.close();

        connector.tableOperations().compact(TABLE_NAME, null, null, true, true);

        Scanner scanner = connector.createScanner(TABLE_NAME, new Authorizations());
        Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        assertTrue(iterator.hasNext());
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row1", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row2", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row3", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row4", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test_car", "car", "4");
        assertKeyValuePair(iterator.next(), "value2", "test_test", "row1", "");
        assertKeyValuePair(iterator.next(), "value2", "test_test", "row2", "");
        assertKeyValuePair(iterator.next(), "value2", "test_test", "row3", "");
        assertKeyValuePair(iterator.next(), "value2", "test_test_car", "car", "3");

        assertFalse(iterator.hasNext());
        scanner.close();
    }

    @Test
    public void testMissingCardinalityColumn()
            throws Exception
    {
        Mutation m1 = new Mutation("value1");
        m1.put("test_test", "row1", "");
        m1.put("test_test", "row2", "");
        m1.put("test_test", "row3", "");
        m1.put("test_test", "row4", "");

        BatchWriter writer = connector.createBatchWriter(TABLE_NAME, new BatchWriterConfig());
        writer.addMutation(m1);
        writer.close();

        connector.tableOperations().compact(TABLE_NAME, null, null, true, true);

        Scanner scanner = connector.createScanner(TABLE_NAME, new Authorizations());
        Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        assertTrue(iterator.hasNext());
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row1", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row2", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row3", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row4", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test_car", "car", "4");

        assertFalse(iterator.hasNext());
        scanner.close();
    }

    @Test
    public void testRowWithVisibility()
            throws Exception
    {
        Mutation m1 = new Mutation("value1");
        m1.put("test_test", "row1", "");
        m1.put("test_test", "row2", "");
        m1.put("test_test", "row3", "");
        m1.put("test_test", "row4", "");
        m1.put("test_test", "row5", new ColumnVisibility("private"), "");
        m1.put("test_test", "row6", new ColumnVisibility("moreprivate"), "");
        m1.put("test_test", "row7", new ColumnVisibility("private"), "");
        m1.put("test_test", "row8", new ColumnVisibility("moreprivate"), "");
        m1.put("test_test_car", "car", "4");
        m1.put("test_test_car", "car", new ColumnVisibility("private"), "2");
        m1.put("test_test_car", "car", new ColumnVisibility("moreprivate"), "2");

        BatchWriter writer = connector.createBatchWriter(TABLE_NAME, new BatchWriterConfig());
        writer.addMutation(m1);
        writer.close();

        connector.tableOperations().compact(TABLE_NAME, null, null, true, true);

        Scanner scanner = connector.createScanner(TABLE_NAME, new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        assertTrue(iterator.hasNext());
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row1", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row2", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row3", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row4", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row5", "private", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row6", "moreprivate", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row7", "private", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row8", "moreprivate", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test_car", "car", "4");
        assertKeyValuePair(iterator.next(), "value1", "test_test_car", "car", "moreprivate", "2");
        assertKeyValuePair(iterator.next(), "value1", "test_test_car", "car", "private", "2");

        assertFalse(iterator.hasNext());
        scanner.close();
    }

    @Test
    public void testRowsWithTimestamps()
            throws Exception
    {
        Mutation m1 = new Mutation(decodeBytes("00\\x08\\x80\\x00\\x00\\xA1\\xC7\\x06\\x18\\x00"));
        m1.put("test_test_tsd_car", "car", "5");

        Mutation m2 = new Mutation(decodeBytes("00\\x08\\x80\\x00\\x00\\xA1\\xCD?\\x1C\\x80"));
        m2.put(b("test_test"), decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x00\\x10\\xC4"), b(""));
        m2.put(b("test_test"), decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x00\\x15!"), b(""));
        m2.put(b("test_test"), decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x00\\x1B\\x82"), b(""));
        m2.put(b("test_test"), decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x009\\x86"), b(""));
        m2.put(b("test_test"), decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x00[&"), b(""));
        m2.put(b("test_test"), decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x00h\\xC2"), b(""));
        m2.put(b("test_test"), decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x00\\xA7\\x86"), b(""));
        m2.put(b("test_test"), decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x00\\xD4#"), b(""));
        m2.put(b("test_test"), decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x00\\xE8@"), b(""));
        m2.put("test_test_car", "car", "4");
        m2.put("test_test_tsh_car", "car", "4");
        m2.put("test_test_tsm_car", "car", "4");
        m2.put("test_test_tss_car", "car", "4");

        BatchWriter writer = connector.createBatchWriter(TABLE_NAME, new BatchWriterConfig());
        writer.addMutation(m1);
        writer.addMutation(m2);
        writer.close();

        connector.tableOperations().compact(TABLE_NAME, null, null, true, true);

        Scanner scanner = connector.createScanner(TABLE_NAME, new Authorizations());
        Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        assertTrue(iterator.hasNext());
        assertKeyValuePair(iterator.next(), decodeBytes("00\\x08\\x80\\x00\\x00\\xA1\\xC7\\x06\\x18\\x00"), "test_test_tsd_car", "car", "5");
        assertKeyValuePair(iterator.next(), decodeBytes("00\\x08\\x80\\x00\\x00\\xA1\\xCD?\\x1C\\x80"), "test_test", decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x00\\x10\\xC4"), "");
        assertKeyValuePair(iterator.next(), decodeBytes("00\\x08\\x80\\x00\\x00\\xA1\\xCD?\\x1C\\x80"), "test_test", decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x00\\x15!"), "");
        assertKeyValuePair(iterator.next(), decodeBytes("00\\x08\\x80\\x00\\x00\\xA1\\xCD?\\x1C\\x80"), "test_test", decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x00\\x1B\\x82"), "");
        assertKeyValuePair(iterator.next(), decodeBytes("00\\x08\\x80\\x00\\x00\\xA1\\xCD?\\x1C\\x80"), "test_test", decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x009\\x86"), "");
        assertKeyValuePair(iterator.next(), decodeBytes("00\\x08\\x80\\x00\\x00\\xA1\\xCD?\\x1C\\x80"), "test_test", decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x00[&"), "");
        assertKeyValuePair(iterator.next(), decodeBytes("00\\x08\\x80\\x00\\x00\\xA1\\xCD?\\x1C\\x80"), "test_test", decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x00h\\xC2"), "");
        assertKeyValuePair(iterator.next(), decodeBytes("00\\x08\\x80\\x00\\x00\\xA1\\xCD?\\x1C\\x80"), "test_test", decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x00\\xA7\\x86"), "");
        assertKeyValuePair(iterator.next(), decodeBytes("00\\x08\\x80\\x00\\x00\\xA1\\xCD?\\x1C\\x80"), "test_test", decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x00\\xD4#"), "");
        assertKeyValuePair(iterator.next(), decodeBytes("00\\x08\\x80\\x00\\x00\\xA1\\xCD?\\x1C\\x80"), "test_test", decodeBytes("\\x08\\x80\\x00\\x00\\x00\\x00\\x00\\xE8@"), "");
        assertKeyValuePair(iterator.next(), decodeBytes("00\\x08\\x80\\x00\\x00\\xA1\\xCD?\\x1C\\x80"), "test_test_car", "car", "9");
        assertKeyValuePair(iterator.next(), decodeBytes("00\\x08\\x80\\x00\\x00\\xA1\\xCD?\\x1C\\x80"), "test_test_tsh_car", "car", "4");
        assertKeyValuePair(iterator.next(), decodeBytes("00\\x08\\x80\\x00\\x00\\xA1\\xCD?\\x1C\\x80"), "test_test_tsm_car", "car", "4");
        assertKeyValuePair(iterator.next(), decodeBytes("00\\x08\\x80\\x00\\x00\\xA1\\xCD?\\x1C\\x80"), "test_test_tss_car", "car", "4");

        assertFalse(iterator.hasNext());
        scanner.close();
    }

    @Test
    public void testIncorrectValues()
            throws Exception
    {
        Mutation m1 = new Mutation("value1");
        m1.put("test_test", "row1", "");
        m1.put("test_test", "row2", "");
        m1.put("test_test", "row3", "");
        m1.put("test_test", "row4", "");
        m1.put("test_test_car", "car", "2");

        Mutation m2 = new Mutation("value2");
        m2.put("test_test", "row1", "");
        m2.put("test_test", "row2", "");
        m2.put("test_test", "row3", "");
        m2.put("test_test_car", "car", "1");

        BatchWriter writer = connector.createBatchWriter(TABLE_NAME, new BatchWriterConfig());
        writer.addMutation(m1);
        writer.addMutation(m2);
        writer.close();

        connector.tableOperations().compact(TABLE_NAME, null, null, true, true);

        Scanner scanner = connector.createScanner(TABLE_NAME, new Authorizations());
        Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        assertTrue(iterator.hasNext());
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row1", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row2", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row3", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test", "row4", "");
        assertKeyValuePair(iterator.next(), "value1", "test_test_car", "car", "4");
        assertKeyValuePair(iterator.next(), "value2", "test_test", "row1", "");
        assertKeyValuePair(iterator.next(), "value2", "test_test", "row2", "");
        assertKeyValuePair(iterator.next(), "value2", "test_test", "row3", "");
        assertKeyValuePair(iterator.next(), "value2", "test_test_car", "car", "3");

        assertFalse(iterator.hasNext());
        scanner.close();
    }

    private static byte[] decodeBytes(String encoded)
            throws DecoderException
    {
        List<Byte> bytes = new ArrayList<>();

        String tmp = encoded;
        while (tmp.length() > 0) {
            if (tmp.length() >= 2 && tmp.substring(0, 2).equals("\\\\")) {
                bytes.add((byte) '\\');
                tmp = tmp.substring(2);
            }
            else if (tmp.length() >= 2 && tmp.substring(0, 2).equals("\\x")) {
                bytes.add(Hex.decodeHex(tmp.substring(2, 4).toCharArray())[0]);
                tmp = tmp.substring(4);
            }
            else {
                bytes.add((byte) tmp.charAt(0));
                tmp = tmp.substring(1);
            }
        }
        byte[] retval = new byte[bytes.size()];

        int i = 0;
        for (byte b : bytes) {
            retval[i++] = b;
        }
        return retval;
    }

    private static byte[] b(String s)
    {
        return s.getBytes(UTF_8);
    }

    private static void assertKeyValuePair(Entry<Key, Value> e, String row, String cf, String cq, String value)
    {
        assertEquals(e.getKey().getRow().toString(), row);
        assertEquals(e.getKey().getColumnFamily().toString(), cf);
        assertEquals(e.getKey().getColumnQualifier().toString(), cq);
        assertTrue(e.getKey().getTimestamp() > 0, "Timestamp is zero");
        assertEquals(e.getValue().toString(), value);
    }

    private static void assertKeyValuePair(Entry<Key, Value> e, String row, String cf, String cq, String cv, String value)
    {
        assertEquals(e.getKey().getRow().toString(), row);
        assertEquals(e.getKey().getColumnFamily().toString(), cf);
        assertEquals(e.getKey().getColumnQualifier().toString(), cq);
        assertEquals(e.getKey().getColumnVisibility().toString(), cv);
        assertTrue(e.getKey().getTimestamp() > 0, "Timestamp is zero");
        assertEquals(e.getValue().toString(), value);
    }

    private static void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, byte[] cq, String value)
    {
        assertEquals(e.getKey().getRow().copyBytes(), row);
        assertEquals(e.getKey().getColumnFamily().toString(), cf);
        assertEquals(e.getKey().getColumnQualifier().copyBytes(), cq);
        assertTrue(e.getKey().getTimestamp() > 0, "Timestamp is zero");
        assertEquals(e.getValue().toString(), value);
    }

    private static void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, String cq, String value)
    {
        assertEquals(e.getKey().getRow().copyBytes(), row);
        assertEquals(e.getKey().getColumnFamily().toString(), cf);
        assertEquals(e.getKey().getColumnQualifier().toString(), cq);
        assertTrue(e.getKey().getTimestamp() > 0, "Timestamp is zero");
        assertEquals(e.getValue().toString(), value);
    }
}
