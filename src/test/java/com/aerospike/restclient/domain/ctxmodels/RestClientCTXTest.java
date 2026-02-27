/*
 * Copyright 2022 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.restclient.domain.ctxmodels;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.restclient.ASTestMapper;
import com.aerospike.restclient.ASTestUtils;
import com.aerospike.restclient.IASTestMapper;
import com.aerospike.restclient.config.JSONMessageConverter;
import com.aerospike.restclient.config.MsgPackConverter;
import com.aerospike.restclient.util.AerospikeAPIConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;


import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class RestClientCTXTest {

    private static final String ns = "test";
    private static final String set = "ctx";

    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JsonCTXMapper()),
                Arguments.of(new MsgPackCTXMapper())
        );
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testEmptyMapDoesNotMapToRestClientCTX(IASTestMapper mapper) {
        Map<String, Object> ctxMap = new HashMap<>();

        try {
            mapper.bytesToObject(mapper.objectToBytes(ctxMap));
            Assertions.fail("Should have not mapped to RestClientCTX");
        } catch (Exception e) {
            // Success
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapsToRestClientCTXListIndex(IASTestMapper mapper) {
        Map<String, Object> ctxMap = new HashMap<>();
        ctxMap.put("type", AerospikeAPIConstants.CTX.LIST_INDEX);
        ctxMap.put("index", 1);

        try {
            ListIndexCTX restCTX = (ListIndexCTX) mapper.bytesToObject(mapper.objectToBytes(ctxMap));
            Assertions.assertEquals(restCTX.type, AerospikeAPIConstants.CTX.LIST_INDEX);
            Assertions.assertEquals(restCTX.index.intValue(), 1);
        } catch (Exception e) {
            Assertions.fail(String.format("Should have mapped to RestClientCTXListIndex %s", e));
        }
    }

    @Test
    public void testRestClientCTXListIndexToASCTX() {
        com.aerospike.client.cdt.CTX expected = com.aerospike.client.cdt.CTX.listIndex(5);

        ListIndexCTX listIndex = new ListIndexCTX();
        listIndex.index = 5;

        ASTestUtils.compareCTX(expected, listIndex.toCTX());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapsToRestClientCTXListIndexCreate(IASTestMapper mapper) {
        Map<String, Object> ctxMap = new HashMap<>();
        ctxMap.put("type", AerospikeAPIConstants.CTX.LIST_INDEX_CREATE);
        ctxMap.put("index", 1);
        ctxMap.put("order", ListOrder.ORDERED);
        ctxMap.put("pad", true);

        try {
            ListIndexCreateCTX restCTX = (ListIndexCreateCTX) mapper.bytesToObject(mapper.objectToBytes(ctxMap));
            Assertions.assertEquals(restCTX.type, AerospikeAPIConstants.CTX.LIST_INDEX_CREATE);
            Assertions.assertEquals(restCTX.index.intValue(), 1);
            Assertions.assertEquals(restCTX.order, ListOrder.ORDERED);
            Assertions.assertTrue(restCTX.pad);
        } catch (Exception e) {
            Assertions.fail(String.format("Should have mapped to RestClientCTXListIndexCreate %s", e));
        }
    }

    @Test
    public void testRestClientCTXListIndexCreateToASCTX() {
        com.aerospike.client.cdt.CTX expected = com.aerospike.client.cdt.CTX.listIndexCreate(5, ListOrder.UNORDERED,
                false);

        ListIndexCreateCTX restCTX = new ListIndexCreateCTX();
        restCTX.index = 5;
        restCTX.order = ListOrder.UNORDERED;
        restCTX.pad = false;

        ASTestUtils.compareCTX(expected, restCTX.toCTX());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapsToRestClientCTXListRank(IASTestMapper mapper) {
        Map<String, Object> ctxMap = new HashMap<>();
        ctxMap.put("type", AerospikeAPIConstants.CTX.LIST_RANK);
        ctxMap.put("rank", 1);

        try {
            ListRankCTX restCTX = (ListRankCTX) mapper.bytesToObject(mapper.objectToBytes(ctxMap));
            Assertions.assertEquals(restCTX.type, AerospikeAPIConstants.CTX.LIST_RANK);
            Assertions.assertEquals(restCTX.rank.intValue(), 1);
        } catch (Exception e) {
            Assertions.fail(String.format("Should have mapped to RestClientCTXListIndexCreate %s", e));
        }
    }

    @Test
    public void testRestClientCTXListRankToASCTX() {
        com.aerospike.client.cdt.CTX expected = com.aerospike.client.cdt.CTX.listRank(5);

        ListRankCTX restCTX = new ListRankCTX();
        restCTX.rank = 5;

        ASTestUtils.compareCTX(expected, restCTX.toCTX());
    }

    // Checking all value types for ListValue. Will assume it works for the rest of the ctx types

    @ParameterizedTest
    @MethodSource("getParams")
    public void tesMapToRestClientCTXListValueWithStrVal(IASTestMapper mapper) {
        Map<String, Object> ctxMap = new HashMap<>();
        ctxMap.put("type", AerospikeAPIConstants.CTX.LIST_VALUE);
        ctxMap.put("value", "abc");

        try {
            ListValueCTX restCTX = (ListValueCTX) mapper.bytesToObject(mapper.objectToBytes(ctxMap));
            Assertions.assertEquals(restCTX.type, AerospikeAPIConstants.CTX.LIST_VALUE);
            Assertions.assertEquals(restCTX.value, "abc");
        } catch (Exception e) {
            Assertions.fail(String.format("Should have mapped to RestClientCTXListIndexCreate %s", e));
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void tesMapToRestClientCTXListValueWithIntVal(IASTestMapper mapper) {
        Map<String, Object> ctxMap = new HashMap<>();
        ctxMap.put("type", AerospikeAPIConstants.CTX.LIST_VALUE);
        ctxMap.put("value", 9);

        try {
            ListValueCTX restCTX = (ListValueCTX) mapper.bytesToObject(mapper.objectToBytes(ctxMap));
            Assertions.assertEquals(restCTX.type, AerospikeAPIConstants.CTX.LIST_VALUE);
            Assertions.assertEquals(restCTX.value, 9);
        } catch (Exception e) {
            Assertions.fail(String.format("Should have mapped to RestClientCTXListIndexCreate %s", e));
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void tesMapToRestClientCTXListValueWithFloat(IASTestMapper mapper) {
        Map<String, Object> ctxMap = new HashMap<>();
        ctxMap.put("type", AerospikeAPIConstants.CTX.LIST_VALUE);
        ctxMap.put("value", 3.14159);

        try {
            ListValueCTX restCTX = (ListValueCTX) mapper.bytesToObject(mapper.objectToBytes(ctxMap));
            Assertions.assertEquals(restCTX.type, AerospikeAPIConstants.CTX.LIST_VALUE);
            Assertions.assertEquals(restCTX.value, 3.14159);
        } catch (Exception e) {
            Assertions.fail(String.format("Should have mapped to RestClientCTXListIndexCreate %s", e));
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void tesMapToRestClientCTXListValueWithBool(IASTestMapper mapper) {
        Map<String, Object> ctxMap = new HashMap<>();
        ctxMap.put("type", AerospikeAPIConstants.CTX.LIST_VALUE);
        ctxMap.put("value", true);

        try {
            ListValueCTX restCTX = (ListValueCTX) mapper.bytesToObject(mapper.objectToBytes(ctxMap));
            Assertions.assertEquals(restCTX.type, AerospikeAPIConstants.CTX.LIST_VALUE);
            Assertions.assertEquals(restCTX.value, true);
        } catch (Exception e) {
            Assertions.fail(String.format("Should have mapped to RestClientCTXListIndexCreate %s", e));
        }
    }

    @Test
    public void testRestClientCTXListValueToASCTX() {
        com.aerospike.client.cdt.CTX expected = com.aerospike.client.cdt.CTX.listValue(Value.get(3.14159));

        ListValueCTX restCTX = new ListValueCTX();
        restCTX.value = 3.14159;

        ASTestUtils.compareCTX(expected, restCTX.toCTX());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void tesMapToRestClientCTXMapIndex(IASTestMapper mapper) {
        Map<String, Object> ctxMap = new HashMap<>();
        ctxMap.put("type", AerospikeAPIConstants.CTX.MAP_INDEX);
        ctxMap.put("index", 11);

        try {
            MapIndexCTX restCTX = (MapIndexCTX) mapper.bytesToObject(mapper.objectToBytes(ctxMap));
            Assertions.assertEquals(restCTX.type, AerospikeAPIConstants.CTX.MAP_INDEX);
            Assertions.assertEquals(restCTX.index.intValue(), 11);
        } catch (Exception e) {
            Assertions.fail(String.format("Should have mapped to RestClientCTXListIndexCreate %s", e));
        }
    }

    @Test
    public void testRestClientCTXMapIndexToASCTX() {
        com.aerospike.client.cdt.CTX expected = com.aerospike.client.cdt.CTX.mapIndex(99);

        MapIndexCTX mapIndex = new MapIndexCTX();
        mapIndex.index = 99;

        ASTestUtils.compareCTX(expected, mapIndex.toCTX());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void tesMapToRestClientCTXMapRank(IASTestMapper mapper) {
        Map<String, Object> ctxMap = new HashMap<>();
        ctxMap.put("type", AerospikeAPIConstants.CTX.MAP_RANK);
        ctxMap.put("rank", 11);

        try {
            MapRankCTX restCTX = (MapRankCTX) mapper.bytesToObject(mapper.objectToBytes(ctxMap));
            Assertions.assertEquals(restCTX.type, AerospikeAPIConstants.CTX.MAP_RANK);
            Assertions.assertEquals(restCTX.rank.intValue(), 11);
        } catch (Exception e) {
            Assertions.fail(String.format("Should have mapped to RestClientCTXListIndexCreate %s", e));
        }
    }

    @Test
    public void testRestClientCTXMapRankToASCTX() {
        com.aerospike.client.cdt.CTX expected = com.aerospike.client.cdt.CTX.mapRank(99);

        MapRankCTX mapIndex = new MapRankCTX();
        mapIndex.rank = 99;

        ASTestUtils.compareCTX(expected, mapIndex.toCTX());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void tesMapToRestClientCTXMapKey(IASTestMapper mapper) {
        Map<String, Object> ctxMap = new HashMap<>();
        ctxMap.put("type", AerospikeAPIConstants.CTX.MAP_KEY);
        ctxMap.put("key", true);

        try {
            MapKeyCTX restCTX = (MapKeyCTX) mapper.bytesToObject(mapper.objectToBytes(ctxMap));
            Assertions.assertEquals(restCTX.type, AerospikeAPIConstants.CTX.MAP_KEY);
            Assertions.assertEquals(restCTX.key, true);
        } catch (Exception e) {
            Assertions.fail(String.format("Should have mapped to RestClientCTXListIndexCreate %s", e));
        }
    }

    @Test
    public void testRestClientCTXMapKeyToASCTX() {
        com.aerospike.client.cdt.CTX expected = com.aerospike.client.cdt.CTX.mapKey(Value.get(3.14159));

        MapKeyCTX restCTX = new MapKeyCTX();
        restCTX.key = 3.14159;

        ASTestUtils.compareCTX(expected, restCTX.toCTX());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void tesMapToRestClientCTXMapKeyCreate(IASTestMapper mapper) {
        Map<String, Object> ctxMap = new HashMap<>();
        ctxMap.put("type", AerospikeAPIConstants.CTX.MAP_KEY_CREATE);
        ctxMap.put("key", true);
        ctxMap.put("order", MapOrder.KEY_VALUE_ORDERED);

        try {
            MapKeyCreateCTX restCTX = (MapKeyCreateCTX) mapper.bytesToObject(mapper.objectToBytes(ctxMap));
            Assertions.assertEquals(restCTX.type, AerospikeAPIConstants.CTX.MAP_KEY_CREATE);
            Assertions.assertEquals(restCTX.key, true);
            Assertions.assertEquals(restCTX.order, MapOrder.KEY_VALUE_ORDERED);
        } catch (Exception e) {
            Assertions.fail(String.format("Should have mapped to RestClientCTXListIndexCreate %s", e));
        }
    }

    @Test
    public void testRestClientCTXMapKeyCreateToASCTX() {
        com.aerospike.client.cdt.CTX expected = com.aerospike.client.cdt.CTX.mapKeyCreate(Value.get(3.14159),
                MapOrder.KEY_VALUE_ORDERED);

        MapKeyCreateCTX restCTX = new MapKeyCreateCTX();
        restCTX.key = 3.14159;
        restCTX.order = MapOrder.KEY_VALUE_ORDERED;

        ASTestUtils.compareCTX(expected, restCTX.toCTX());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void tesMapToRestClientCTXMapValue(IASTestMapper mapper) {
        Map<String, Object> ctxMap = new HashMap<>();
        ctxMap.put("type", AerospikeAPIConstants.CTX.MAP_VALUE);
        ctxMap.put("value", 10);

        try {
            MapValueCTX restCTX = (MapValueCTX) mapper.bytesToObject(mapper.objectToBytes(ctxMap));
            Assertions.assertEquals(AerospikeAPIConstants.CTX.MAP_VALUE, restCTX.type);
            Assertions.assertEquals(10, restCTX.value);
        } catch (Exception e) {
            Assertions.fail(String.format("Should have mapped to RestClientCTXListIndexCreate %s", e));
        }
    }

    @Test
    public void testRestClientCTXMapValueToASCTX() {
        com.aerospike.client.cdt.CTX expected = com.aerospike.client.cdt.CTX.mapValue(Value.get(3.14159));

        MapValueCTX restCTX = new MapValueCTX();
        restCTX.value = 3.14159;

        ASTestUtils.compareCTX(expected, restCTX.toCTX());
    }

}

class JsonCTXMapper extends ASTestMapper {
    public JsonCTXMapper() {
        super(JSONMessageConverter.getJSONObjectMapper(), CTX.class);
    }
}

class MsgPackCTXMapper extends ASTestMapper {

    public MsgPackCTXMapper() {
        super(MsgPackConverter.getASMsgPackObjectMapper(), CTX.class);
    }
}
