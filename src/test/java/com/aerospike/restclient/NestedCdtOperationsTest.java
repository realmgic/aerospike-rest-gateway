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
package com.aerospike.restclient;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.restclient.util.AerospikeOperation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.aerospike.restclient.util.AerospikeAPIConstants.OPERATION_FIELD;
import static com.aerospike.restclient.util.AerospikeAPIConstants.OPERATION_VALUES_FIELD;

@SpringBootTest
public class NestedCdtOperationsTest {

    private static final Key testKey = new Key("test", "junit", "nested");
    private static final String testEndpoint = ASTestUtils.buildEndpointV1("operate", "test", "junit", "nested");

    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    private List<Object> l1, l2, l3, objectList;
    private Map<Object, Object> m1, m2, m3, objectMap;

    public static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JSONOperationV1Performer()),
                Arguments.of(new MsgPackOperationV1Performer())
        );
    }

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();

        l1 = new ArrayList<>();
        l1.add(7);
        l1.add(9);
        l1.add(5);

        l2 = new ArrayList<>();
        l2.add(1);
        l2.add(2);
        l2.add(3);

        l3 = new ArrayList<>();
        l3.add(6);
        l3.add(5);
        l3.add(4);
        l3.add(1);

        objectList = new ArrayList<>();
        objectList.add(l1);
        objectList.add(l2);
        objectList.add(l3);

        Bin listBin = new Bin("list", objectList);

        m1 = new HashMap<>();
        m1.put("one", 1);
        m1.put("two", 2);
        m1.put("three", 3);

        m3 = new HashMap<>();
        m3.put("one", 1);
        m3.put("two", 2);
        m3.put("three", 3);

        m2 = new HashMap<>();
        m2.put("one", 1);
        m2.put("two", 2);
        m2.put("three", 3);
        m2.put("m3", m3);

        objectMap = new HashMap<>();
        objectMap.put("m1", m1);
        objectMap.put("m2", m2);

        Bin mapBin = new Bin("map", objectMap);
        client.put(null, testKey, listBin, mapBin);
    }

    @AfterEach
    public void clean() {
        client.delete(null, testKey);
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListAppendCdtListIndex(OperationV1Performer opPerformer) {
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        Map<String, Object> item = new HashMap<>();
        List<Map<String, Object>> ctx = new ArrayList<>();
        opValues.put("bin", "list");
        opValues.put("value", 100);

        item.put("type", "listIndex");
        item.put("index", -1);
        ctx.add(item);
        opValues.put("ctx", ctx);

        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_APPEND);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);
        List<Object> realList = (List<Object>) client.get(null, testKey).bins.get("list");
        l3.add(100);

        Assertions.assertTrue(ASTestUtils.compareCollection(objectList, realList));
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListAppendCdtListValue(OperationV1Performer opPerformer) {
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        Map<String, Object> item = new HashMap<>();
        List<Map<String, Object>> ctx = new ArrayList<>();

        opValues.put("bin", "list");
        opValues.put("value", 100);

        item.put("type", "listValue");
        item.put("value", l2);
        ctx.add(item);
        opValues.put("ctx", ctx);

        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_APPEND);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);
        List<Object> realList = (List<Object>) client.get(null, testKey).bins.get("list");
        l2.add(100);

        Assertions.assertTrue(ASTestUtils.compareCollection(objectList, realList));
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapPutCdtMapKey(OperationV1Performer opPerformer) {
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> operation = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        Map<String, Object> item = new HashMap<>();
        List<Map<String, Object>> ctx = new ArrayList<>();
        opValues.put("bin", "map");
        opValues.put("key", "one");
        opValues.put("value", 11);

        item.put("type", "mapKey");
        item.put("key", "m1");
        ctx.add(item);
        opValues.put("ctx", ctx);

        operation.put(OPERATION_FIELD, AerospikeOperation.MAP_PUT);
        operation.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(operation);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);
        Map<String, Object> bins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) bins.get("map");
        m1.put("one", 11);

        Assertions.assertTrue(ASTestUtils.compareMap(realMapBin, objectMap));
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapPutCdtMapRank(OperationV1Performer opPerformer) {
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> operation = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        Map<String, Object> item = new HashMap<>();
        List<Map<String, Object>> ctx = new ArrayList<>();
        opValues.put("bin", "map");
        opValues.put("key", "one");
        opValues.put("value", 11);

        item.put("type", "mapRank");
        item.put("rank", 1);
        ctx.add(item);
        opValues.put("ctx", ctx);

        operation.put(OPERATION_FIELD, AerospikeOperation.MAP_PUT);
        operation.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(operation);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);
        Map<String, Object> bins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) bins.get("map");
        m2.put("one", 11);

        Assertions.assertTrue(ASTestUtils.compareMap(realMapBin, objectMap));
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapPutCdtMultipuleCTX(OperationV1Performer opPerformer) {
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> operation = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        Map<String, Object> item1 = new HashMap<>();
        Map<String, Object> item2 = new HashMap<>();
        List<Map<String, Object>> ctx = new ArrayList<>();
        opValues.put("bin", "map");
        opValues.put("key", "two");
        opValues.put("value", 11);

        item1.put("type", "mapKey");
        item1.put("key", "m2");
        ctx.add(item1);

        item2.put("type", "mapKey");
        item2.put("key", "m3");
        ctx.add(item2);
        opValues.put("ctx", ctx);

        operation.put(OPERATION_FIELD, AerospikeOperation.MAP_PUT);
        operation.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(operation);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);
        Map<String, Object> bins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) bins.get("map");
        m3.put("two", 11);

        Assertions.assertTrue(ASTestUtils.compareMap(realMapBin, objectMap));
    }
}
