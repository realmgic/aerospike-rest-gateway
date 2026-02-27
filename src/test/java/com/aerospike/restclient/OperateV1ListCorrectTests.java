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
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.restclient.util.AerospikeOperation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import java.util.*;
import java.util.stream.Stream;

import static com.aerospike.restclient.util.AerospikeAPIConstants.OPERATION_FIELD;
import static com.aerospike.restclient.util.AerospikeAPIConstants.OPERATION_VALUES_FIELD;

@SpringBootTest
@AutoConfigureMockMvc
public class OperateV1ListCorrectTests {

    @Autowired
    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    List<Object> objectList;
    List<Object> objectMapList;

    public static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JSONOperationV1Performer(), true),
                Arguments.of(new MsgPackOperationV1Performer(), true),
                Arguments.of(new JSONOperationV1Performer(), false),
                Arguments.of(new MsgPackOperationV1Performer(), false)
        );
    }

    private static Key keyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", "listop") : new Key("test", null, "listop");
    }

    private static String endpointFor(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("operate", "test", "junit", "listop") : ASTestUtils.buildEndpointV1("operate", "test", "listop");
    }

    @BeforeEach
    public void setup() {
        objectList = new ArrayList<>();
        objectList.add(1L);
        objectList.add(2L);
        objectList.add(0L);
        objectList.add(3L);
        objectList.add(4L);

        Map<String, String> mapValue = new HashMap<>();
        mapValue.put("3", "b");
        mapValue.put("1", "a");
        mapValue.put("5", "c");

        objectMapList = new ArrayList<>();
        objectMapList.add(mapValue);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListAppend(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "list");
        opValues.put("value", "aerospike");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_APPEND);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> realList = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        objectList.add("aerospike");

        Assertions.assertTrue(ASTestUtils.compareCollection(objectList, realList));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListAppendItems(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        List<Object> appendValues = Arrays.asList("aero", "spike", "aero");

        opValues.put("bin", "list");
        opValues.put("values", appendValues);
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_APPEND_ITEMS);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> realList = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        objectList.add("aero");
        objectList.add("spike");
        objectList.add("aero");

        Assertions.assertTrue(ASTestUtils.compareCollection(objectList, realList));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListAppendItemsWithPolicy(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        Map<String, Object> listPolicyMap = buildListPolicyMap(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE);
        List<Object> appendValues = Arrays.asList("aero", "spike");

        opValues.put("values", appendValues);
        opValues.put("listPolicy", listPolicyMap);
        opValues.put("bin", "list");

        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_APPEND_ITEMS);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> realList = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        objectList.add("aero");
        objectList.add("spike");

        Assertions.assertTrue(ASTestUtils.compareCollection(objectList, realList));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListClear(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_CLEAR);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> realList = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        Assertions.assertEquals(realList.size(), 0);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGet(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 2);
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(binsObject.get("list"), objectList.get(2)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByIndexIndex(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 2);
        opValues.put("listReturnType", "INDEX");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_BY_INDEX);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(binsObject.get("list"), 2));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByIndexReverseRank(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 2);
        opValues.put("listReturnType", "REVERSE_RANK");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_BY_INDEX);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(binsObject.get("list"), 4));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByIndexRange(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opValues.put("count", 3);
        opValues.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_BY_INDEX_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        List<Object> retItems = (List<Object>) binsObject.get("list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(2, 0, 3)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByIndexRangeNoCount(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opValues.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_BY_INDEX_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        List<Object> retItems = (List<Object>) binsObject.get("list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(2, 0, 3, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByRankValue(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("rank", 2);
        opValues.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_BY_RANK);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(binsObject.get("list"), 2));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByRankIndex(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("rank", 2);
        opValues.put("listReturnType", "INDEX");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_BY_RANK);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(binsObject.get("list"), 1));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByRankRange(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("rank", 1);
        opValues.put("count", 3);
        opValues.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_BY_RANK_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) binsObject.get("list");

        Set<Object> retItemSet = new HashSet<>(retItems);
        Set<Object> expectedSet = new HashSet<>(Arrays.asList(1, 2, 3));
        Assertions.assertEquals(retItemSet, expectedSet);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByRankRangeNoCount(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("rank", 1);
        opValues.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_BY_RANK_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) binsObject.get("list");

        Set<Object> retItemSet = new HashSet<>(retItems);
        Set<Object> expectedSet = new HashSet<>(Arrays.asList(1, 2, 3, 4));
        Assertions.assertEquals(retItemSet, expectedSet);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByValueRelRankRange(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("rank", 1);
        opValues.put("value", 2);
        opValues.put("count", 2);
        opValues.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_BY_VALUE_REL_RANK_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) binsObject.get("list");

        Set<Object> retItemSet = new HashSet<>(retItems);
        Set<Object> expectedSet = new HashSet<>(Arrays.asList(3, 4));
        Assertions.assertEquals(retItemSet, expectedSet);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByValueRelRankRangeNoCount(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("rank", 2);
        opValues.put("value", 0);
        opValues.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_BY_VALUE_REL_RANK_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) binsObject.get("list");

        Set<Object> retItemSet = new HashSet<>(retItems);
        Set<Object> expectedSet = new HashSet<>(Arrays.asList(2, 3, 4));
        Assertions.assertEquals(retItemSet, expectedSet);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByValueIndex(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("value", 0);
        opValues.put("listReturnType", "INDEX");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_BY_VALUE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        Assertions.assertTrue(
                ASTestUtils.compareCollection((List<?>) binsObject.get("list"), Collections.singletonList(2)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByValueRange(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("valueBegin", 1);
        opValues.put("valueEnd", 4);
        opValues.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_BY_VALUE_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        List<Object> retItems = (List<Object>) binsObject.get("list");

        Set<Object> retItemSet = new HashSet<>(retItems);
        Set<Object> expectedSet = new HashSet<>(Arrays.asList(1, 2, 3));
        Assertions.assertEquals(retItemSet, expectedSet);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByValueRangeNoEnd(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("valueBegin", 1);
        opValues.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_BY_VALUE_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) binsObject.get("list");

        Set<Object> retItemSet = new HashSet<>(retItems);
        Set<Object> expectedSet = new HashSet<>(Arrays.asList(1, 2, 3, 4));
        Assertions.assertEquals(retItemSet, expectedSet);
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByValueRangeNoBegin(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("valueEnd", 4);
        opValues.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_BY_VALUE_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        List<Object> retItems = (List<Object>) binsObject.get("list");

        Set<Object> retItemSet = new HashSet<>(retItems);
        Set<Object> expectedSet = new HashSet<>(Arrays.asList(0, 1, 2, 3));
        Assertions.assertEquals(retItemSet, expectedSet);
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByValueList(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("values", Arrays.asList(0, 1, 4));
        opValues.put("listReturnType", "INDEX");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_BY_VALUE_LIST);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        List<Object> retItems = (List<Object>) binsObject.get("list");

        Set<Object> retItemSet = new HashSet<>(retItems);
        Set<Object> expectedSet = new HashSet<>(Arrays.asList(2, 0, 4));
        Assertions.assertEquals(retItemSet, expectedSet);
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByMapValueList(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "map");
        opValues.put("values", objectMapList);
        opValues.put("listReturnType", "INDEX");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_BY_VALUE_LIST);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        List<Object> retItems = (List<Object>) binsObject.get("map");

        Set<Object> retItemSet = new HashSet<>(retItems);
        Set<Object> expectedSet = new HashSet<>(Collections.singletonList(0));
        Assertions.assertEquals(expectedSet, retItemSet);
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetRange(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opValues.put("count", 3);
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        List<Object> retItems = (List<Object>) binsObject.get("list");

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(2, 0, 3)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetRangeNoCount(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_GET_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        List<Object> retItems = (List<Object>) binsObject.get("list");

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(2, 0, 3, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListIncrement(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opValues.put("incr", 10);
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_INCREMENT);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(retItems.get(1), 12));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListIncrementNoValue(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "list");
        opValues.put("index", 1);
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_INCREMENT);
        opMap.put(OPERATION_VALUES_FIELD, opValues);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(retItems.get(1), 3));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListIncrementWithPolicy(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        Map<String, Object> listPolicyMap = buildListPolicyMap(ListOrder.UNORDERED, ListWriteFlags.DEFAULT);

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opValues.put("incr", 10);
        opValues.put("listPolicy", listPolicyMap);

        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_INCREMENT);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(retItems.get(1), 12));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListInsert(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opValues.put("value", "one");

        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_INSERT);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        objectList.add(1, "one");

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, objectList));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListInsertPolicy(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        Map<String, Object> listPolicyMap = buildListPolicyMap(ListOrder.UNORDERED, ListWriteFlags.DEFAULT);

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opValues.put("value", 5);
        opValues.put("listPolicy", listPolicyMap);
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_INSERT);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(1, 5, 2, 0, 3, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListInsertItems(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opValues.put("values", Arrays.asList("one", "two", "three"));

        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_INSERT_ITEMS);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        objectList.add(1, "three");
        objectList.add(1, "two");
        objectList.add(1, "one");

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, objectList));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListInsertItemsPolicy(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        Map<String, Object> listPolicyMap = buildListPolicyMap(ListOrder.UNORDERED, ListWriteFlags.DEFAULT);

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opValues.put("values", Arrays.asList("one", "two", "three"));
        opValues.put("listPolicy", listPolicyMap);
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_INSERT_ITEMS);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        objectList.add(1, "three");
        objectList.add(1, "two");
        objectList.add(1, "one");

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, objectList));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListPop(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_POP);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(returnedBins.get("list"), 2));

        List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get("list");

        objectList.remove(1);
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, objectList));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListPopRange(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opValues.put("count", 3);
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_POP_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(returnedBins.get("list"), Arrays.asList(2, 0, 3)));

        List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get("list");
        // Remove 3 items starting from index 1
        objectList.remove(1);
        objectList.remove(1);
        objectList.remove(1);

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, objectList));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListPopRangeNoCount(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_POP_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(returnedBins.get("list"), Arrays.asList(2, 0, 3, 4)));

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        // Remove 4 items starting from index 1
        objectList.remove(1);
        objectList.remove(1);
        objectList.remove(1);
        objectList.remove(1);

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, objectList));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemove(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 2);
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_REMOVE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        objectList.remove(2);
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, objectList));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveByIndex(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 2);
        opValues.put("listReturnType", "RANK");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_REMOVE_BY_INDEX);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        //The popped value was the smallest element, so it's rank should be 0
        Assertions.assertTrue(ASTestUtils.compareSimpleValues(returnedBins.get("list"), 0));

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        objectList.remove(2);
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, objectList));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveByIndexRange(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opValues.put("count", 3);
        opValues.put("listReturnType", "COUNT");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_REMOVE_BY_INDEX_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        //Three items were removed, so the server should return 3 items
        Assertions.assertTrue(ASTestUtils.compareSimpleValues(returnedBins.get("list"), 3));

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        objectList.remove(1);
        objectList.remove(1);
        objectList.remove(1);
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, objectList));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveByIndexRangeNoCount(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opValues.put("listReturnType", "COUNT");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_REMOVE_BY_INDEX_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        //Three items were removed, so the server should return 3 items
        Assertions.assertTrue(ASTestUtils.compareSimpleValues(returnedBins.get("list"), 4));

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        objectList.remove(1);
        objectList.remove(1);
        objectList.remove(1);
        objectList.remove(1);
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, objectList));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveByRank(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("rank", 0);
        opValues.put("listReturnType", "INDEX");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_REMOVE_BY_RANK);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        //The popped value was the smallest element, it was at index 2
        Assertions.assertTrue(ASTestUtils.compareSimpleValues(returnedBins.get("list"), 2));

        List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get("list");
        objectList.remove(2);
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, objectList));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveByRankRange(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("rank", 1);
        opValues.put("count", 3);
        opValues.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_REMOVE_BY_RANK_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(0, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveByRankRangeNoCount(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("rank", 1);
        opValues.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_REMOVE_BY_RANK_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Collections.singletonList(0)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveByValueRelRankRange(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("value", 1);
        opValues.put("rank", 1);
        opValues.put("count", 2);
        opValues.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_REMOVE_BY_VALUE_REL_RANK_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(1, 0, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveByValueRelRankRangeNoCount(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("value", 1);
        opValues.put("rank", 1);
        opValues.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_REMOVE_BY_VALUE_REL_RANK_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(1, 0)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveByValue(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        objectList.add(0);
        Bin newListBin = new Bin("list", objectList);
        client.put(null, testKey, newListBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("value", 0);
        opValues.put("listReturnType", "INDEX");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_REMOVE_BY_VALUE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        //The popped value was the smallest element, it was at index 2
        Assertions.assertTrue(ASTestUtils.compareCollection((List<?>) returnedBins.get("list"), Arrays.asList(2, 5)));

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        objectList.remove(5);
        objectList.remove(2);
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, objectList));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveByValueRange(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("valueBegin", 1);
        opValues.put("valueEnd", 3);
        opValues.put("listReturnType", "COUNT");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_REMOVE_BY_VALUE_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        //The popped value was the smallest element, it was at index 2
        Assertions.assertTrue(ASTestUtils.compareSimpleValues(returnedBins.get("list"), 2));

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(0, 3, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveByValueRangeNoBegin(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("valueEnd", 3);
        opValues.put("listReturnType", "COUNT");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_REMOVE_BY_VALUE_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        //The popped value was the smallest element, it was at index 2
        Assertions.assertTrue(ASTestUtils.compareSimpleValues(returnedBins.get("list"), 3));

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(3, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveByValueRangeNoEnd(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("valueBegin", 1);
        opValues.put("listReturnType", "COUNT");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_REMOVE_BY_VALUE_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        //The popped value was the smallest element, it was at index 2
        Assertions.assertTrue(ASTestUtils.compareSimpleValues(returnedBins.get("list"), 4));

        List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get("list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Collections.singletonList(0)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveByValueList(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        objectList.add(0);
        Bin newListBin = new Bin("list", objectList);
        client.put(null, testKey, newListBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("values", Arrays.asList(0, 2, 4));
        opValues.put("listReturnType", "COUNT");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_REMOVE_BY_VALUE_LIST);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        //The popped value was the smallest element, it was at index 2
        Assertions.assertTrue(ASTestUtils.compareSimpleValues(returnedBins.get("list"), 4));

        List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get("list");

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(1, 3)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveRange(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opValues.put("count", 3);
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_REMOVE_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        objectList.remove(1);
        objectList.remove(1);
        objectList.remove(1);
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, objectList));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveRangeNoCount(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_REMOVE_RANGE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        objectList.remove(1);
        objectList.remove(1);
        objectList.remove(1);
        objectList.remove(1);
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, objectList));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListSetValue(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("value", "two");
        opValues.put("index", 1);
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_SET);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(1, "two", 0, 3, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListSetValueWithPolicy(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        Map<String, Object> listPolicyMap = buildListPolicyMap(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE);

        opValues.put("bin", "list");
        opValues.put("value", "two");
        opValues.put("index", 1);
        opValues.put("listPolicy", listPolicyMap);
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_SET);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(1, "two", 0, 3, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListSetOrder(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("listOrder", "ORDERED");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_SET_ORDER);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(0, 1, 2, 3, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListSize(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_SIZE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> retBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(retBins.get("list"), 5));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListSort(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
         Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_SORT);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(0, 1, 2, 3, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListTrim(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "list");
        opValues.put("index", 1);
        opValues.put("count", 3);
        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_TRIM);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(2, 0, 3)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListCreate(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey,  new Bin("str", "bin"), 
                                              new Bin("list", objectList), 
                                              new Bin("map", objectMapList));
            

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();                                              

        Map<String, Object> item = new HashMap<>();
        List<Map<String, Object>> ctx = new ArrayList<>();

        opValues.put("bin", "list");
        opValues.put("ctx", ctx);
        opValues.put("listOrder", "UNORDERED");
        opValues.put("pad", true);


        item.put("type", "listIndex");
        item.put("index", 7);
        ctx.add(item);

        opMap.put(OPERATION_FIELD, AerospikeOperation.LIST_CREATE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        Assertions.assertTrue(((List<?>) retItems.get(7)).isEmpty());
        } finally {
            client.delete(null, testKey);
        }
    }

    private Map<String, Object> buildListPolicyMap(ListOrder order, int flags) {
        Map<String, Object> policyMap = new HashMap<>();
        List<Object> flagStrings = new ArrayList<>();
        if (order == ListOrder.ORDERED) {
            policyMap.put("order", "ORDERED");
        } else if (order == ListOrder.UNORDERED) {
            policyMap.put("order", "UNORDERED");
        }
        if ((flags & ListWriteFlags.ADD_UNIQUE) != 0) {
            flagStrings.add("ADD_UNIQUE");
        }
        if ((flags & ListWriteFlags.INSERT_BOUNDED) != 0) {
            flagStrings.add("INSERT_BOUNDED");
        }
        policyMap.put("writeFlags", flagStrings);
        return policyMap;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getReturnedBins(Map<String, Object> rec) {
        return (Map<String, Object>) rec.get("bins");
    }

}


