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
import com.aerospike.restclient.domain.operationmodels.OperationTypes;
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

@SpringBootTest
@AutoConfigureMockMvc
public class OperateV2ListCorrectTests {

    @Autowired
    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    List<Object> objectList;
    List<Object> objectMapList;
    private Map<String, Object> opRequest;
    private List<Map<String, Object>> opList;

    private final String OPERATION_FIELD = "type";

    public static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JSONOperationV2Performer(), true),
                Arguments.of(new MsgPackOperationV2Performer(), true),
                Arguments.of(new JSONOperationV2Performer(), false),
                Arguments.of(new MsgPackOperationV2Performer(), false)
        );
    }

    private static Key keyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", "listop") : new Key("test", null, "listop");
    }

    private static String endpointFor(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV2("operate", "test", "junit", "listop") : ASTestUtils.buildEndpointV2("operate", "test", "listop");
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
    public void testListAppend(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);

        Map<String, Object> opMap = new HashMap<>();

        opRequest.put("opsList", opList);
        opList.add(opMap);
        opMap.put("binName", "list");
        opMap.put("value", "aerospike");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_APPEND);
//        

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

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
    public void testListAppendItems(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);

        Map<String, Object> opMap = new HashMap<>();

        List<Object> appendValues = Arrays.asList("aero", "spike", "aero");

        opMap.put("binName", "list");
        opMap.put("values", appendValues);
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_APPEND_ITEMS);

        opList.add(opMap);
        opRequest.put("opsList", opList);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

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
    public void testListAppendItemsWithPolicy(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        Map<String, Object> listPolicyMap = buildListPolicyMap(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE);
        List<Object> appendValues = Arrays.asList("aero", "spike");

        opMap.put("values", appendValues);
        opMap.put("listPolicy", listPolicyMap);
        opMap.put("binName", "list");

        opMap.put(OPERATION_FIELD, OperationTypes.LIST_APPEND_ITEMS);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

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
    public void testListClear(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_CLEAR);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        @SuppressWarnings("unchecked") List<Object> realList = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        Assertions.assertEquals(realList.size(), 0);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGet(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 2);
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(binsObject.get("list"), objectList.get(2)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByIndexIndex(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 2);
        opMap.put("listReturnType", "INDEX");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_BY_INDEX);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(binsObject.get("list"), 2));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByIndexReverseRank(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 2);
        opMap.put("listReturnType", "REVERSE_RANK");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_BY_INDEX);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(binsObject.get("list"), 4));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByIndexRange(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put("count", 3);
        opMap.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_BY_INDEX_RANGE);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        List<Object> retItems = (List<Object>) binsObject.get("list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(2, 0, 3)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByIndexRangeNoCount(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_BY_INDEX_RANGE);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        List<Object> retItems = (List<Object>) binsObject.get("list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(2, 0, 3, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByRankValue(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("rank", 2);
        opMap.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_BY_RANK);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(binsObject.get("list"), 2));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByRankIndex(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("rank", 2);
        opMap.put("listReturnType", "INDEX");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_BY_RANK);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(binsObject.get("list"), 1));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByRankRange(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("rank", 1);
        opMap.put("count", 3);
        opMap.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_BY_RANK_RANGE);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListGetByRankRangeNoCount(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("rank", 1);
        opMap.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_BY_RANK_RANGE);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListGetByValueRelRankRange(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("rank", 1);
        opMap.put("value", 2);
        opMap.put("count", 2);
        opMap.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_BY_VALUE_RELATIVE_RANK_RANGE);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListGetByValueRelRankRangeNoCount(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("rank", 2);
        opMap.put("value", 0);
        opMap.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_BY_VALUE_RELATIVE_RANK_RANGE);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListGetByValueIndex(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("value", 0);
        opMap.put("listReturnType", "INDEX");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_BY_VALUE);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        Assertions.assertTrue(
                ASTestUtils.compareCollection((List<?>) binsObject.get("list"), Collections.singletonList(2)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetByValueRange(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("valueBegin", 1);
        opMap.put("valueEnd", 4);
        opMap.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_BY_VALUE_RANGE);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListGetByValueRangeNoEnd(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("valueBegin", 1);
        opMap.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_BY_VALUE_RANGE);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListGetByValueRangeNoBegin(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("valueEnd", 4);
        opMap.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_BY_VALUE_RANGE);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListGetByValueList(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("values", Arrays.asList(0, 1, 4));
        opMap.put("listReturnType", "INDEX");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_BY_VALUE_LIST);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListGetByMapValueList(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "map");
        opMap.put("values", objectMapList);
        opMap.put("listReturnType", "INDEX");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_BY_VALUE_LIST);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListGetRange(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put("count", 3);
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_RANGE);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        List<Object> retItems = (List<Object>) binsObject.get("list");

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(2, 0, 3)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testListGetRangeNoCount(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_GET_RANGE);

        opList.add(opMap);

        Map<String, Object> binsObject = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        List<Object> retItems = (List<Object>) binsObject.get("list");

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(2, 0, 3, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListIncrement(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put("incr", 10);
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_INCREMENT);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(retItems.get(1), 12));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListIncrementWithPolicy(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        Map<String, Object> listPolicyMap = buildListPolicyMap(ListOrder.UNORDERED, ListWriteFlags.DEFAULT);

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put("incr", 10);
        opMap.put("listPolicy", listPolicyMap);

        opMap.put(OPERATION_FIELD, OperationTypes.LIST_INCREMENT);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(retItems.get(1), 12));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListInsert(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put("value", "one");

        opMap.put(OPERATION_FIELD, OperationTypes.LIST_INSERT);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

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
    public void testListInsertPolicy(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        Map<String, Object> listPolicyMap = buildListPolicyMap(ListOrder.UNORDERED, ListWriteFlags.DEFAULT);

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put("value", 5);
        opMap.put("listPolicy", listPolicyMap);
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_INSERT);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(1, 5, 2, 0, 3, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListInsertItems(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put("values", Arrays.asList("one", "two", "three"));

        opMap.put(OPERATION_FIELD, OperationTypes.LIST_INSERT_ITEMS);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

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
    public void testListInsertItemsPolicy(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        Map<String, Object> listPolicyMap = buildListPolicyMap(ListOrder.UNORDERED, ListWriteFlags.DEFAULT);

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put("values", Arrays.asList("one", "two", "three"));
        opMap.put("listPolicy", listPolicyMap);
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_INSERT_ITEMS);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

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
    public void testListPop(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_POP);

        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListPopRange(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put("count", 3);
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_POP_RANGE);

        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListPopRangeNoCount(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_POP_RANGE);

        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListRemove(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 2);
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_REMOVE);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

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
    public void testListRemoveByIndex(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 2);
        opMap.put("listReturnType", "RANK");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_REMOVE_BY_INDEX);

        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListRemoveByIndexRange(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put("count", 3);
        opMap.put("listReturnType", "COUNT");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_REMOVE_BY_INDEX_RANGE);

        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListRemoveByIndexRangeNoCount(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put("listReturnType", "COUNT");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_REMOVE_BY_INDEX_RANGE);

        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListRemoveByRank(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("rank", 0);
        opMap.put("listReturnType", "INDEX");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_REMOVE_BY_RANK);

        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListRemoveByRankRange(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("rank", 1);
        opMap.put("count", 3);
        opMap.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_REMOVE_BY_RANK_RANGE);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(0, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveByRankRangeNoCount(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("rank", 1);
        opMap.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_REMOVE_BY_RANK_RANGE);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Collections.singletonList(0)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveByValueRelRankRange(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("value", 1);
        opMap.put("rank", 1);
        opMap.put("count", 2);
        opMap.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_REMOVE_BY_VALUE_RELATIVE_RANK_RANGE);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(1, 0, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveByValueRelRankRangeNoCount(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("value", 1);
        opMap.put("rank", 1);
        opMap.put("listReturnType", "VALUE");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_REMOVE_BY_VALUE_RELATIVE_RANK_RANGE);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(1, 0)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListRemoveByValue(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        objectList.add(0);
        Bin newListBin = new Bin("list", objectList);
        client.put(null, testKey, newListBin);

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("value", 0);
        opMap.put("listReturnType", "INDEX");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_REMOVE_BY_VALUE);

        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListRemoveByValueRange(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("valueBegin", 1);
        opMap.put("valueEnd", 3);
        opMap.put("listReturnType", "COUNT");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_REMOVE_BY_VALUE_RANGE);

        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListRemoveByValueRangeNoBegin(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("valueEnd", 3);
        opMap.put("listReturnType", "COUNT");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_REMOVE_BY_VALUE_RANGE);

        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListRemoveByValueRangeNoEnd(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("valueBegin", 1);
        opMap.put("listReturnType", "COUNT");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_REMOVE_BY_VALUE_RANGE);

        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListRemoveByValueList(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        objectList.add(0);
        Bin newListBin = new Bin("list", objectList);
        client.put(null, testKey, newListBin);

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("values", Arrays.asList(0, 2, 4));
        opMap.put("listReturnType", "COUNT");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_REMOVE_BY_VALUE_LIST);

        opList.add(opMap);

        Map<String, Object> returnedBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

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
    public void testListRemoveRange(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put("count", 3);
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_REMOVE_RANGE);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

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
    public void testListRemoveRangeNoCount(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_REMOVE_RANGE);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

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
    public void testListSetValue(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("value", "two");
        opMap.put("index", 1);
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_SET);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(1, "two", 0, 3, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListSetValueWithPolicy(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        Map<String, Object> listPolicyMap = buildListPolicyMap(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE);

        opMap.put("binName", "list");
        opMap.put("value", "two");
        opMap.put("index", 1);
        opMap.put("listPolicy", listPolicyMap);
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_SET);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(1, "two", 0, 3, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListSetOrder(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("listOrder", "ORDERED");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_SET_ORDER);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");

        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(0, 1, 2, 3, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListSize(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_SIZE);

        opList.add(opMap);

        Map<String, Object> retBins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(retBins.get("list"), 5));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListSort(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_SORT);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(0, 1, 2, 3, 4)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListTrim(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "list");
        opMap.put("index", 1);
        opMap.put("count", 3);
        opMap.put(OPERATION_FIELD, OperationTypes.LIST_TRIM);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        @SuppressWarnings("unchecked") List<Object> retItems = (List<Object>) client.get(null, testKey).bins.get(
                "list");
        Assertions.assertTrue(ASTestUtils.compareCollection(retItems, Arrays.asList(2, 0, 3)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testListCreate(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.put(null, testKey, new Bin("str", "bin"), new Bin("list", objectList), new Bin("map", objectMapList));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();

        Map<String, Object> item = new HashMap<>();
        List<Map<String, Object>> ctx = new ArrayList<>();

        opMap.put("binName", "list");
        opMap.put("ctx", ctx);
        opMap.put("order", "UNORDERED");
        opMap.put("pad", true);

        item.put("type", "listIndex");
        item.put("index", 7);
        ctx.add(item);

        opMap.put(OPERATION_FIELD, OperationTypes.LIST_CREATE);

        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

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
    private Map<String, Object> getReturnedBins(Map<String, Object> resp) {
        Map<String, Object> record = (Map<String, Object>) resp.get("record");
        return (Map<String, Object>) record.get("bins");
    }

}
