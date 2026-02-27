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
import com.aerospike.client.Operation;
import com.aerospike.client.cdt.*;
import com.aerospike.restclient.domain.operationmodels.OperationTypes;
import com.aerospike.restclient.util.converters.OperationConverter;
import org.junit.jupiter.api.Assertions;
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

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
public class OperateV2MapCorrectTests {

    @Autowired
    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    private Map<Object, Object> objectMap;
    private Map<Object, Object> objectMapInt;
    private Map<String, Object> opRequest;
    private List<Map<String, Object>> opList;
    private final String OPERATION_FIELD_TYPE = "type";
    private final String mapBinName = "map";
    private final String mapBinNameInt = "mapint";

    public static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JSONOperationV2Performer(), true),
                Arguments.of(new MsgPackOperationV2Performer(), true),
                Arguments.of(new JSONOperationV2Performer(), false),
                Arguments.of(new MsgPackOperationV2Performer(), false)
        );
    }

    private static Key keyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", "mapop") : new Key("test", null, "mapop");
    }

    private static String endpointFor(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV2("operate", "test", "junit", "mapop") : ASTestUtils.buildEndpointV2("operate", "test", "mapop");
    }

    @BeforeEach
    public void setup() {
        objectMap = new HashMap<>();
        objectMap.put("one", 1);
        objectMap.put("two", 2);
        objectMap.put("three", 3);
        objectMap.put("ten", 10);
        objectMap.put("aero", "spike");

        objectMapInt = new HashMap<>();
        objectMapInt.put("one", 1);
        objectMapInt.put("two", 2);
        objectMapInt.put("three", 3);
        objectMapInt.put("ten", 10);
        objectMapInt.put("zero", 0);
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapClear(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);

        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_CLEAR);

        opList.add(operation);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        Map<String, Object> bins = client.get(null, testKey).bins;
        Map<String, Object> realMapBin = (Map<String, Object>) bins.get(mapBinName);
        Assertions.assertEquals(realMapBin.size(), 0);
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapIncrement(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        Map<String, Object> policy = getMapPolicyMap(MapOrder.UNORDERED, MapWriteFlags.DEFAULT);

        operation.put(OperationConverter.MAP_POLICY_KEY, policy);
        operation.put("binName", mapBinName);
        operation.put("key", "ten");
        operation.put("incr", 3);
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_INCREMENT);

        opList.add(operation);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        Map<String, Object> bins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) bins.get(mapBinName);
        objectMap.put("ten", 13);
        Assertions.assertTrue(ASTestUtils.compareMap(realMapBin, objectMap));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByIndex(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);
        operation.put("index", 0);
        operation.put(OperationConverter.MAP_RETURN_KEY, "KEY");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_INDEX);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(bins.get(mapBinName), "aero"));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByIndexRange(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);
        operation.put("index", 1);
        operation.put("count", 3);
        operation.put(OperationConverter.MAP_RETURN_KEY, "KEY");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_INDEX_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        List<Object> keys = (List<Object>) bins.get(mapBinName);

        Assertions.assertTrue(ASTestUtils.compareCollection(keys, Arrays.asList("one", "ten", "three")));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByIndexRangeNoCount(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);
        operation.put("index", 1);
        operation.put(OperationConverter.MAP_RETURN_KEY, "KEY");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_INDEX_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        List<Object> keys = (List<Object>) bins.get(mapBinName);

        Assertions.assertTrue(ASTestUtils.compareCollection(keys, Arrays.asList("one", "ten", "three", "two")));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByKey(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);
        operation.put("key", "three");
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_KEY);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(bins.get(mapBinName), 3));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapCreate(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        Map<String, Object> item = new HashMap<>();
        List<Map<String, Object>> ctx = new ArrayList<>();

        operation.put("binName", mapBinName);
        operation.put("mapOrder", "UNORDERED");
        item.put("type", "mapKey");
        item.put("key", "key1");
        ctx.add(item);
        operation.put("ctx", ctx);
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_CREATE);

        opList.add(operation);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        Map<String, Object> bins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) bins.get(mapBinName);
        Assertions.assertNotNull(realMapBin.get("key1"));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByKeyList(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);
        operation.put("keys", Arrays.asList("aero", "two", "three"));
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_KEY_LIST);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        List<Object> retValues = (List<Object>) bins.get(mapBinName);

        Assertions.assertTrue(ASTestUtils.compareCollection(retValues, Arrays.asList("spike", 3, 2)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByKeyRange(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);
        operation.put(OperationConverter.MAP_KEY_BEGIN_KEY, "one");
        // A value after "ten"
        operation.put(OperationConverter.MAP_KEY_END_KEY, "threez");
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_KEY_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        List<Object> retValues = (List<Object>) bins.get(mapBinName);

        Assertions.assertTrue(ASTestUtils.compareCollection(retValues, Arrays.asList(1, 10, 3)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByKeyRangeNoBegin(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);
        // A value after "ten"
        operation.put(OperationConverter.MAP_KEY_END_KEY, "threez");
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_KEY_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        List<Object> retValues = (List<Object>) bins.get(mapBinName);

        Assertions.assertTrue(ASTestUtils.compareCollection(retValues, Arrays.asList("spike", 1, 10, 3)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByKeyRangeNoEnd(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);
        operation.put(OperationConverter.MAP_KEY_BEGIN_KEY, "one");
        // A value after "ten"
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_KEY_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        List<Object> retValues = (List<Object>) bins.get(mapBinName);

        Assertions.assertTrue(ASTestUtils.compareCollection(retValues, Arrays.asList(1, 10, 3, 2)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByRank(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put("rank", 4);
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_RANK);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(bins.get(mapBinNameInt), 10));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByRankRange(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put("rank", 1);
        operation.put("count", 3);
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_RANK_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        @SuppressWarnings("unchecked") List<Object> retVals = (List<Object>) bins.get(mapBinNameInt);
        Assertions.assertTrue(ASTestUtils.compareCollection(retVals, Arrays.asList(1, 2, 3)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByRankRangeNoCount(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put("rank", 1);
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_RANK_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        @SuppressWarnings("unchecked") List<Object> retVals = (List<Object>) bins.get(mapBinNameInt);
        Assertions.assertTrue(ASTestUtils.compareCollection(retVals, Arrays.asList(1, 2, 3, 10)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByValue(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);
        operation.put("value", 3);
        operation.put(OperationConverter.MAP_RETURN_KEY, "KEY");
        /* Store a second item with the value of 3 to show that we get all keys with the provided value*/
        objectMap.put("threez", 3);
        Bin newBin = new Bin(mapBinName, objectMap);
        client.put(null, testKey, newBin);

        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_VALUE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        Assertions.assertTrue(
                ASTestUtils.compareCollection((List<Object>) bins.get(mapBinName), Arrays.asList("three", "threez")));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByValueRange(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put(OperationConverter.VALUE_BEGIN_KEY, 1);
        operation.put(OperationConverter.VALUE_END_KEY, 4);
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");

        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_VALUE_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        /* These keys come back in key sorted order, so "one" < "three" < "two" */
        Assertions.assertTrue(ASTestUtils.containSameItems((List<Object>) bins.get(mapBinNameInt), Arrays.asList(1, 3, 2)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByValueRangeNoBegin(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put(OperationConverter.VALUE_END_KEY, 4);
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");

        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_VALUE_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        /* These keys come back in key sorted order, so "one" < "three" < "two" < "zero" */
        Assertions.assertTrue(
                ASTestUtils.containSameItems((List<Object>) bins.get(mapBinNameInt), Arrays.asList(1, 3, 2, 0)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByValueRangeNoEnd(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put(OperationConverter.VALUE_BEGIN_KEY, 1);
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");

        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_VALUE_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));
        /* These keys come back in key sorted order, so "one" < "ten", "three" < "two" */
        Assertions.assertTrue(
                ASTestUtils.containSameItems((List<Object>) bins.get(mapBinNameInt), Arrays.asList(1, 10, 3, 2)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByValueList(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Assumptions.assumeTrue(ASTestUtils.supportsNewCDT(client));
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put("values", Arrays.asList(0, 2, 10));
        operation.put(OperationConverter.MAP_RETURN_KEY, "KEY");

        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_VALUE_LIST);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));
        /* These keys come back in key sorted order, so "ten" < "two" < "zero" */
        Assertions.assertTrue(ASTestUtils.containSameItems((List<Object>) bins.get(mapBinNameInt),
                Arrays.asList("ten", "two", "zero")));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByKeyRelIndexRange(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put("value", "one");
        operation.put("index", 1);

        operation.put(OperationConverter.MAP_RETURN_KEY, "KEY");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_KEY_RELATIVE_INDEX_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));
        List<Object> keys = (List<Object>) bins.get(mapBinNameInt);
        Assertions.assertTrue(ASTestUtils.compareCollection(keys, Arrays.asList("ten", "three", "two", "zero")));

        operation.put("count", 3);
        bins = getReturnedBins(opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));
        keys = (List<Object>) bins.get(mapBinNameInt);
        Assertions.assertTrue(ASTestUtils.compareCollection(keys, Arrays.asList("ten", "three", "two")));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapGetByValueRelRankRange(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put("value", 1);
        operation.put("rank", -1);
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_GET_BY_VALUE_RELATIVE_RANK_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));
        List<Object> retVals = (List<Object>) bins.get(mapBinNameInt);
        Assertions.assertTrue(ASTestUtils.compareCollection(retVals, Arrays.asList(0, 1, 2, 3, 10)));

        operation.put("count", 3);
        bins = getReturnedBins(opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));
        retVals = (List<Object>) bins.get(mapBinNameInt);
        Assertions.assertTrue(ASTestUtils.compareCollection(retVals, Arrays.asList(0, 1, 2)));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapPut(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        Map<String, Object> policy = getMapPolicyMap(MapOrder.UNORDERED, MapWriteFlags.DEFAULT);

        operation.put(OperationConverter.MAP_POLICY_KEY, policy);
        operation.put("binName", mapBinName);
        operation.put("key", "five");
        operation.put("value", 5);
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_PUT);

        opList.add(operation);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        Map<String, Object> bins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) bins.get(mapBinName);
        objectMap.put("five", 5);
        Assertions.assertTrue(ASTestUtils.compareMap(realMapBin, objectMap));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapPutItems(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        Map<String, Object> policy = getMapPolicyMap(MapOrder.UNORDERED, MapWriteFlags.DEFAULT);

        Map<Object, Object> putValues = new HashMap<>();
        putValues.put("five", 5);
        putValues.put("six", 6);
        putValues.put("list", Arrays.asList(1, 2, 3));

        objectMap.put("five", 5);
        objectMap.put("six", 6);
        objectMap.put("list", Arrays.asList(1, 2, 3));

        operation.put("map", putValues);
        operation.put(OperationConverter.MAP_POLICY_KEY, policy);
        operation.put("binName", mapBinName);

        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_PUT_ITEMS);

        opList.add(operation);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        Map<String, Object> bins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) bins.get(mapBinName);

        Assertions.assertTrue(ASTestUtils.compareMap(realMapBin, objectMap));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapRemoveByIndex(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);
        operation.put("index", 0);
        operation.put(OperationConverter.MAP_RETURN_KEY, "KEY");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_REMOVE_BY_INDEX);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));
        Assertions.assertTrue(ASTestUtils.compareSimpleValues(bins.get(mapBinName), "aero"));

        Map<String, Object> realBins = client.get(null, testKey).bins;
        @SuppressWarnings("unchecked") Map<Object, Object> realMapBin = (Map<Object, Object>) realBins.get(mapBinName);
        Assertions.assertFalse(realMapBin.containsKey("aero"));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapRemoveByIndexRange(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);
        operation.put("index", 0);
        operation.put("count", 3);
        operation.put(OperationConverter.MAP_RETURN_KEY, "KEY");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_REMOVE_BY_INDEX_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        List<Object> retVals = (List<Object>) bins.get(mapBinName);

        Assertions.assertTrue(ASTestUtils.compareCollection(retVals, Arrays.asList("aero", "one", "ten")));

        Map<String, Object> realBins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) realBins.get(mapBinName);
        Assertions.assertFalse(realMapBin.containsKey("aero"));
        Assertions.assertFalse(realMapBin.containsKey("one"));
        Assertions.assertFalse(realMapBin.containsKey("ten"));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapRemoveByIndexRangeNoCount(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);
        operation.put("index", 0);
        operation.put(OperationConverter.MAP_RETURN_KEY, "KEY");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_REMOVE_BY_INDEX_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        List<Object> retVals = (List<Object>) bins.get(mapBinName);
        Assertions.assertTrue(ASTestUtils.compareCollection(retVals, Arrays.asList("aero", "one", "ten", "three", "two")));

        Map<String, Object> realBins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) realBins.get(mapBinName);
        Assertions.assertEquals(realMapBin.size(), 0);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapRemoveByKey(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);
        operation.put("key", "two");
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_REMOVE_BY_KEY);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(bins.get(mapBinName), 2));

        Map<String, Object> realBins = client.get(null, testKey).bins;
        @SuppressWarnings("unchecked") Map<Object, Object> realMapBin = (Map<Object, Object>) realBins.get(mapBinName);
        Assertions.assertFalse(realMapBin.containsKey("two"));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapRemoveByKeyRange(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);
        operation.put(OperationConverter.MAP_KEY_BEGIN_KEY, "one");
        // A value after "three"
        operation.put(OperationConverter.MAP_KEY_END_KEY, "threez");
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_REMOVE_BY_KEY_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        List<Object> retValues = (List<Object>) bins.get(mapBinName);
        Assertions.assertTrue(ASTestUtils.compareCollection(retValues, Arrays.asList(1, 10, 3)));

        Map<String, Object> realBins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) realBins.get(mapBinName);
        Assertions.assertFalse(realMapBin.containsKey("one"));
        Assertions.assertFalse(realMapBin.containsKey("ten"));
        Assertions.assertFalse(realMapBin.containsKey("three"));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapRemoveByKeyRangeNoBegin(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);

        // A value after "three"
        operation.put(OperationConverter.MAP_KEY_END_KEY, "threez");
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_REMOVE_BY_KEY_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        List<Object> retValues = (List<Object>) bins.get(mapBinName);
        Assertions.assertTrue(ASTestUtils.compareCollection(retValues, Arrays.asList("spike", 1, 10, 3)));

        Map<String, Object> realBins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) realBins.get(mapBinName);
        Assertions.assertFalse(realMapBin.containsKey("aero"));
        Assertions.assertFalse(realMapBin.containsKey("one"));
        Assertions.assertFalse(realMapBin.containsKey("ten"));
        Assertions.assertFalse(realMapBin.containsKey("three"));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapRemoveByKeyRangeNoEnd(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);
        operation.put(OperationConverter.MAP_KEY_BEGIN_KEY, "one");
        // A value after "three"
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_REMOVE_BY_KEY_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        List<Object> retValues = (List<Object>) bins.get(mapBinName);
        Assertions.assertTrue(ASTestUtils.compareCollection(retValues, Arrays.asList(1, 10, 3, 2)));

        Map<String, Object> realBins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) realBins.get(mapBinName);
        Assertions.assertFalse(realMapBin.containsKey("one"));
        Assertions.assertFalse(realMapBin.containsKey("ten"));
        Assertions.assertFalse(realMapBin.containsKey("three"));
        Assertions.assertFalse(realMapBin.containsKey("two"));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapRemoveByRank(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put("rank", 2);
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_REMOVE_BY_RANK);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        Assertions.assertTrue(ASTestUtils.compareSimpleValues(bins.get(mapBinNameInt), 2));

        Map<String, Object> realBins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) realBins.get(mapBinNameInt);

        Assertions.assertFalse(realMapBin.containsKey("two"));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapRemoveByRankRange(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put("rank", 1);
        operation.put("count", 3);
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_REMOVE_BY_RANK_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        List<Object> retVals = (List<Object>) bins.get(mapBinNameInt);

        Assertions.assertTrue(ASTestUtils.compareCollection(retVals, Arrays.asList(1, 2, 3)));

        Map<String, Object> realBins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) realBins.get(mapBinNameInt);

        objectMapInt.remove("one");
        objectMapInt.remove("three");
        objectMapInt.remove("two");

        Assertions.assertTrue(ASTestUtils.compareMap(objectMapInt, realMapBin));

        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapRemoveByRankRangeNoCount(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put("rank", 1);
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_REMOVE_BY_RANK_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        List<Object> retVals = (List<Object>) bins.get(mapBinNameInt);

        Assertions.assertTrue(ASTestUtils.compareCollection(retVals, Arrays.asList(1, 2, 3, 10)));

        Map<String, Object> realBins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) realBins.get(mapBinNameInt);

        objectMapInt.remove("one");
        objectMapInt.remove("three");
        objectMapInt.remove("two");
        objectMapInt.remove("ten");
        Assertions.assertTrue(ASTestUtils.compareMap(objectMapInt, realMapBin));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapRemoveByKeyRelIndexRange(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put("value", "one");
        operation.put("index", 1);
        operation.put("count", 2);
        operation.put(OperationConverter.MAP_RETURN_KEY, "KEY");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_REMOVE_BY_KEY_RELATIVE_INDEX_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));
        List<Object> retVals = (List<Object>) bins.get(mapBinNameInt);
        Assertions.assertTrue(ASTestUtils.compareCollection(retVals, Arrays.asList("ten", "three")));

        Map<String, Object> realBins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) realBins.get(mapBinNameInt);
        Assertions.assertFalse(realMapBin.containsKey("ten"));
        Assertions.assertFalse(realMapBin.containsKey("three"));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapRemoveByValueRelRankRange(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put("value", 10);
        operation.put("rank", -1);
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_REMOVE_BY_VALUE_RELATIVE_RANK_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));
        List<Object> retVals = (List<Object>) bins.get(mapBinNameInt);
        Assertions.assertTrue(ASTestUtils.compareCollection(retVals, Arrays.asList(3, 10)));

        Map<String, Object> realBins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) realBins.get(mapBinNameInt);
        objectMapInt.remove("three");
        objectMapInt.remove("ten");
        Assertions.assertTrue(ASTestUtils.compareMap(objectMapInt, realMapBin));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapRemoveByValue(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinName);
        operation.put("value", 3);
        operation.put(OperationConverter.MAP_RETURN_KEY, "KEY");
        /* Store a second item with the value of 3 to show that we get all keys with the provided value*/
        objectMap.put("threez", 3);
        Bin newBin = new Bin(mapBinName, objectMap);
        client.put(null, testKey, newBin);

        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_REMOVE_BY_VALUE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));

        Assertions.assertTrue(
                ASTestUtils.compareCollection((List<Object>) bins.get(mapBinName), Arrays.asList("three", "threez")));

        Map<String, Object> realBins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) realBins.get(mapBinName);

        objectMap.remove("three");
        objectMap.remove("threez");
        Assertions.assertTrue(ASTestUtils.compareMap(objectMap, realMapBin));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapRemoveByValueRange(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put(OperationConverter.VALUE_BEGIN_KEY, 1);
        operation.put(OperationConverter.VALUE_END_KEY, 4);
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");

        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_REMOVE_BY_VALUE_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));
        /* These keys come back in key sorted order, so "one" < "three" < "two" */
        Assertions.assertTrue(ASTestUtils.containSameItems((List<Object>) bins.get(mapBinNameInt), Arrays.asList(1, 3, 2)));

        Map<String, Object> realBins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) realBins.get(mapBinNameInt);

        objectMapInt.remove("one");
        objectMapInt.remove("two");
        objectMapInt.remove("three");
        Assertions.assertTrue(ASTestUtils.compareMap(objectMapInt, realMapBin));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapRemoveByValueRangeNoBegin(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put(OperationConverter.VALUE_END_KEY, 4);
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");

        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_REMOVE_BY_VALUE_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));
        /* These keys come back in key sorted order, so "one" < "three" < "two" < "zero"*/
        Assertions.assertTrue(
                ASTestUtils.containSameItems((List<Object>) bins.get(mapBinNameInt), Arrays.asList(1, 3, 2, 0)));

        Map<String, Object> realBins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) realBins.get(mapBinNameInt);

        objectMapInt.remove("zero");
        objectMapInt.remove("one");
        objectMapInt.remove("two");
        objectMapInt.remove("three");
        Assertions.assertTrue(ASTestUtils.compareMap(objectMapInt, realMapBin));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapRemoveByValueRangeNoEnd(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put(OperationConverter.VALUE_BEGIN_KEY, 1);
        operation.put(OperationConverter.MAP_RETURN_KEY, "VALUE");

        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_REMOVE_BY_VALUE_RANGE);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));
        /* These keys come back in key sorted order, so "one" < "ten", "three" < "two" */
        Assertions.assertTrue(
                ASTestUtils.containSameItems((List<Object>) bins.get(mapBinNameInt), Arrays.asList(1, 10, 3, 2)));

        Map<String, Object> realBins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) realBins.get(mapBinNameInt);

        objectMapInt.remove("one");
        objectMapInt.remove("two");
        objectMapInt.remove("three");
        objectMapInt.remove("ten");
        Assertions.assertTrue(ASTestUtils.compareMap(objectMapInt, realMapBin));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapRemoveByValueList(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> operation = new HashMap<>();

        operation.put("binName", mapBinNameInt);
        operation.put("values", Arrays.asList(1, 2, 3));
        operation.put(OperationConverter.MAP_RETURN_KEY, "KEY");

        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_REMOVE_BY_VALUE_LIST);

        opList.add(operation);

        Map<String, Object> bins = getReturnedBins(
                opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));
        Assertions.assertTrue(ASTestUtils.containSameItems((List<Object>) bins.get(mapBinNameInt),
                Arrays.asList("one", "three", "two")));

        Map<String, Object> realBins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) realBins.get(mapBinNameInt);

        objectMapInt.remove("one");
        objectMapInt.remove("two");
        objectMapInt.remove("three");
        Assertions.assertTrue(ASTestUtils.compareMap(objectMapInt, realMapBin));
        } finally {
            client.delete(null, testKey);
        }
    }

    /*
     * Test that a create_only map write flag prevents updating an existing value
     */
    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapWriteFlagsCreateOnly(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> policyMap = getMapPolicyMap(MapOrder.UNORDERED, MapWriteFlags.CREATE_ONLY);
        Map<String, Object> operation = new HashMap<>();

        String newKey = "new_key";
        String newVal = "new_value";

        operation.put("binName", mapBinName);
        operation.put("key", newKey);
        operation.put("value", newVal);
        operation.put(OperationConverter.MAP_POLICY_KEY, policyMap);
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_PUT);

        opList.add(operation);

        /* This should succeed because we are doing a create only on a new value */
        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
        Map<String, Object> bins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) bins.get(mapBinName);

        Assertions.assertEquals(newVal, realMapBin.get(newKey));

        opPerformer.performOperationsAndExpect(mockMVC, testEndpoint, opRequest, status().isInternalServerError());
        } finally {
            client.delete(null, testKey);
        }
    }

    /*
     * Test that an update only map operation cannot add a field to the map.
     */
    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapWriteFlagsUpdateOnlyError(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> policyMap = getMapPolicyMap(MapOrder.UNORDERED, MapWriteFlags.UPDATE_ONLY);
        Map<String, Object> operation = new HashMap<>();

        String newKey = "new_key";
        String newVal = "new_value";

        operation.put("binName", mapBinName);
        operation.put("key", newKey);
        operation.put("value", newVal);
        operation.put(OperationConverter.MAP_POLICY_KEY, policyMap);
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_PUT);

        opList.add(operation);
        opPerformer.performOperationsAndExpect(mockMVC, testEndpoint, opRequest, status().isInternalServerError());
        Map<String, Object> bins = client.get(null, testKey).bins;
        Map<Object, Object> realMapBin = (Map<Object, Object>) bins.get(mapBinName);
        Assertions.assertNull(realMapBin.get(newKey));
        } finally {
            client.delete(null, testKey);
        }
    }

    /*
     * Test that a map put operation with update only can update an existing item.
     */
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapWriteFlagsUpdateOnlyNoError(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> policyMap = getMapPolicyMap(MapOrder.UNORDERED, MapWriteFlags.UPDATE_ONLY);
        Map<String, Object> operation = new HashMap<>();

        String existingKey = "aero";
        String newVal = "new_value";

        operation.put("binName", mapBinName);
        operation.put("key", existingKey);
        operation.put("value", newVal);
        operation.put(OperationConverter.MAP_POLICY_KEY, policyMap);
        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_PUT);

        opList.add(operation);
        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
        Map<String, Object> bins = client.get(null, testKey).bins;
        @SuppressWarnings("unchecked") Map<Object, Object> realMapBin = (Map<Object, Object>) bins.get(mapBinName);

        Assertions.assertEquals(newVal, realMapBin.get(existingKey));
        } finally {
            client.delete(null, testKey);
        }
    }

    /*
     * The map currently has {"aero":"spike"}
     * we attempt to do an update with {"aero"=>"new_val", "new_key"=>"new_val"}
     * along with the update_only partial no_fail flags
     * We expect the operation to not raise an error
     * and the resulting map to not have new_key, and to contain {"aero"=>"new_val"}
     */
    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapWriteFlagsUpdateOnlyPartialNoFail(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin mapBin = new Bin(mapBinName, objectMap);
            Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> policyMap = getMapPolicyMap(MapOrder.UNORDERED,
                MapWriteFlags.UPDATE_ONLY | MapWriteFlags.PARTIAL | MapWriteFlags.NO_FAIL);
        Map<String, Object> operation = new HashMap<>();

        String existingKey = "aero";
        String newKey = "new_key";
        String newVal = "new_value";

        operation.put("binName", mapBinName);
        operation.put(OperationConverter.MAP_POLICY_KEY, policyMap);

        Map<Object, Object> putValues = new HashMap<>();
        putValues.put(existingKey, newVal);
        putValues.put(newKey, newVal);
        operation.put("map", putValues);

        operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_PUT_ITEMS);

        opList.add(operation);
        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
        Map<String, Object> bins = client.get(null, testKey).bins;
        @SuppressWarnings("unchecked") Map<Object, Object> realMapBin = (Map<Object, Object>) bins.get(mapBinName);

        Assertions.assertEquals(newVal, realMapBin.get(existingKey));
        Assertions.assertNull(realMapBin.get(newKey));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapSize(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            // Bin mapBin = new Bin(mapBinName, objectMap);
            // Bin mapBinInt = new Bin(mapBinNameInt, objectMapInt);
            // client.put(null, testKey, mapBin, mapBinInt);
            client.operate(null, testKey,
                    Operation.put(new Bin(mapBinName, objectMap)),
                    Operation.put(new Bin(mapBinNameInt, objectMapInt)),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinName),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), mapBinNameInt));
            
            opList = new ArrayList<>();
            opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
            
            Map<String, Object> operation = new HashMap<>();
           
            operation.put("binName", mapBinName);
            operation.put(OPERATION_FIELD_TYPE, OperationTypes.MAP_SIZE);
            opList.add(operation);

            Map<String, Object> bins = getReturnedBins(
                    opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest));
            
            Assertions.assertTrue(ASTestUtils.compareSimpleValues(bins.get(mapBinName), objectMap.size()));
        } finally {
            client.delete(null, testKey);
        }
    }

    private Map<String, Object> getMapPolicyMap(MapOrder order, int flags) {
        Map<String, Object> policyMap = new HashMap<>();
        String orderString;
        List<String> writeFlags = new ArrayList<>();
        switch (order) {
            case KEY_VALUE_ORDERED:
                orderString = "KEY_VALUE_ORDERED";
                break;
            case KEY_ORDERED:
                orderString = "KEY_ORDERED";
                break;
            case UNORDERED:
            default:
                orderString = "UNORDERED";
        }

        if ((flags & MapWriteFlags.CREATE_ONLY) != 0) {
            writeFlags.add("CREATE_ONLY");
        }

        if ((flags & MapWriteFlags.UPDATE_ONLY) != 0) {
            writeFlags.add("UPDATE_ONLY");
        }

        if ((flags & MapWriteFlags.PARTIAL) != 0) {
            writeFlags.add("PARTIAL");
        }

        if ((flags & MapWriteFlags.NO_FAIL) != 0) {
            writeFlags.add("NO_FAIL");
        }

        policyMap.put("order", orderString);
        policyMap.put(OperationConverter.WRITE_FLAGS_KEY, writeFlags);
        return policyMap;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getReturnedBins(Map<String, Object> resp) {
        Map<String, Object> record = (Map<String, Object>) resp.get("record");
        return (Map<String, Object>) record.get("bins");
    }
}

