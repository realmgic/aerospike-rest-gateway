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
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.operation.HLLOperation;
import com.aerospike.client.operation.HLLPolicy;
import com.aerospike.restclient.util.AerospikeOperation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.*;
import java.util.stream.Stream;

import static com.aerospike.restclient.util.AerospikeAPIConstants.OPERATION_FIELD;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static com.aerospike.restclient.util.AerospikeAPIConstants.OPERATION_VALUES_FIELD;

@SpringBootTest
public class OperateV1HLLCorrectTests {

    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    private static final List<Object> values = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "g");
    private static final List<Object> values2 = Arrays.asList("a", "b", "c", "d", "e", "h", "i", "j");

    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JSONOperationV1Performer(), true),
                Arguments.of(new MsgPackOperationV1Performer(), true),
                Arguments.of(new JSONOperationV1Performer(), false),
                Arguments.of(new MsgPackOperationV1Performer(), false)
        );
    }

    private static Key keyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", "hllop") : new Key("test", null, "hllop");
    }

    private static String endpointFor(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("operate", "test", "junit", "hllop") : ASTestUtils.buildEndpointV1("operate", "test", "hllop");
    }

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    private void createHLLBin(Key testKey, String testEndpoint, OperationV1Performer opPerformer, String binName, List<Object> vals) {
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", binName);
        opValues.put("values", vals);
        opMap.put(OPERATION_FIELD, AerospikeOperation.HLL_ADD);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);
        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLAdd(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
            createHLLBin(testKey, testEndpoint, opPerformer, "hll", values);

            Record record = client.operate(null, testKey, HLLOperation.getCount("hll"));
            long count = (long) record.bins.get("hll");

            Assertions.assertEquals(7, count);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLSetUnion(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
            createHLLBin(testKey, testEndpoint, opPerformer, "hll", values);
            createHLLBin(testKey, testEndpoint, opPerformer, "hll2", values2);

        Value.HLLValue hll2Bin = (Value.HLLValue) client.get(null, testKey).bins.get("hll2");

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "hll");
        opValues.put("values", Collections.singletonList(Base64.getEncoder().encodeToString(hll2Bin.getBytes())));
        opMap.put(OPERATION_FIELD, AerospikeOperation.HLL_SET_UNION);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        Record record = client.operate(null, testKey, HLLOperation.getCount("hll"));
        long count = (long) record.bins.get("hll");

        Assertions.assertEquals(10, count);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLRefreshCount(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
            createHLLBin(testKey, testEndpoint, opPerformer, "hll", values);

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "hll");
        opMap.put(OPERATION_FIELD, AerospikeOperation.HLL_SET_COUNT);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        Record record = client.operate(null, testKey, HLLOperation.getCount("hll"));
        long count = (long) record.bins.get("hll");

        Assertions.assertEquals(7, count);
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLGetUnion(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
            createHLLBin(testKey, testEndpoint, opPerformer, "hll", values);
            createHLLBin(testKey, testEndpoint, opPerformer, "hll2", values2);

        Value.HLLValue hll2Bin = (Value.HLLValue) client.get(null, testKey).bins.get("hll2");

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "hll");
        opValues.put("values", Collections.singletonList(Base64.getEncoder().encodeToString(hll2Bin.getBytes())));
        opMap.put(OPERATION_FIELD, AerospikeOperation.HLL_UNION);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);
        byte[] expected;
        try {
            expected = Base64.getDecoder()
                    .decode(((Map<String, Map<String, String>>) res.get("bins")).get("hll").get("object").getBytes());
        } catch (ClassCastException e) {
            expected = (byte[]) ((Map<String, Map<String, Object>>) res.get("bins")).get("hll").get("object");
        }
        Value.HLLValue value = new Value.HLLValue(expected);
        Assertions.assertNotNull(value);
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLGetUnionCount(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
            createHLLBin(testKey, testEndpoint, opPerformer, "hll", values);
            createHLLBin(testKey, testEndpoint, opPerformer, "hll2", values2);

        Value.HLLValue hll2Bin = (Value.HLLValue) client.get(null, testKey).bins.get("hll2");

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "hll");
        opValues.put("values", Collections.singletonList(Base64.getEncoder().encodeToString(hll2Bin.getBytes())));
        opMap.put(OPERATION_FIELD, AerospikeOperation.HLL_UNION_COUNT);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);
        Integer count = ((Map<String, Integer>) res.get("bins")).get("hll");
        Assertions.assertEquals(10, count.intValue());
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLGetIntersectCount(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
            createHLLBin(testKey, testEndpoint, opPerformer, "hll", values);
            createHLLBin(testKey, testEndpoint, opPerformer, "hll2", values2);

        Value.HLLValue hll2Bin = (Value.HLLValue) client.get(null, testKey).bins.get("hll2");

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "hll");
        opValues.put("values", Collections.singletonList(Base64.getEncoder().encodeToString(hll2Bin.getBytes())));
        opMap.put(OPERATION_FIELD, AerospikeOperation.HLL_INTERSECT_COUNT);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);
        Integer count = ((Map<String, Integer>) res.get("bins")).get("hll");
        Assertions.assertEquals(5, count.intValue());
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLGetSimilarity(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
            createHLLBin(testKey, testEndpoint, opPerformer, "hll", values);
            createHLLBin(testKey, testEndpoint, opPerformer, "hll2", values2);

        Value.HLLValue hll2Bin = (Value.HLLValue) client.get(null, testKey).bins.get("hll2");

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "hll");
        opValues.put("values", Collections.singletonList(Base64.getEncoder().encodeToString(hll2Bin.getBytes())));
        opMap.put(OPERATION_FIELD, AerospikeOperation.HLL_SIMILARITY);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);
        Double count = ((Map<String, Double>) res.get("bins")).get("hll");
        Assertions.assertEquals(0.5, count, 0.005);
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLDescribe(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
            createHLLBin(testKey, testEndpoint, opPerformer, "hll", values);

        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();

        opValues.put("bin", "hll");
        opMap.put(OPERATION_FIELD, AerospikeOperation.HLL_DESCRIBE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);
        List<Integer> describeValues = ((Map<String, List<Integer>>) res.get("bins")).get("hll");
        Assertions.assertEquals(Arrays.asList(8, 8), describeValues);
        } finally {
            client.delete(null, testKey);
        }
    }

}

