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

import com.aerospike.client.Record;
import com.aerospike.client.*;
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

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
public class OperateV2HLLCorrectTests {

    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    private static final String OPERATION_TYPE_KEY = "type";
    private static final List<Object> values = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "g");
    private static final List<Object> values2 = Arrays.asList("a", "b", "c", "d", "e", "h", "i", "j");

    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JSONOperationV2Performer(), true),
                Arguments.of(new MsgPackOperationV2Performer(), true),
                Arguments.of(new JSONOperationV2Performer(), false),
                Arguments.of(new MsgPackOperationV2Performer(), false)
        );
    }

    private static Key keyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", "hllop") : new Key("test", null, "hllop");
    }

    private static String endpointFor(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV2("operate", "test", "junit", "hllop") : ASTestUtils.buildEndpointV2("operate", "test", "hllop");
    }

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    private void createHLLBin(Key testKey, String testEndpoint, OperationV2Performer opPerformer, String binName, List<Object> vals) {
        Map<String, Object> opRequest = new HashMap<>();
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", binName);
        opMap.put("values", vals);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.HLL_ADD);
        opList.add(opMap);
        opRequest.put("opsList", opList);
        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLAdd(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
            createHLLBin(testKey, testEndpoint, opPerformer, "hll", values);

            Record record = client.operate(null, testKey, HLLOperation.getCount("hll"));
            long count = (long) record.bins.get("hll");

            assertEquals(7, count);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLAddWithIndexBitCount(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
        client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
        client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opRequest = new HashMap<>();
        opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "hll");
        opMap.put("values", values);
        opMap.put("indexBitCount", 4);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.HLL_ADD);
        opList.add(opMap);

        opPerformer.performOperationsAndExpect(mockMVC, testEndpoint, opRequest, status().isOk());

        Record record = client.operate(null, testKey, HLLOperation.getCount("hll"));
        long count = (long) record.bins.get("hll");

        Assertions.assertEquals(7, count);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLAddWithMinHashBitCount(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
        client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
        client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opRequest = new HashMap<>();
        opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "hll");
        opMap.put("values", values);
        opMap.put("indexBitCount", 4);
        opMap.put("minHashBitCount", 16);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.HLL_ADD);
        opList.add(opMap);

        opPerformer.performOperationsAndExpect(mockMVC, testEndpoint, opRequest, status().isOk());

        Record record = client.operate(null, testKey, HLLOperation.getCount("hll"));
        long count = (long) record.bins.get("hll");

        Assertions.assertEquals(7, count);
        } finally {
            client.delete(null, testKey);
        }
    }

    private boolean isWithinRelativeError(long expected, long estimate, double relativeError) {
        return expected * (1 - relativeError) <= estimate || estimate <= expected * (1 + relativeError);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLFold(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
        client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
        client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opRequest = new HashMap<>();
        opRequest.put("opsList", opList);
        List<Value> vals0 = new ArrayList<>();
        List<Value> vals1 = new ArrayList<>();
        int nEntries = 1 << 18;
        String binName = "hll";

        for (int i = 0; i < nEntries / 2; i++) {
            vals0.add(new Value.StringValue("key " + i));
        }

        for (int i = nEntries / 2; i < nEntries; i++) {
            vals1.add(new Value.StringValue("key " + i));
        }

        client.operate(null, testKey, Operation.delete(), HLLOperation.add(HLLPolicy.Default, binName, vals0, 4),
                HLLOperation.getCount(binName), HLLOperation.refreshCount(binName));

        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", binName);
        opMap.put("indexBitCount", 4);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.HLL_FOLD);
        opList.add(opMap);

        opPerformer.performOperationsAndExpect(mockMVC, testEndpoint, opRequest, status().isOk());

        Record record = client.operate(null, testKey, HLLOperation.fold(binName, 4), HLLOperation.getCount(binName),
                HLLOperation.add(HLLPolicy.Default, binName, vals0),
                HLLOperation.add(HLLPolicy.Default, binName, vals1), HLLOperation.getCount(binName));

        List<?> result = record.getList(binName);

        long countb = (Long) result.get(1);
        long countb1 = (Long) result.get(4);
        double countErr = (1.04 / Math.sqrt(Math.pow(2, 4))) * 6;

        Assertions.assertTrue(isWithinRelativeError(vals0.size(), countb, countErr));
        Assertions.assertTrue(isWithinRelativeError(vals0.size() + vals1.size(), countb1, countErr));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    @SuppressWarnings("unchecked")
    public void testHLLGetCount(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
            createHLLBin(testKey, testEndpoint, opPerformer, "hll", values);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);

        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "hll");
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.HLL_COUNT);
        opList.add(opMap);

        Map<String, Object> resp = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
        Map<String, Object> record = (Map<String, Object>) resp.get("record");
        Map<String, Object> bins = (Map<String, Object>) record.get("bins");
        int count = (int) bins.get("hll");

        Assertions.assertEquals(7, count);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLSetUnion(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
            createHLLBin(testKey, testEndpoint, opPerformer, "hll", values);
            createHLLBin(testKey, testEndpoint, opPerformer, "hll2", values2);

        Value.HLLValue hll2Bin = (Value.HLLValue) client.get(null, testKey).bins.get("hll2");

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "hll");
        opMap.put("values", Collections.singletonList(Base64.getEncoder().encodeToString(hll2Bin.getBytes())));
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.HLL_SET_UNION);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        Record record = client.operate(null, testKey, HLLOperation.getCount("hll"));
        long count = (long) record.bins.get("hll");

        Assertions.assertEquals(10, count);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLRefreshCount(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
            createHLLBin(testKey, testEndpoint, opPerformer, "hll", values);

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "hll");
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.HLL_SET_COUNT);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

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
    public void testHLLGetUnion(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
            createHLLBin(testKey, testEndpoint, opPerformer, "hll", values);
            createHLLBin(testKey, testEndpoint, opPerformer, "hll2", values2);

        Value.HLLValue hll2Bin = (Value.HLLValue) client.get(null, testKey).bins.get("hll2");

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "hll");
        opMap.put("values", Collections.singletonList(Base64.getEncoder().encodeToString(hll2Bin.getBytes())));
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.HLL_UNION);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
        Map<String, Object> record = (Map<String, Object>) res.get("record");
        byte[] expected;
        try {
            expected = Base64.getDecoder()
                    .decode(((Map<String, Map<String, String>>) record.get("bins")).get("hll")
                            .get("object")
                            .getBytes());
        } catch (ClassCastException e) {
            expected = (byte[]) ((Map<String, Map<String, Object>>) record.get("bins")).get("hll").get("object");
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
    public void testHLLGetUnionCount(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
            createHLLBin(testKey, testEndpoint, opPerformer, "hll", values);
            createHLLBin(testKey, testEndpoint, opPerformer, "hll2", values2);

        Value.HLLValue hll2Bin = (Value.HLLValue) client.get(null, testKey).bins.get("hll2");

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "hll");
        opMap.put("values", Collections.singletonList(Base64.getEncoder().encodeToString(hll2Bin.getBytes())));
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.HLL_UNION_COUNT);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
        Map<String, Object> record = (Map<String, Object>) res.get("record");
        Integer count = ((Map<String, Integer>) record.get("bins")).get("hll");
        Assertions.assertEquals(10, count.intValue());
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLInit(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        int indexBits = 16;
        int minHashBits = 16;

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "hll");
        opMap.put("indexBitCount", indexBits);
        opMap.put("minHashBitCount", minHashBits);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.HLL_INIT);
        opList.add(opMap);

        opPerformer.performOperationsAndExpect(mockMVC, testEndpoint, opRequest, status().isOk());

        Record record = client.operate(null, testKey, HLLOperation.describe("hll"));
        List<?> description = record.getList("hll");

        Assertions.assertEquals(16L, description.get(0));
        Assertions.assertEquals(16L, description.get(1));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLInitWithNullMinHashBits(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        
            int indexBits = 16;

            Map<String, Object> opMap = new HashMap<>();

            opMap.put("binName", "hll");
            opMap.put("indexBitCount", indexBits);
            opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.HLL_INIT);
            opList.add(opMap);

            System.out.println("opRequest: " + opRequest);
            System.out.println("opList: " + opList);
            System.out.println("opMap: " + opMap);

            opPerformer.performOperationsAndExpect(mockMVC, testEndpoint, opRequest, status().isOk());

            Record record = client.operate(null, testKey, HLLOperation.describe("hll"));
            List<?> description = record.getList("hll");
            
            System.out.println("record: " + record);
            System.out.println("description: " + description);

            Assertions.assertEquals(16L, description.get(0));
            Assertions.assertEquals(0L, description.get(1));
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLGetIntersectCount(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
            createHLLBin(testKey, testEndpoint, opPerformer, "hll", values);
            createHLLBin(testKey, testEndpoint, opPerformer, "hll2", values2);

        Value.HLLValue hll2Bin = (Value.HLLValue) client.get(null, testKey).bins.get("hll2");

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "hll");
        opMap.put("values", Collections.singletonList(Base64.getEncoder().encodeToString(hll2Bin.getBytes())));
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.HLL_INTERSECT_COUNT);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
        Map<String, Object> record = (Map<String, Object>) res.get("record");
        Integer count = ((Map<String, Integer>) record.get("bins")).get("hll");
        Assertions.assertEquals(5, count.intValue());
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLGetSimilarity(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
            createHLLBin(testKey, testEndpoint, opPerformer, "hll", values);
            createHLLBin(testKey, testEndpoint, opPerformer, "hll2", values2);

        Value.HLLValue hll2Bin = (Value.HLLValue) client.get(null, testKey).bins.get("hll2");

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "hll");
        opMap.put("values", Collections.singletonList(Base64.getEncoder().encodeToString(hll2Bin.getBytes())));
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.HLL_SIMILARITY);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
        Map<String, Object> record = (Map<String, Object>) res.get("record");
        Double count = ((Map<String, Double>) record.get("bins")).get("hll");
        Assertions.assertEquals(0.5, count, 0.005);
        } finally {
            client.delete(null, testKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testHLLDescribe(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll", 8, 8));
            client.operate(null, testKey, HLLOperation.init(HLLPolicy.Default, "hll2", 8, 8));
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
            createHLLBin(testKey, testEndpoint, opPerformer, "hll", values);

        Map<String, Object> opMap = new HashMap<>();

        opMap.put("binName", "hll");
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.HLL_DESCRIBE);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
        Map<String, Object> record = (Map<String, Object>) res.get("record");
        List<Integer> values = ((Map<String, List<Integer>>) record.get("bins")).get("hll");
        Assertions.assertEquals(Arrays.asList(8, 8), values);
        } finally {
            client.delete(null, testKey);
        }
    }

}
