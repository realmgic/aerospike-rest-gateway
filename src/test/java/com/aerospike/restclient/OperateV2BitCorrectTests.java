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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class OperateV2BitCorrectTests {

    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    static final byte[] byteArray = new byte[]{12, 5, 110, 47};
    private static final String OPERATION_TYPE_KEY = "type";

    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JSONOperationV2Performer(), true),
                Arguments.of(new MsgPackOperationV2Performer(), true),
                Arguments.of(new JSONOperationV2Performer(), false),
                Arguments.of(new MsgPackOperationV2Performer(), false)
        );
    }

    private static Key keyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", "bitop") : new Key("test", null, "bitop");
    }

    private static String endpointFor(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV2("operate", "test", "junit", "bitop") : ASTestUtils.buildEndpointV2("operate", "test", "bitop");
    }

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitResize(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("byteSize", 8);
        opMap.put("resizeFlags", 0);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_RESIZE);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        Assertions.assertArrayEquals(new byte[]{12, 5, 110, 47, 0, 0, 0, 0}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitInsert(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("byteOffset", 1);
        opMap.put("value", new byte[]{11});
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_INSERT);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        Assertions.assertArrayEquals(new byte[]{12, 11, 5, 110, 47}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitRemove(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("byteOffset", 1);
        opMap.put("byteSize", 2);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_REMOVE);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        Assertions.assertArrayEquals(new byte[]{12, 47}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitSet(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("bitOffset", 16);
        opMap.put("value", new byte[]{127});
        opMap.put("bitSize", 4);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_SET);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        Assertions.assertArrayEquals(new byte[]{12, 5, 126, 47}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitOr(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("bitOffset", 16);
        opMap.put("value", new byte[]{57});
        opMap.put("bitSize", 8);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_OR);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        Assertions.assertArrayEquals(new byte[]{12, 5, 127, 47}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitXor(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("bitOffset", 16);
        opMap.put("value", new byte[]{57});
        opMap.put("bitSize", 8);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_XOR);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        Assertions.assertArrayEquals(new byte[]{12, 5, 87, 47}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitAnd(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("bitOffset", 16);
        opMap.put("value", new byte[]{57});
        opMap.put("bitSize", 8);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_AND);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        Assertions.assertArrayEquals(new byte[]{12, 5, 40, 47}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitNot(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("bitOffset", 16);
        opMap.put("bitSize", 16);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_NOT);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        Assertions.assertArrayEquals(new byte[]{12, 5, -111, -48}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitLshift(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("bitOffset", 24);
        opMap.put("bitSize", 8);
        opMap.put("shift", 3);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_LSHIFT);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        Assertions.assertArrayEquals(new byte[]{12, 5, 110, 120}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitRshift(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("bitOffset", 24);
        opMap.put("bitSize", 8);
        opMap.put("shift", 3);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_RSHIFT);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        Assertions.assertArrayEquals(new byte[]{12, 5, 110, 5}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitAdd(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("bitOffset", 24);
        opMap.put("bitSize", 8);
        opMap.put("value", 3);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_ADD);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        Assertions.assertArrayEquals(new byte[]{12, 5, 110, 50}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitSubtract(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("bitOffset", 24);
        opMap.put("bitSize", 8);
        opMap.put("value", 3);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_SUBTRACT);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        Assertions.assertArrayEquals(new byte[]{12, 5, 110, 44}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitSetInt(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("bitOffset", 24);
        opMap.put("bitSize", 8);
        opMap.put("value", 3);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_SET_INT);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        Assertions.assertArrayEquals(new byte[]{12, 5, 110, 3}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    @SuppressWarnings("unchecked")
    public void testBitGet(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("bitOffset", 24);
        opMap.put("bitSize", 8);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_GET);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
        Map<String, Object> record = (Map<String, Object>) res.get("record");
        byte[] expected;
        try {
            expected = Base64.getDecoder().decode(((Map<String, String>) record.get("bins")).get("bit").getBytes());
        } catch (ClassCastException e) {
            expected = (byte[]) ((Map<String, Object>) record.get("bins")).get("bit");
        }

        Assertions.assertArrayEquals(new byte[]{47}, expected);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    @SuppressWarnings("unchecked")
    public void testBitCount(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("bitOffset", 20);
        opMap.put("bitSize", 4);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_COUNT);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
        Map<String, Object> record = (Map<String, Object>) res.get("record");
        int expected = ((Map<String, Integer>) record.get("bins")).get("bit");

        Assertions.assertEquals(3, expected);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    @SuppressWarnings("unchecked")
    public void testBitLscan(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("bitOffset", 16);
        opMap.put("bitSize", 8);
        opMap.put("value", true);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_LSCAN);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
        Map<String, Object> record = (Map<String, Object>) res.get("record");
        int expected = ((Map<String, Integer>) record.get("bins")).get("bit");

        Assertions.assertEquals(1, expected);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    @SuppressWarnings("unchecked")
    public void testBitRscan(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("bitOffset", 16);
        opMap.put("bitSize", 8);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_RSCAN);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
        Map<String, Object> record = (Map<String, Object>) res.get("record");
        int expected = ((Map<String, Integer>) record.get("bins")).get("bit");

        Assertions.assertEquals(7, expected);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    @SuppressWarnings("unchecked")
    public void testBitGetInt(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opRequest = new HashMap<>();
            opRequest.put("opsList", opList);
        Map<String, Object> opMap = new HashMap<>();
        opMap.put("binName", "bit");
        opMap.put("bitOffset", 12);
        opMap.put("bitSize", 16);
        opMap.put(OPERATION_TYPE_KEY, AerospikeOperation.BIT_GET_INT);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
        Map<String, Object> record = (Map<String, Object>) res.get("record");
        int expected = ((Map<String, Integer>) record.get("bins")).get("bit");

        Assertions.assertEquals(22242, expected);
        } finally {
            client.delete(null, testKey);
        }
    }

}

