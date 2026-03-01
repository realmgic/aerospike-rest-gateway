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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.*;
import java.util.stream.Stream;

import static com.aerospike.restclient.util.AerospikeAPIConstants.OPERATION_FIELD;
import static com.aerospike.restclient.util.AerospikeAPIConstants.OPERATION_VALUES_FIELD;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@SpringBootTest
public class OperateV1BitCorrectTests {

    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    static final byte[] byteArray = new byte[]{12, 5, 110, 47};

    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JSONOperationV1Performer(), true),
                Arguments.of(new MsgPackOperationV1Performer(), true),
                Arguments.of(new JSONOperationV1Performer(), false),
                Arguments.of(new MsgPackOperationV1Performer(), false)
        );
    }

    private static Key keyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", "bitop") : new Key("test", null, "bitop");
    }

    private static String endpointFor(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("operate", "test", "junit", "bitop") : ASTestUtils.buildEndpointV1("operate", "test", "bitop");
    }

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitResize(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("byteSize", 8);
        opValues.put("resizeFlags", 0);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_RESIZE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        assertArrayEquals(new byte[]{12, 5, 110, 47, 0, 0, 0, 0}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitInsert(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("byteOffset", 1);
        String value = Base64.getEncoder().encodeToString(new byte[]{11});
        opValues.put("value", value);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_INSERT);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{12, 11, 5, 110, 47}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitRemove(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("byteOffset", 1);
        opValues.put("byteSize", 2);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_REMOVE);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{12, 47}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitSet(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("bitOffset", 16);
        String value = Base64.getEncoder().encodeToString(new byte[]{127});
        opValues.put("value", value);
        opValues.put("bitSize", 4);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_SET);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{12, 5, 126, 47}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitOr(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("bitOffset", 16);
        String value = Base64.getEncoder().encodeToString(new byte[]{57});
        opValues.put("value", value);
        opValues.put("bitSize", 8);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_OR);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{12, 5, 127, 47}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitXor(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("bitOffset", 16);
        String value = Base64.getEncoder().encodeToString(new byte[]{57});
        opValues.put("value", value);
        opValues.put("bitSize", 8);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_XOR);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{12, 5, 87, 47}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitAnd(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("bitOffset", 16);
        String value = Base64.getEncoder().encodeToString(new byte[]{57});
        opValues.put("value", value);
        opValues.put("bitSize", 8);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_AND);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{12, 5, 40, 47}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitNot(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("bitOffset", 16);
        opValues.put("bitSize", 16);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_NOT);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{12, 5, -111, -48}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitLshift(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("bitOffset", 24);
        opValues.put("bitSize", 8);
        opValues.put("shift", 3);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_LSHIFT);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{12, 5, 110, 120}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitRshift(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("bitOffset", 24);
        opValues.put("bitSize", 8);
        opValues.put("shift", 3);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_RSHIFT);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{12, 5, 110, 5}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitAdd(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("bitOffset", 24);
        opValues.put("bitSize", 8);
        opValues.put("value", 3);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_ADD);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{12, 5, 110, 50}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitSubtract(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("bitOffset", 24);
        opValues.put("bitSize", 8);
        opValues.put("value", 3);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_SUBTRACT);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{12, 5, 110, 44}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testBitSetInt(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("bitOffset", 24);
        opValues.put("bitSize", 8);
        opValues.put("value", 3);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_SET_INT);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);

        byte[] realByteArray = (byte[]) client.get(null, testKey).bins.get("bit");

        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{12, 5, 110, 3}, realByteArray);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    @SuppressWarnings("unchecked")
    public void testBitGet(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("bitOffset", 24);
        opValues.put("bitSize", 8);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_GET);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);
        byte[] expected;
        try {
            expected = Base64.getDecoder().decode(((Map<String, String>) res.get("bins")).get("bit").getBytes());
        } catch (ClassCastException e) {
            expected = (byte[]) ((Map<String, Object>) res.get("bins")).get("bit");
        }

        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{47}, expected);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    @SuppressWarnings("unchecked")
    public void testBitCount(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("bitOffset", 20);
        opValues.put("bitSize", 4);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_COUNT);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);
        int expected = ((Map<String, Integer>) res.get("bins")).get("bit");

        org.junit.jupiter.api.Assertions.assertEquals(3, expected);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    @SuppressWarnings("unchecked")
    public void testBitLscan(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("bitOffset", 16);
        opValues.put("bitSize", 8);
        opValues.put("value", true);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_LSCAN);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);
        int expected = ((Map<String, Integer>) res.get("bins")).get("bit");

        org.junit.jupiter.api.Assertions.assertEquals(1, expected);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    @SuppressWarnings("unchecked")
    public void testBitRscan(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("bitOffset", 16);
        opValues.put("bitSize", 8);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_RSCAN);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);
        int expected = ((Map<String, Integer>) res.get("bins")).get("bit");

        org.junit.jupiter.api.Assertions.assertEquals(7, expected);
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    @SuppressWarnings("unchecked")
    public void testBitGetInt(OperationV1Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        String testEndpoint = endpointFor(useSet);
        try {
            Bin bitBin = new Bin("bit", byteArray);
            client.put(null, testKey, bitBin);
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        Map<String, Object> opValues = new HashMap<>();
        opValues.put("bin", "bit");
        opValues.put("bitOffset", 12);
        opValues.put("bitSize", 16);
        opMap.put(OPERATION_FIELD, AerospikeOperation.BIT_GET_INT);
        opMap.put(OPERATION_VALUES_FIELD, opValues);
        opList.add(opMap);

        Map<String, Object> res = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opList);
        int expected = ((Map<String, Integer>) res.get("bins")).get("bit");

        org.junit.jupiter.api.Assertions.assertEquals(22242, expected);
        } finally {
            client.delete(null, testKey);
        }
    }

}
