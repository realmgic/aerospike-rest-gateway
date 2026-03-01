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
import com.aerospike.client.Record;
import com.aerospike.restclient.domain.operationmodels.OperationTypes;
import com.aerospike.restclient.util.AerospikeAPIConstants;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
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

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
public class OperateV2CorrectTest {

    private static final byte[] KEY_BYTES = {1, 127, 127, 1};
    String OPERATION_TYPE_KEY = "type";

    @Autowired
    private ObjectMapper objectMapper;

    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    public static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JSONOperationV2Performer(), true),
                Arguments.of(new MsgPackOperationV2Performer(), true),
                Arguments.of(new JSONOperationV2Performer(), false),
                Arguments.of(new MsgPackOperationV2Performer(), false)
        );
    }

    private static Key keyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", "operate") : new Key("test", null, "operate");
    }

    private static Key testKey2For(boolean useSet) {
        return useSet ? new Key("test", "junit", "operate2") : new Key("test", null, "operate2");
    }

    private static Key testKey3For(boolean useSet) {
        return useSet ? new Key("test", "junit", "operate3") : new Key("test", null, "operate3");
    }

    private static Key intKeyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", 1) : new Key("test", null, 1);
    }

    private static Key bytesKeyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", KEY_BYTES) : new Key("test", null, KEY_BYTES);
    }

    private static String testEndpointFor(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV2("operate", "test", "junit", "operate") : ASTestUtils.buildEndpointV2("operate", "test", "operate");
    }

    private static String batchEndpointFor(boolean useSet) {
        return useSet ? "/v2/operate/read/test/junit" : "/v2/operate/read/test";
    }

    private void setupData(Key testKey, Key testKey2, Key testKey3, Key intKey, Key bytesKey) {
        Bin strBin = new Bin("str", "bin");
        Bin intBin = new Bin("int", 5);
        Bin doubleBin = new Bin("double", 5.2);
        client.put(null, testKey, strBin, intBin);
        client.put(null, testKey2, strBin, intBin);
        client.put(null, testKey3, doubleBin);
        client.put(null, intKey, strBin, intBin);
        client.put(null, bytesKey, strBin, intBin);
    }

    private void cleanup(Key testKey, Key testKey2, Key testKey3, Key intKey, Key bytesKey) {
        client.delete(null, testKey);
        client.delete(null, testKey2);
        client.delete(null, testKey3);
        client.delete(null, intKey);
        client.delete(null, bytesKey);
    }

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testGetHeaderOp(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        Key testKey2 = testKey2For(useSet);
        Key testKey3 = testKey3For(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            setupData(testKey, testKey2, testKey3, intKey, bytesKey);

            Map<String, Object> opRequest = new HashMap<>();
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opMap = new HashMap<>();
            opRequest.put("opsList", opList);
            opList.add(opMap);

            opMap.put(OPERATION_TYPE_KEY, OperationTypes.GET_HEADER);

            Map<String, Object> resp = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
            Map<String, Object> rcRecord = (Map<String, Object>) resp.get("record");

            Assertions.assertNull(rcRecord.get(AerospikeAPIConstants.RECORD_BINS));

            Record realRecord = client.getHeader(null, testKey);

            int generation = (int) rcRecord.get(AerospikeAPIConstants.GENERATION);
            Assertions.assertEquals(generation, realRecord.generation);
        } finally {
            cleanup(testKey, testKey2, testKey3, intKey, bytesKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testGetOp(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        Key testKey2 = testKey2For(useSet);
        Key testKey3 = testKey3For(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            setupData(testKey, testKey2, testKey3, intKey, bytesKey);

        Map<String, Object> opRequest = new HashMap<>();
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        opRequest.put("opsList", opList);
        opList.add(opMap);

        opMap.put(OPERATION_TYPE_KEY, OperationTypes.GET);

        Map<String, Object> resp = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
        Map<String, Object> rcRecord = (Map<String, Object>) resp.get("record");
        Map<String, Object> realBins = client.get(null, testKey).bins;

        Assertions.assertTrue(ASTestUtils.compareMapStringObj((Map<String, Object>) rcRecord.get("bins"), realBins));
        } finally {
            cleanup(testKey, testKey2, testKey3, intKey, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testAddIntOp(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        Key testKey2 = testKey2For(useSet);
        Key testKey3 = testKey3For(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            setupData(testKey, testKey2, testKey3, intKey, bytesKey);

        Map<String, Object> opRequest = new HashMap<>();
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        opRequest.put("opsList", opList);
        opList.add(opMap);

        opMap.put("binName", "int");
        opMap.put("incr", 2);
        opMap.put(OPERATION_TYPE_KEY, OperationTypes.ADD);

        opPerformer.performOperationsAndExpect(mockMVC, testEndpoint, opRequest, status().isOk());

        Map<String, Object> expectedBins = new HashMap<>();
        expectedBins.put("str", "bin");
        expectedBins.put("int", 7L);

        Map<String, Object> realBins = client.get(null, testKey).bins;

        Assertions.assertTrue(ASTestUtils.compareMapStringObj(expectedBins, realBins));
        } finally {
            cleanup(testKey, testKey2, testKey3, intKey, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testAddDoubleOp(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        Key testKey2 = testKey2For(useSet);
        Key testKey3 = testKey3For(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            setupData(testKey, testKey2, testKey3, intKey, bytesKey);

        Map<String, Object> opRequest = new HashMap<>();
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        opRequest.put("opsList", opList);
        opList.add(opMap);

        opMap.put("binName", "double");
        opMap.put("incr", 2.2);
        opMap.put(OPERATION_TYPE_KEY, OperationTypes.ADD);

        opPerformer.performOperationsAndExpect(mockMVC, testEndpoint + "3", opRequest, status().isOk());

        Map<String, Object> expectedBins = new HashMap<>();
        expectedBins.put("str", "bin");
        expectedBins.put("double", 7.2);

        Map<String, Object> realBins = client.get(null, testKey).bins;

        Assertions.assertTrue(ASTestUtils.compareMapStringObj(expectedBins, realBins));
        } finally {
            cleanup(testKey, testKey2, testKey3, intKey, bytesKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testReadOp(OperationV2Performer opPerformer, boolean useSet) {
        Key testKey = keyFor(useSet);
        Key testKey2 = testKey2For(useSet);
        Key testKey3 = testKey3For(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            setupData(testKey, testKey2, testKey3, intKey, bytesKey);

        Map<String, Object> opRequest = new HashMap<>();
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        opRequest.put("opsList", opList);
        opList.add(opMap);

        opMap.put(OPERATION_TYPE_KEY, OperationTypes.READ);
        opMap.put("binName", "str");

        Map<String, Object> resp = opPerformer.performOperationsAndReturn(mockMVC, testEndpoint, opRequest);
        Map<String, Object> rcRecord = (Map<String, Object>) resp.get("record");
        /* Only read the str bin on the get*/
        Map<String, Object> realBins = client.get(null, testKey, "str").bins;

        Assertions.assertTrue(ASTestUtils.compareMapStringObj((Map<String, Object>) rcRecord.get("bins"), realBins));
        } finally {
            cleanup(testKey, testKey2, testKey3, intKey, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testPutOp(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            setupData(testKey, testKey2For(useSet), testKey3For(useSet), intKeyFor(useSet), bytesKeyFor(useSet));
            Map<String, Object> opRequest = new HashMap<>();
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opMap = new HashMap<>();
            opRequest.put("opsList", opList);
            opList.add(opMap);

            opMap.put("binName", "new");
            opMap.put("value", "put");
            opMap.put(OPERATION_TYPE_KEY, OperationTypes.PUT);

            opList.add(opMap);

            opPerformer.performOperationsAndExpect(mockMVC, testEndpoint, opRequest, status().isOk());

            Map<String, Object> expectedBins = new HashMap<>();
            expectedBins.put("str", "bin");
            expectedBins.put("int", 5L);
            expectedBins.put("new", "put");

            Map<String, Object> realBins = client.get(null, testKey).bins;

            Assertions.assertTrue(ASTestUtils.compareMapStringObj(expectedBins, realBins));
        } finally {
            cleanup(testKey, testKey2For(useSet), testKey3For(useSet), intKeyFor(useSet), bytesKeyFor(useSet));
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testAppendOp(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            setupData(testKey, testKey2For(useSet), testKey3For(useSet), intKeyFor(useSet), bytesKeyFor(useSet));
            Map<String, Object> opRequest = new HashMap<>();
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opMap = new HashMap<>();
            opRequest.put("opsList", opList);
            opList.add(opMap);

            opMap.put(OPERATION_TYPE_KEY, OperationTypes.APPEND);
            opMap.put("value", "ary");
            opMap.put("binName", "str");

            opPerformer.performOperationsAndExpect(mockMVC, testEndpoint, opRequest, status().isOk());

            /* Only read the str bin on the get*/
            Map<String, Object> expectedBins = new HashMap<>();
            expectedBins.put("str", "binary");
            Map<String, Object> realBins = client.get(null, testKey, "str").bins;

            Assertions.assertTrue(ASTestUtils.compareMapStringObj(expectedBins, realBins));
        } finally {
            cleanup(testKey, testKey2For(useSet), testKey3For(useSet), intKeyFor(useSet), bytesKeyFor(useSet));
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testPrependOp(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            setupData(testKey, testKey2For(useSet), testKey3For(useSet), intKeyFor(useSet), bytesKeyFor(useSet));
            Map<String, Object> opRequest = new HashMap<>();
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opMap = new HashMap<>();
            opRequest.put("opsList", opList);
            opList.add(opMap);

            opMap.put(OPERATION_TYPE_KEY, OperationTypes.PREPEND);
            opMap.put("value", "ro");
            opMap.put("binName", "str");

            opPerformer.performOperationsAndExpect(mockMVC, testEndpoint, opRequest, status().isOk());

            /* Only read the str bin on the get*/
            Map<String, Object> expectedBins = new HashMap<>();
            expectedBins.put("str", "robin");
            Map<String, Object> realBins = client.get(null, testKey, "str").bins;

            Assertions.assertTrue(ASTestUtils.compareMapStringObj(expectedBins, realBins));
        } finally {
            cleanup(testKey, testKey2For(useSet), testKey3For(useSet), intKeyFor(useSet), bytesKeyFor(useSet));
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testTouchOp(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            setupData(testKey, testKey2For(useSet), testKey3For(useSet), intKeyFor(useSet), bytesKeyFor(useSet));
            Map<String, Object> opRequest = new HashMap<>();
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opMap = new HashMap<>();
            opRequest.put("opsList", opList);
            opList.add(opMap);

            Record record = client.get(null, testKey);
            int oldGeneration = record.generation;

            opMap.put(OPERATION_TYPE_KEY, OperationTypes.TOUCH);

            opList.add(opMap);

            opPerformer.performOperationsAndExpect(mockMVC, testEndpoint, opRequest, status().isOk());

            record = client.get(null, testKey);
            Assertions.assertEquals(oldGeneration + 1, record.generation);
        } finally {
            cleanup(testKey, testKey2For(useSet), testKey3For(useSet), intKeyFor(useSet), bytesKeyFor(useSet));
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testTouchOpWithIntegerKey(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key intKey = intKeyFor(useSet);
        String intEndpoint = useSet ? ASTestUtils.buildEndpointV2("operate", "test", "junit", "1") + "?keytype=INTEGER" : ASTestUtils.buildEndpointV2("operate", "test", "1") + "?keytype=INTEGER";
        try {
            setupData(keyFor(useSet), testKey2For(useSet), testKey3For(useSet), intKey, bytesKeyFor(useSet));
            Map<String, Object> opRequest = new HashMap<>();
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opMap = new HashMap<>();
            opRequest.put("opsList", opList);
            opList.add(opMap);

            Record record = client.get(null, intKey);
            int oldGeneration = record.generation;

            opMap.put(OPERATION_TYPE_KEY, OperationTypes.TOUCH);

            opList.add(opMap);

            opPerformer.performOperationsAndExpect(mockMVC, intEndpoint, opRequest, status().isOk());

            record = client.get(null, intKey);
            Assertions.assertEquals(oldGeneration + 1, record.generation);
        } finally {
            cleanup(keyFor(useSet), testKey2For(useSet), testKey3For(useSet), intKey, bytesKeyFor(useSet));
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testTouchOpWithBytesKey(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key bytesKey = bytesKeyFor(useSet);
        try {
            setupData(keyFor(useSet), testKey2For(useSet), testKey3For(useSet), intKeyFor(useSet), bytesKey);
            String urlBytes = Base64.getUrlEncoder().encodeToString((byte[]) bytesKey.userKey.getObject());
            String bytesEndpoint = useSet ? ASTestUtils.buildEndpointV2("operate", "test", "junit", urlBytes) + "?keytype=BYTES" : ASTestUtils.buildEndpointV2("operate", "test", urlBytes) + "?keytype=BYTES";

            Map<String, Object> opRequest = new HashMap<>();
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opMap = new HashMap<>();
            opRequest.put("opsList", opList);
            opList.add(opMap);

            Record record = client.get(null, bytesKey);
            int oldGeneration = record.generation;

            opMap.put(OPERATION_TYPE_KEY, OperationTypes.TOUCH);

            opPerformer.performOperationsAndExpect(mockMVC, bytesEndpoint, opRequest, status().isOk());

            record = client.get(null, bytesKey);
            Assertions.assertEquals(oldGeneration + 1, record.generation);
        } finally {
            cleanup(keyFor(useSet), testKey2For(useSet), testKey3For(useSet), intKeyFor(useSet), bytesKey);
        }
    }

    /*
     * Touch a record by providing it's digest
     */
    @ParameterizedTest
    @MethodSource("getParams")
    public void testTouchOpWithDigestKey(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        try {
            setupData(testKey, testKey2For(useSet), testKey3For(useSet), intKeyFor(useSet), bytesKeyFor(useSet));
            Map<String, Object> opRequest = new HashMap<>();
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opMap = new HashMap<>();
            opRequest.put("opsList", opList);
            opList.add(opMap);

            String urlBytes = Base64.getUrlEncoder().encodeToString(testKey.digest);
            String bytesEndpoint = useSet ? ASTestUtils.buildEndpointV2("operate", "test", "junit", urlBytes) + "?keytype=DIGEST" : ASTestUtils.buildEndpointV2("operate", "test", urlBytes) + "?keytype=DIGEST";

            Record record = client.get(null, testKey);
            int oldGeneration = record.generation;

            opMap.put(OPERATION_TYPE_KEY, OperationTypes.TOUCH);

            opList.add(opMap);

            opPerformer.performOperationsAndExpect(mockMVC, bytesEndpoint, opRequest, status().isOk());

            record = client.get(null, testKey);
            Assertions.assertEquals(oldGeneration + 1, record.generation);
        } finally {
            cleanup(testKey, testKey2For(useSet), testKey3For(useSet), intKeyFor(useSet), bytesKeyFor(useSet));
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testGetOpNonExistentRecord(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        // Key that does not exist
        String fakeEndpoint = useSet
                ? ASTestUtils.buildEndpointV2("operate", "test", "junit", "nonexistent12345")
                : ASTestUtils.buildEndpointV2("operate", "test", "nonexistent12345");
        Map<String, Object> opRequest = new HashMap<>();
        List<Map<String, Object>> opList = new ArrayList<>();
        Map<String, Object> opMap = new HashMap<>();
        opRequest.put("opsList", opList);
        opList.add(opMap);

        opMap.put(OPERATION_TYPE_KEY, OperationTypes.GET);

        opPerformer.performOperationsAndExpect(mockMVC, fakeEndpoint, opRequest, status().isNotFound());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testDeleteOp(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            setupData(testKey, testKey2For(useSet), testKey3For(useSet), intKeyFor(useSet), bytesKeyFor(useSet));
            Map<String, Object> opRequest = new HashMap<>();
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opMap = new HashMap<>();
            opRequest.put("opsList", opList);
            opList.add(opMap);

            opMap.put(OPERATION_TYPE_KEY, OperationTypes.DELETE);

            opPerformer.performOperationsAndExpect(mockMVC, testEndpoint, opRequest, status().isOk());

            Record record = client.get(null, testKey);
            Assertions.assertNull(record);
        } finally {
            cleanup(testKey, testKey2For(useSet), testKey3For(useSet), intKeyFor(useSet), bytesKeyFor(useSet));
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testBatchGetOp(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        Key testKey2 = testKey2For(useSet);
        String batchEndpoint = batchEndpointFor(useSet);
        try {
            setupData(testKey, testKey2, testKey3For(useSet), intKeyFor(useSet), bytesKeyFor(useSet));
            Map<String, Object> opRequest = new HashMap<>();
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opMap = new HashMap<>();
            opRequest.put("opsList", opList);
            opList.add(opMap);

            opMap.put(OPERATION_TYPE_KEY, OperationTypes.GET);

            opList.add(opMap);
            String jsString = objectMapper.writeValueAsString(opRequest);
            String batchUrl = batchEndpoint + "?key=operate&key=operate2";
            String jsonResult = ASTestUtils.performOperationAndReturn(mockMVC, batchUrl, jsString);

            TypeReference<Map<String, Object>> ref = new TypeReference<>() {
            };
            List<Map<String, Object>> records = (List<Map<String, Object>>) objectMapper.readValue(jsonResult, ref)
                    .get("records");
            List<Object> recordBins = Collections.singletonList(records.stream()
                    .map(r -> (Map<String, Object>) r.get("bins"))
                    .map(Map::keySet)
                    .flatMap(Collection::stream)
                    .toList());
            List<Object> expected = Collections.singletonList(Arrays.stream(client.get(null, new Key[]{testKey, testKey2}))
                    .map(r -> r.bins)
                    .map(Map::keySet)
                    .flatMap(Collection::stream)
                    .toList());

            assertIterableEquals(expected, recordBins);
        } finally {
            cleanup(testKey, testKey2, testKey3For(useSet), intKeyFor(useSet), bytesKeyFor(useSet));
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void testBatchGetBinOp(OperationV2Performer opPerformer, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        Key testKey2 = testKey2For(useSet);
        String batchEndpoint = batchEndpointFor(useSet);
        try {
            setupData(testKey, testKey2, testKey3For(useSet), intKeyFor(useSet), bytesKeyFor(useSet));
            Map<String, Object> opRequest = new HashMap<>();
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opMap = new HashMap<>();
            opRequest.put("opsList", opList);
            opList.add(opMap);

            opMap.put("binName", "str");
            opMap.put(OPERATION_TYPE_KEY, OperationTypes.GET);

            opList.add(opMap);
            String jsString = objectMapper.writeValueAsString(opRequest);
            String batchUrl = batchEndpoint + "?key=operate&key=operate2";
            String jsonResult = ASTestUtils.performOperationAndReturn(mockMVC, batchUrl, jsString);

            TypeReference<Map<String, Object>> ref = new TypeReference<>() {
            };
            List<Map<String, Object>> records = (List<Map<String, Object>>) objectMapper.readValue(jsonResult, ref)
                    .get("records");
            List<Object> recordBins = Collections.singletonList(records.stream()
                    .map(r -> (Map<String, Object>) r.get("bins"))
                    .map(Map::keySet)
                    .flatMap(Collection::stream)
                    .toList());
            List<Object> expected = Collections.singletonList(Arrays.stream(client.get(null, new Key[]{testKey, testKey2}))
                    .map(r -> r.bins)
                    .map(Map::keySet)
                    .flatMap(Collection::stream)
                    .filter(k -> k.equals("str"))
                    .toList());

            assertIterableEquals(expected, recordBins);
        } finally {
            cleanup(testKey, testKey2, testKey3For(useSet), intKeyFor(useSet), bytesKeyFor(useSet));
        }
    }
}
