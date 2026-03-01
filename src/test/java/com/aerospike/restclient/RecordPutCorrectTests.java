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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.*;
import java.util.stream.Stream;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
public class RecordPutCorrectTests {

    @Autowired
    private ObjectMapper objectMapper;

    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    private static final byte[] KEY_BYTES = {1, 127, 127, 1};

    public static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JSONPutPerformer(MediaType.APPLICATION_JSON.toString(), new ObjectMapper()), true),
                Arguments.of(new MsgPackPutPerformer("application/msgpack", new ObjectMapper(new MessagePackFactory())), true),
                Arguments.of(new JSONPutPerformer(MediaType.APPLICATION_JSON.toString(), new ObjectMapper()), false),
                Arguments.of(new MsgPackPutPerformer("application/msgpack", new ObjectMapper(new MessagePackFactory())), false)
        );
    }

    private static Key testKeyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", "getput") : new Key("test", null, "getput");
    }

    private static Key intKeyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", 1) : new Key("test", null, 1);
    }

    private static Key bytesKeyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", KEY_BYTES) : new Key("test", null, KEY_BYTES);
    }

    private static String testEndpointFor(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "junit", "getput") : ASTestUtils.buildEndpointV1("kvs", "test", "getput");
    }

    private static String digestEndpointFor(Key testKey, boolean useSet) {
        String urlDigest = Base64.getUrlEncoder().encodeToString(testKey.digest);
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "junit", urlDigest) + "?keytype=DIGEST" : ASTestUtils.buildEndpointV1("kvs", "test", urlDigest) + "?keytype=DIGEST";
    }

    private static String bytesEndpointFor(boolean useSet) {
        Key bytesKey = bytesKeyFor(useSet);
        String urlBytes = Base64.getUrlEncoder().encodeToString((byte[]) bytesKey.userKey.getObject());
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "junit", urlBytes) + "?keytype=BYTES" : ASTestUtils.buildEndpointV1("kvs", "test", urlBytes) + "?keytype=BYTES";
    }

    private static String intEndpointFor(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "junit", "1") + "?keytype=INTEGER" : ASTestUtils.buildEndpointV1("kvs", "test", "1") + "?keytype=INTEGER";
    }

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PutInteger(PutPerformer putPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            Bin baseBin = new Bin("initial", "bin");
            client.put(null, testKey, baseBin);
            client.put(null, intKey, baseBin);
            client.put(null, bytesKey, baseBin);

            Map<String, Object> binMap = new HashMap<>();
            binMap.put("integer", 12345);
            putPerformer.perform(mockMVC, testEndpoint, binMap);

            Record record = client.get(null, testKey);
            Assertions.assertFalse(record.bins.containsKey("initial"));
            Assertions.assertEquals(record.bins.get("integer"), 12345L);
        } finally {
            client.delete(null, testKey);
            client.delete(null, intKey);
            client.delete(null, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PutString(PutPerformer putPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            Bin baseBin = new Bin("initial", "bin");
            client.put(null, testKey, baseBin);
            client.put(null, intKey, baseBin);
            client.put(null, bytesKey, baseBin);

            Map<String, Object> binMap = new HashMap<>();
            binMap.put("string", "Aerospike");
            putPerformer.perform(mockMVC, testEndpoint, binMap);

            Record record = client.get(null, testKey);
            Assertions.assertFalse(record.bins.containsKey("initial"));
            Assertions.assertEquals(record.bins.get("string"), "Aerospike");
        } finally {
            client.delete(null, testKey);
            client.delete(null, intKey);
            client.delete(null, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PutDouble(PutPerformer putPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            Bin baseBin = new Bin("initial", "bin");
            client.put(null, testKey, baseBin);
            client.put(null, intKey, baseBin);
            client.put(null, bytesKey, baseBin);

            Map<String, Object> binMap = new HashMap<>();
            binMap.put("double", 2.718);
            putPerformer.perform(mockMVC, testEndpoint, binMap);

            Record record = client.get(null, testKey);
            Assertions.assertFalse(record.bins.containsKey("initial"));
            Assertions.assertEquals(record.bins.get("double"), 2.718);
        } finally {
            client.delete(null, testKey);
            client.delete(null, intKey);
            client.delete(null, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PutList(PutPerformer putPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            Bin baseBin = new Bin("initial", "bin");
            client.put(null, testKey, baseBin);
            client.put(null, intKey, baseBin);
            client.put(null, bytesKey, baseBin);

            Map<String, Object> binMap = new HashMap<>();
            List<?> trueList = Arrays.asList(1L, "a", 3.5);
            binMap.put("ary", trueList);
            putPerformer.perform(mockMVC, testEndpoint, binMap);

            Record record = client.get(null, testKey);
            Assertions.assertFalse(record.bins.containsKey("initial"));
            Assertions.assertTrue(ASTestUtils.compareCollection((List<?>) record.bins.get("ary"), trueList));
        } finally {
            client.delete(null, testKey);
            client.delete(null, intKey);
            client.delete(null, bytesKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void PutMapStringKeys(PutPerformer putPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            Bin baseBin = new Bin("initial", "bin");
            client.put(null, testKey, baseBin);
            client.put(null, intKey, baseBin);
            client.put(null, bytesKey, baseBin);

            Map<Object, Object> testMap = new HashMap<>();
            testMap.put("string", "a string");
            testMap.put("long", 2L);
            testMap.put("double", 4.5);
            Map<String, Object> binMap = new HashMap<>();
            binMap.put("map", testMap);
            putPerformer.perform(mockMVC, testEndpoint, binMap);

            Record record = client.get(null, testKey);
            Assertions.assertFalse(record.bins.containsKey("initial"));
            Assertions.assertTrue(ASTestUtils.compareMap((Map<Object, Object>) record.bins.get("map"), testMap));
        } finally {
            client.delete(null, testKey);
            client.delete(null, intKey);
            client.delete(null, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PutIntegerKey(PutPerformer putPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        String intEndpoint = intEndpointFor(useSet);
        try {
            Bin baseBin = new Bin("initial", "bin");
            client.put(null, testKey, baseBin);
            client.put(null, intKey, baseBin);
            client.put(null, bytesKey, baseBin);

            Map<String, Object> binMap = new HashMap<>();
            binMap.put("integer", 12345);
            putPerformer.perform(mockMVC, intEndpoint, binMap);

            Record record = client.get(null, intKey);
            Assertions.assertFalse(record.bins.containsKey("initial"));
            Assertions.assertEquals(record.bins.get("integer"), 12345L);
        } finally {
            client.delete(null, testKey);
            client.delete(null, intKey);
            client.delete(null, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PutBytesKey(PutPerformer putPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        String bytesEndpoint = bytesEndpointFor(useSet);
        try {
            Bin baseBin = new Bin("initial", "bin");
            client.put(null, testKey, baseBin);
            client.put(null, intKey, baseBin);
            client.put(null, bytesKey, baseBin);

            Map<String, Object> binMap = new HashMap<>();
            binMap.put("integer", 12345);
            putPerformer.perform(mockMVC, bytesEndpoint, binMap);

            Record record = client.get(null, bytesKey);
            Assertions.assertFalse(record.bins.containsKey("initial"));
            Assertions.assertEquals(record.bins.get("integer"), 12345L);
        } finally {
            client.delete(null, testKey);
            client.delete(null, intKey);
            client.delete(null, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PutDigestKey(PutPerformer putPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String digestEndpoint = digestEndpointFor(testKey, useSet);
        try {
            Bin baseBin = new Bin("initial", "bin");
            client.put(null, testKey, baseBin);
            client.put(null, intKey, baseBin);
            client.put(null, bytesKey, baseBin);

            Map<String, Object> binMap = new HashMap<>();
            binMap.put("integer", 12345);
            putPerformer.perform(mockMVC, digestEndpoint, binMap);

            Record record = client.get(null, testKey);
            Assertions.assertFalse(record.bins.containsKey("initial"));
            Assertions.assertEquals(record.bins.get("integer"), 12345L);
        } finally {
            client.delete(null, testKey);
            client.delete(null, intKey);
            client.delete(null, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PutIntegerWithGenerationMismatch(PutPerformer putPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            Bin baseBin = new Bin("initial", "bin");
            client.put(null, testKey, baseBin);
            client.put(null, intKey, baseBin);
            client.put(null, bytesKey, baseBin);

            Map<String, Object> binMap = new HashMap<>();
            binMap.put("integer", 12345);
            String queryParams = "?generation=150&generationPolicy=EXPECT_GEN_EQUAL";
            mockMVC.perform(put(testEndpoint + queryParams).contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(binMap))).andExpect(status().isConflict());

            Record record = client.get(null, testKey);
            Assertions.assertTrue(record.bins.containsKey("initial"));
            Assertions.assertFalse(record.bins.containsKey("integer"));
        } finally {
            client.delete(null, testKey);
            client.delete(null, intKey);
            client.delete(null, bytesKey);
        }
    }
}

interface PutPerformer {
    void perform(MockMvc mockMVC, String testEndpoint, Map<String, Object> binMap) throws Exception;
}

class JSONPutPerformer implements PutPerformer {
    String mediaType;
    ObjectMapper mapper;

    public JSONPutPerformer(String mediaType, ObjectMapper mapper) {
        this.mediaType = mediaType;
        this.mapper = mapper;
    }

    @Override
    public void perform(MockMvc mockMVC, String testEndpoint, Map<String, Object> binMap) throws Exception {
        mockMVC.perform(put(testEndpoint).contentType(mediaType).content(mapper.writeValueAsString(binMap)))
                .andExpect(status().isNoContent());
    }

}

class MsgPackPutPerformer implements PutPerformer {
    String mediaType;
    ObjectMapper mapper;

    public MsgPackPutPerformer(String mediaType, ObjectMapper mapper) {
        this.mediaType = mediaType;
        this.mapper = mapper;
    }

    @Override
    public void perform(MockMvc mockMVC, String testEndpoint, Map<String, Object> binMap) throws Exception {
        mockMVC.perform(put(testEndpoint).contentType(mediaType).content(mapper.writeValueAsBytes(binMap)))
                .andExpect(status().isNoContent());
    }

}
