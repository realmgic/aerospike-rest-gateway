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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Record;
import com.aerospike.client.*;
import com.aerospike.restclient.config.JSONMessageConverter;
import com.aerospike.restclient.config.MsgPackConverter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.*;
import java.util.stream.Stream;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
public class RecordPostCorrectTests {

    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    private static final byte[] KEY_BYTES = {1, 127, 127, 1};

    public static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JSONPostPerformer(MediaType.APPLICATION_JSON.toString(), JSONMessageConverter.getJSONObjectMapper()), true),
                Arguments.of(new MsgPackPostPerformer("application/msgpack", MsgPackConverter.getASMsgPackObjectMapper()), true),
                Arguments.of(new JSONPostPerformer(MediaType.APPLICATION_JSON.toString(), JSONMessageConverter.getJSONObjectMapper()), false),
                Arguments.of(new MsgPackPostPerformer("application/msgpack", MsgPackConverter.getASMsgPackObjectMapper()), false)
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

    private static void cleanup(AerospikeClient client, Key testKey, Key intKey, Key bytesKey) {
        client.delete(null, testKey);
        try {
            client.delete(null, bytesKey);
        } catch (AerospikeException ignore) {
        }
        try {
            client.delete(null, intKey);
        } catch (AerospikeException ignore) {
        }
    }

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PutInteger(PostPerformer postPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            binMap.put("integer", 12345);
            postPerformer.perform(mockMVC, testEndpoint, binMap);

            Record record = client.get(null, testKey);
            Assertions.assertEquals(record.bins.get("integer"), 12345L);
        } finally {
            cleanup(client, testKey, intKey, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PutString(PostPerformer postPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            binMap.put("string", "Aerospike");
            postPerformer.perform(mockMVC, testEndpoint, binMap);
            Record record = client.get(null, testKey);
            Assertions.assertEquals(record.bins.get("string"), "Aerospike");
        } finally {
            cleanup(client, testKey, intKey, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PutDouble(PostPerformer postPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            binMap.put("double", 2.718);
            postPerformer.perform(mockMVC, testEndpoint, binMap);
            Record record = client.get(null, testKey);
            Assertions.assertEquals(record.bins.get("double"), 2.718);
        } finally {
            cleanup(client, testKey, intKey, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PutList(PostPerformer postPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            List<?> trueList = Arrays.asList(1L, "a", 3.5);
            binMap.put("ary", trueList);
            postPerformer.perform(mockMVC, testEndpoint, binMap);
            Record record = client.get(null, testKey);
            Assertions.assertTrue(ASTestUtils.compareCollection((List<?>) record.bins.get("ary"), trueList));
        } finally {
            cleanup(client, testKey, intKey, bytesKey);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("getParams")
    public void PutMapStringKeys(PostPerformer postPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            Map<Object, Object> testMap = new HashMap<>();
            testMap.put("string", "a string");
            testMap.put("long", 2L);
            testMap.put("double", 4.5);
            Map<String, Object> binMap = new HashMap<>();
            binMap.put("map", testMap);
            postPerformer.perform(mockMVC, testEndpoint, binMap);
            Record record = client.get(null, testKey);
            Assertions.assertTrue(ASTestUtils.compareMap((Map<Object, Object>) record.bins.get("map"), testMap));
        } finally {
            cleanup(client, testKey, intKey, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PutWithIntegerKey(PostPerformer postPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String intEndpoint = intEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            binMap.put("integer", 12345);
            postPerformer.perform(mockMVC, intEndpoint, binMap);
            Record record = client.get(null, intKey);
            Assertions.assertEquals(record.bins.get("integer"), 12345L);
        } finally {
            cleanup(client, testKey, intKey, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PutWithBytesKey(PostPerformer postPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String bytesEndpoint = bytesEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            binMap.put("integer", 12345);
            postPerformer.perform(mockMVC, bytesEndpoint, binMap);
            Record record = client.get(null, bytesKey);
            Assertions.assertEquals(record.bins.get("integer"), 12345L);
        } finally {
            cleanup(client, testKey, intKey, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PutWithDigestKey(PostPerformer postPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String digestEndpoint = digestEndpointFor(testKey, useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            binMap.put("integer", 12345);
            postPerformer.perform(mockMVC, digestEndpoint, binMap);
            Record record = client.get(null, testKey);
            Assertions.assertEquals(record.bins.get("integer"), 12345L);
        } finally {
            cleanup(client, testKey, intKey, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PostByteArray(PostPerformer postPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            Map<String, String> byteArray = new HashMap<>();
            byte[] arr = new byte[]{1, 101};
            byteArray.put("type", "BYTE_ARRAY");
            byteArray.put("value", Base64.getEncoder().encodeToString(arr));
            binMap.put("byte_array", byteArray);
            postPerformer.perform(mockMVC, testEndpoint, binMap);
            Record record = client.get(null, testKey);
            Assertions.assertArrayEquals((byte[]) record.bins.get("byte_array"), arr);
        } finally {
            cleanup(client, testKey, intKey, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PostBase64EncodedGeoJson(PostPerformer postPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            Map<String, String> geoJson = new HashMap<>();
            String geoStr = "{\"type\": \"Point\", \"coordinates\": [-80.604333, 28.608389]}";
            geoJson.put("type", "GEO_JSON");
            geoJson.put("value", Base64.getEncoder().encodeToString(geoStr.getBytes()));
            binMap.put("geo_json", geoJson);
            postPerformer.perform(mockMVC, testEndpoint, binMap);
            Record record = client.get(null, testKey);
            Assertions.assertEquals(((Value.GeoJSONValue) record.bins.get("geo_json")).getObject(), geoStr);
        } finally {
            cleanup(client, testKey, intKey, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PostGeoJson(PostPerformer postPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            Map<String, Object> geoJson = new HashMap<>();
            List<Double> coordinates = new ArrayList<>();
            coordinates.add(-80.604333);
            coordinates.add(28.608389);
            String geoStr = "{\"coordinates\":[-80.604333,28.608389],\"type\":\"Point\"}";
            geoJson.put("coordinates", coordinates);
            geoJson.put("type", "Point");
            binMap.put("geo_json", geoJson);
            postPerformer.perform(mockMVC, testEndpoint, binMap);
            Record record = client.get(null, testKey);
            Assertions.assertEquals(((Value.GeoJSONValue) record.bins.get("geo_json")).getObject(), geoStr);
        } finally {
            cleanup(client, testKey, intKey, bytesKey);
        }
    }

    @Disabled("Fails because GeoJSON can't be nested in a CDT for JSON. Only MSGPack")
    @ParameterizedTest
    @MethodSource("getParams")
    public void PostNestedGeoJson(PostPerformer postPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            Map<String, Object> geoJson = new HashMap<>();
            List<Double> coordinates = new ArrayList<>();
            coordinates.add(-80.604333);
            coordinates.add(28.608389);
            String geoStr = "{\"coordinates\":[-80.604333,28.608389],\"type\":\"Point\"}";
            geoJson.put("coordinates", coordinates);
            geoJson.put("type", "Point");
            List<Object> geoJsonList = new ArrayList<>();
            geoJsonList.add(geoJson);
            binMap.put("geo_json_list", geoJsonList);
            postPerformer.perform(mockMVC, testEndpoint, binMap);
            Record record = client.get(null, testKey);
            List<Object> actualGeoJsonList = (List<Object>) record.bins.get("geo_json_list");
            Assertions.assertEquals(((Value.GeoJSONValue) actualGeoJsonList.get(0)).getObject(), geoStr);
        } finally {
            cleanup(client, testKey, intKey, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PostBoolean(PostPerformer postPerformer, boolean useSet) throws Exception {
        Key testKey = testKeyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            binMap.put("bool", true);
            postPerformer.perform(mockMVC, testEndpoint, binMap);
            Record record = client.get(null, testKey);
            Assertions.assertEquals((long) record.bins.get("bool"), 1L);
        } finally {
            cleanup(client, testKey, intKey, bytesKey);
        }
    }
}

interface PostPerformer {
    void perform(MockMvc mockMVC, String testEndpoint, Map<String, Object> binMap) throws Exception;
}

class JSONPostPerformer implements PostPerformer {
    String mediaType;
    ObjectMapper mapper;

    public JSONPostPerformer(String mediaType, ObjectMapper mapper) {
        this.mediaType = mediaType;
        this.mapper = mapper;
    }

    @Override
    public void perform(MockMvc mockMVC, String testEndpoint, Map<String, Object> binMap) throws Exception {
        mockMVC.perform(post(testEndpoint).contentType(mediaType).content(mapper.writeValueAsString(binMap)))
                .andExpect(status().isCreated());
    }

}

class MsgPackPostPerformer implements PostPerformer {
    String mediaType;
    ObjectMapper mapper;

    public MsgPackPostPerformer(String mediaType, ObjectMapper mapper) {
        this.mediaType = mediaType;
        this.mapper = mapper;
    }

    @Override
    public void perform(MockMvc mockMVC, String testEndpoint, Map<String, Object> binMap) throws Exception {
        mockMVC.perform(post(testEndpoint).contentType(mediaType).content(mapper.writeValueAsBytes(binMap)))
                .andExpect(status().isCreated());
    }

}