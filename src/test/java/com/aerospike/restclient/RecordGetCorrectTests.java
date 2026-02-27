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
import com.aerospike.client.Value;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;

import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.stream.Stream;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/*
 * These Tests are simple tests which store a simple value via the Java Client, then retrieve it via REST
 * The expected and returned values are compared.
 *
 */
@SpringBootTest
public class RecordGetCorrectTests {

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    private MockMvc mockMVC;

    private static final byte[] KEY_BYTES = {1, 127, 127, 1};

    public static Stream<Arguments> mappers() {
        return Stream.of(
                Arguments.of(new JSONRestRecordDeserializer(), MediaType.APPLICATION_JSON.toString(), true),
                Arguments.of(new MsgPackRestRecordDeserializer(), "application/msgpack", true),
                Arguments.of(new JSONRestRecordDeserializer(), MediaType.APPLICATION_JSON.toString(), false),
                Arguments.of(new MsgPackRestRecordDeserializer(), "application/msgpack", false)
        );
    }

    private static Key keyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", "getput") : new Key("test", null, "getput");
    }

    private static Key intKeyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", 1) : new Key("test", null, 1);
    }

    private static Key bytesKeyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", new Value.BytesValue(KEY_BYTES)) : new Key("test", null, new Value.BytesValue(KEY_BYTES));
    }

    private static String noBinEndpointFor(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "junit", "getput") : ASTestUtils.buildEndpointV1("kvs", "test", "getput");
    }

    private static String intEndpointFor(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "junit", "1") + "?keytype=INTEGER" : ASTestUtils.buildEndpointV1("kvs", "test", "1") + "?keytype=INTEGER";
    }

    private static String bytesEndpointFor(boolean useSet) {
        String b64 = Base64.getUrlEncoder().encodeToString(KEY_BYTES);
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "junit", b64) + "?keytype=BYTES" : ASTestUtils.buildEndpointV1("kvs", "test", b64) + "?keytype=BYTES";
    }

    private static String digestEndpointFor(Key testKey, boolean useSet) {
        String keyDigest = Base64.getUrlEncoder().encodeToString(testKey.digest);
        return useSet ? ASTestUtils.buildEndpointV1("kvs", testKey.namespace, testKey.setName, keyDigest) + "?keytype=DIGEST" : ASTestUtils.buildEndpointV1("kvs", testKey.namespace, keyDigest) + "?keytype=DIGEST";
    }

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @ParameterizedTest
    @MethodSource("mappers")
    public void GetInteger(RecordDeserializer recordDeserializer, String currentMediaType, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String noBinEndpoint = noBinEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            Bin intBin = new Bin("integer", 10);
            binMap.put(intBin.name, intBin.value.toInteger());
            client.put(null, testKey, intBin);

            MockHttpServletResponse res = mockMVC.perform(
                            get(noBinEndpoint).contentType(MediaType.APPLICATION_JSON).accept(currentMediaType))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse();

            Map<String, Object> resObject = recordDeserializer.getReturnedBins(res);
            Assertions.assertTrue(ASTestUtils.compareMapStringObj(resObject, binMap));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("mappers")
    public void GetString(RecordDeserializer recordDeserializer, String currentMediaType, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String noBinEndpoint = noBinEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            Bin intBin = new Bin("string", "aerospike");
            binMap.put("string", "aerospike");
            client.put(null, testKey, intBin);

            MockHttpServletResponse res = mockMVC.perform(
                            get(noBinEndpoint).contentType(MediaType.APPLICATION_JSON).accept(currentMediaType))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse();

            Map<String, Object> resObject = recordDeserializer.getReturnedBins(res);
            Assertions.assertTrue(ASTestUtils.compareMapStringObj(resObject, binMap));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("mappers")
    public void GetDouble(RecordDeserializer recordDeserializer, String currentMediaType, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String noBinEndpoint = noBinEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            Bin intBin = new Bin("double", 2.718);
            binMap.put("double", 2.718);
            client.put(null, testKey, intBin);

            MockHttpServletResponse res = mockMVC.perform(
                            get(noBinEndpoint).contentType(MediaType.APPLICATION_JSON).accept(currentMediaType))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse();

            Map<String, Object> resObject = recordDeserializer.getReturnedBins(res);
            Assertions.assertTrue(ASTestUtils.compareMapStringObj(resObject, binMap));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("mappers")
    public void GetList(RecordDeserializer recordDeserializer, String currentMediaType, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String noBinEndpoint = noBinEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            List<String> putList = Arrays.asList("a", "e", "r", "o");
            Bin intBin = new Bin("list", putList);
            binMap.put("list", putList);
            client.put(null, testKey, intBin);

            MockHttpServletResponse res = mockMVC.perform(
                            get(noBinEndpoint).contentType(MediaType.APPLICATION_JSON).accept(currentMediaType))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse();

            Map<String, Object> resObject = recordDeserializer.getReturnedBins(res);
            Assertions.assertTrue(ASTestUtils.compareMapStringObj(resObject, binMap));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("mappers")
    public void GetMap(RecordDeserializer recordDeserializer, String currentMediaType, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String noBinEndpoint = noBinEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            Map<Object, Object> putMap = new HashMap<>();
            putMap.put("aero", "spike");
            putMap.put("int", 5);
            Bin intBin = new Bin("map", putMap);
            binMap.put("map", putMap);
            client.put(null, testKey, intBin);

            MockHttpServletResponse res = mockMVC.perform(
                            get(noBinEndpoint).contentType(MediaType.APPLICATION_JSON).accept(currentMediaType))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse();

            Map<String, Object> resObject = recordDeserializer.getReturnedBins(res);
            Assertions.assertTrue(ASTestUtils.compareMapStringObj(resObject, binMap));
        } finally {
            client.delete(null, testKey);
        }
    }

    /*
     * Ensure that passing a number of bin names at the end of the URI
     * causes only the specified bins to be contained in the Rest Gateway's response
     */
    @ParameterizedTest
    @MethodSource("mappers")
    public void GetFilteredByBins(RecordDeserializer recordDeserializer, String currentMediaType, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String noBinEndpoint = noBinEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            String[] returnBins = {"A", "B", "E"};
            Bin binA = new Bin("A", 1);
            Bin binB = new Bin("B", 2);
            Bin binC = new Bin("C", 3);
            Bin binD = new Bin("D", 4);
            Bin binE = new Bin("E", 5);
            binMap.put("A", 1);
            binMap.put("B", 2);
            binMap.put("E", 5);
            client.put(null, testKey, binA, binB, binC, binD, binE);

            MockHttpServletResponse res = mockMVC.perform(
                    get(ASTestUtils.addFilterBins(noBinEndpoint, returnBins)).contentType(MediaType.APPLICATION_JSON)
                            .accept(currentMediaType)).andExpect(status().isOk()).andReturn().getResponse();

            Map<String, Object> resObject = recordDeserializer.getReturnedBins(res);
            Assertions.assertTrue(ASTestUtils.compareMapStringObj(resObject, binMap));
        } finally {
            client.delete(null, testKey);
        }
    }

    /*
     * Test to ensure that a user can specify that the userKey is an integer by appending
     * the correct query parameter to a get request.
     */
    @ParameterizedTest
    @MethodSource("mappers")
    public void TestIntegerKey(RecordDeserializer recordDeserializer, String currentMediaType, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        Key intKey = intKeyFor(useSet);
        String intEndpoint = intEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            binMap.put("id", "integer");
            Bin idBin = new Bin("id", "integer");
            client.put(null, intKey, idBin);

            MockHttpServletResponse res = mockMVC.perform(
                            get(intEndpoint).contentType(MediaType.APPLICATION_JSON).accept(currentMediaType))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse();

            Map<String, Object> resObject = recordDeserializer.getReturnedBins(res);
            Assertions.assertTrue(ASTestUtils.compareMapStringObj(resObject, binMap));
        } finally {
            client.delete(null, intKey);
        }
    }

    @ParameterizedTest
    @MethodSource("mappers")
    public void GetWithDigest(RecordDeserializer recordDeserializer, String currentMediaType, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String digestEndpoint = digestEndpointFor(testKey, useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            binMap.put("integer", 10);
            Bin intBin = new Bin("integer", 10);
            client.put(null, testKey, intBin);

            MockHttpServletResponse res = mockMVC.perform(
                            get(digestEndpoint).contentType(MediaType.APPLICATION_JSON).accept(currentMediaType))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse();

            Map<String, Object> resObject = recordDeserializer.getReturnedBins(res);
            Assertions.assertTrue(ASTestUtils.compareMapStringObj(resObject, binMap));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("mappers")
    public void TestBytesKey(RecordDeserializer recordDeserializer, String currentMediaType, boolean useSet) throws Exception {
        Key bytesKey = bytesKeyFor(useSet);
        String bytesEndpoint = bytesEndpointFor(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            Bin idBin = new Bin("id", "bytes");
            client.put(null, bytesKey, idBin);
            binMap.put("id", "bytes");

            MockHttpServletResponse res = mockMVC.perform(
                            get(bytesEndpoint).contentType(MediaType.APPLICATION_JSON).accept(currentMediaType))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse();

            Map<String, Object> resObject = recordDeserializer.getReturnedBins(res);
            Assertions.assertTrue(ASTestUtils.compareMapStringObj(resObject, binMap));
        } finally {
            client.delete(null, bytesKey);
        }
    }

}

interface RecordDeserializer {
    Map<String, Object> getReturnedBins(MockHttpServletResponse res);
}

class MsgPackRestRecordDeserializer implements RecordDeserializer {

    ObjectMapper msgPackMapper;

    public MsgPackRestRecordDeserializer() {
        msgPackMapper = new ObjectMapper(new MessagePackFactory());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> getReturnedBins(MockHttpServletResponse res) {
        byte[] response = res.getContentAsByteArray();
        TypeReference<Map<String, Object>> sOMapType = new TypeReference<Map<String, Object>>() {
        };
        Map<String, Object> recMap = null;
        try {
            recMap = msgPackMapper.readValue(response, sOMapType);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (Map<String, Object>) Objects.requireNonNull(recMap).get("bins");
    }
}

class JSONRestRecordDeserializer implements RecordDeserializer {

    ObjectMapper mapper;

    public JSONRestRecordDeserializer() {
        this.mapper = new ObjectMapper();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> getReturnedBins(MockHttpServletResponse res) {
        String response = null;
        try {
            response = res.getContentAsString();
        } catch (UnsupportedEncodingException e1) {
            e1.printStackTrace();
        }
        TypeReference<Map<String, Object>> sOMapType = new TypeReference<Map<String, Object>>() {
        };
        Map<String, Object> recMap = null;
        try {
            recMap = mapper.readValue(response, sOMapType);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (Map<String, Object>) Objects.requireNonNull(recMap).get("bins");
    }
}
