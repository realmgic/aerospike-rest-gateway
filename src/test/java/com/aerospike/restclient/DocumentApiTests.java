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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Stream;

import static com.aerospike.restclient.util.AerospikeAPIConstants.JSON_PATH;
import static com.aerospike.restclient.util.AerospikeAPIConstants.RECORD_BINS;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
public class DocumentApiTests {

    private static final String testBinName1 = "docBin1";
    private static final String testBinName2 = "docBin2";

    @Autowired
    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    public static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new ObjectMapper(), MediaType.APPLICATION_JSON.toString(), true),
                Arguments.of(new ObjectMapper(new MessagePackFactory()), "application/msgpack", true),
                Arguments.of(new ObjectMapper(), MediaType.APPLICATION_JSON.toString(), false),
                Arguments.of(new ObjectMapper(new MessagePackFactory()), "application/msgpack", false)
        );
    }

    private static Key keyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", "doc") : new Key("test", null, "doc");
    }

    private static String testEndpointFor(boolean useSet) {
        return useSet ? "/v1/document/test/junit/doc" : "/v1/document/test/doc";
    }

    @BeforeEach
    public void setup() {
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testDocumentGet(ObjectMapper objectMapper, String mediaType, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        Map<String, Object> objectMap = new HashMap<>();
        objectMap.put("example1", ImmutableMap.of("key01", Arrays.asList("A1", "B1", "C1")));
        objectMap.put("example2", Arrays.asList(ImmutableMap.of("key02", "E1", "key03", "F1"),
                ImmutableMap.of("key04", "G1", "key05", "H1")));
        try {
            Bin docBin1 = new Bin(testBinName1, objectMap);
            Bin docBin2 = new Bin(testBinName2, objectMap);
            client.put(null, testKey, docBin1, docBin2);

        String binParam = "?" + RECORD_BINS + "=" + testBinName1 + "&" + RECORD_BINS + "=" + testBinName2;
        String jsonPathParam = "&" + JSON_PATH + "=" + URLEncoder.encode("$.example1",
                StandardCharsets.UTF_8.toString());

        MvcResult result = mockMVC.perform(get(testEndpoint + binParam + jsonPathParam))
                .andExpect(status().isOk())
                .andReturn();

        MockHttpServletResponse res = result.getResponse();
        String resJson = res.getContentAsString();
        TypeReference<Map<String, Object>> typeReference = new TypeReference<Map<String, Object>>() {
        };
        Map<String, Object> pathResult = objectMapper.readValue(resJson, typeReference);

        Assertions.assertEquals(objectMap.get("example1"), pathResult.get(testBinName1));
        Assertions.assertEquals(objectMap.get("example1"), pathResult.get(testBinName2));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testDocumentPut(ObjectMapper objectMapper, String mediaType, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        Map<String, Object> objectMap = new HashMap<>();
        objectMap.put("example1", ImmutableMap.of("key01", Arrays.asList("A1", "B1", "C1")));
        objectMap.put("example2", Arrays.asList(ImmutableMap.of("key02", "E1", "key03", "F1"),
                ImmutableMap.of("key04", "G1", "key05", "H1")));
        try {
            Bin docBin1 = new Bin(testBinName1, objectMap);
            Bin docBin2 = new Bin(testBinName2, objectMap);
            client.put(null, testKey, docBin1, docBin2);

            String binParam = "?" + RECORD_BINS + "=" + testBinName1 + "&" + RECORD_BINS + "=" + testBinName2;
            String jsonPathParam = "&" + JSON_PATH + "=" + URLEncoder.encode("$.example3",
                    StandardCharsets.UTF_8.toString());

            byte[] payload = objectMapper.writeValueAsBytes("str3");
            mockMVC.perform(
                            put(testEndpoint + binParam + jsonPathParam).contentType(mediaType).content(payload).accept(mediaType))
                    .andExpect(status().isAccepted());

            MvcResult result = mockMVC.perform(get(testEndpoint + binParam + jsonPathParam))
                    .andExpect(status().isOk())
                    .andReturn();

            MockHttpServletResponse res = result.getResponse();
            String resJson = res.getContentAsString();
            TypeReference<Map<String, String>> typeReference = new TypeReference<Map<String, String>>() {
            };
            Map<String, String> pathResult = objectMapper.readValue(resJson, typeReference);

            Assertions.assertEquals("str3", pathResult.get(testBinName1));
            Assertions.assertEquals("str3", pathResult.get(testBinName2));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testDocumentAppend(ObjectMapper objectMapper, String mediaType, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        Map<String, Object> objectMap = new HashMap<>();
        objectMap.put("example1", ImmutableMap.of("key01", Arrays.asList("A1", "B1", "C1")));
        objectMap.put("example2", Arrays.asList(ImmutableMap.of("key02", "E1", "key03", "F1"),
                ImmutableMap.of("key04", "G1", "key05", "H1")));
        try {
            Bin docBin1 = new Bin(testBinName1, objectMap);
            Bin docBin2 = new Bin(testBinName2, objectMap);
            client.put(null, testKey, docBin1, docBin2);

            String binParam = "?" + RECORD_BINS + "=" + testBinName1 + "&" + RECORD_BINS + "=" + testBinName2;
            String jsonPathParam = "&" + JSON_PATH + "=" + URLEncoder.encode("$.example1.key01",
                    StandardCharsets.UTF_8.toString());

            byte[] payload = objectMapper.writeValueAsBytes("D1");
            mockMVC.perform(
                            post(testEndpoint + binParam + jsonPathParam).contentType(mediaType).content(payload).accept(mediaType))
                    .andExpect(status().isAccepted());

            MvcResult result = mockMVC.perform(get(testEndpoint + binParam + jsonPathParam))
                    .andExpect(status().isOk())
                    .andReturn();

            MockHttpServletResponse res = result.getResponse();
            String resJson = res.getContentAsString();
            TypeReference<Map<String, List<String>>> typeReference = new TypeReference<Map<String, List<String>>>() {
            };
            Map<String, List<String>> pathResult = objectMapper.readValue(resJson, typeReference);

            Assertions.assertEquals(Arrays.asList("A1", "B1", "C1", "D1"), pathResult.get(testBinName1));
            Assertions.assertEquals(Arrays.asList("A1", "B1", "C1", "D1"), pathResult.get(testBinName2));
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testDocumentDelete(ObjectMapper objectMapper, String mediaType, boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        String testEndpoint = testEndpointFor(useSet);
        Map<String, Object> objectMap = new HashMap<>();
        objectMap.put("example1", ImmutableMap.of("key01", Arrays.asList("A1", "B1", "C1")));
        objectMap.put("example2", Arrays.asList(ImmutableMap.of("key02", "E1", "key03", "F1"),
                ImmutableMap.of("key04", "G1", "key05", "H1")));
        try {
            Bin docBin1 = new Bin(testBinName1, objectMap);
            Bin docBin2 = new Bin(testBinName2, objectMap);
            client.put(null, testKey, docBin1, docBin2);

            String binParam = "?" + RECORD_BINS + "=" + testBinName1 + "&" + RECORD_BINS + "=" + testBinName2;
            String jsonDeletePathParam = "&" + JSON_PATH + "=" + URLEncoder.encode("$.example2[1]",
                    StandardCharsets.UTF_8.toString());
            String jsonPathParam = "&" + JSON_PATH + "=" + URLEncoder.encode("$.example2",
                    StandardCharsets.UTF_8.toString());

            mockMVC.perform(delete(testEndpoint + binParam + jsonDeletePathParam)).andExpect(status().isNoContent());

            MvcResult result = mockMVC.perform(get(testEndpoint + binParam + jsonPathParam))
                    .andExpect(status().isOk())
                    .andReturn();

            MockHttpServletResponse res = result.getResponse();
            String resJson = res.getContentAsString();
            TypeReference<Map<String, List<Map<String, Object>>>> typeReference = new TypeReference<Map<String, List<Map<String, Object>>>>() {
            };
            Map<String, List<Map<String, Object>>> pathResult = objectMapper.readValue(resJson, typeReference);

            List<Map<String, Object>> expected = Collections.singletonList(ImmutableMap.of("key02", "E1", "key03", "F1"));

            Assertions.assertEquals(expected, pathResult.get(testBinName1));
            Assertions.assertEquals(expected, pathResult.get(testBinName2));
        } finally {
            client.delete(null, testKey);
        }
    }
}
