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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
public class ExpressionParserTest {

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    private MockMvc mockMVC;

    static Stream<Arguments> mappers() {
        return Stream.of(
                Arguments.of(new JSONRestRecordDeserializer(), MediaType.APPLICATION_JSON.toString(), true, "filterexp"),
                Arguments.of(new MsgPackRestRecordDeserializer(), "application/msgpack", true, "filterexp"),
                Arguments.of(new JSONRestRecordDeserializer(), MediaType.APPLICATION_JSON.toString(), false, "filterexp"),
                Arguments.of(new MsgPackRestRecordDeserializer(), "application/msgpack", false, "filterexp")
        );
    }

    private static Key testKey(boolean useSet) {
        return useSet ? new Key("test", "junit", "getput") : new Key("test", null, "getput");
    }

    private static String noBinEndpoint(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "junit", "getput") : ASTestUtils.buildEndpointV1("kvs", "test", "getput");
    }

    private static String buildEndpoint(String noBinEndpoint, String expParameter, String encoded) {
        return noBinEndpoint + "?" + expParameter + "=" + encoded;
    }

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @ParameterizedTest
    @MethodSource("mappers")
    public void GetInteger(RecordDeserializer recordDeserializer, String currentMediaType, boolean useSet, String expParameter) throws Exception {
        Key key = testKey(useSet);
        String noBinEndpt = noBinEndpoint(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();

            Bin intBin = new Bin("integer", 10);
            binMap.put(intBin.name, intBin.value.toInteger());

            client.put(null, key, intBin);
            List<String> exps = Arrays.asList("integer > 1", "not(integer < 1) and LAST_UPDATE(>=, 1577880000)",
                    "DIGEST_MODULO(3, ==, 1) or not VOID_TIME(!=, 1577880000)" //VOID_TIME is 0
            );
            for (String predexp : exps) {
                String encoded = Base64.getUrlEncoder().encodeToString(predexp.getBytes());
                String endpoint = buildEndpoint(noBinEndpt, expParameter, encoded);
                MockHttpServletResponse res = mockMVC.perform(
                                get(endpoint).contentType(MediaType.APPLICATION_JSON).accept(currentMediaType))
                        .andExpect(status().isOk())
                        .andReturn()
                        .getResponse();
                Map<String, Object> resObject = recordDeserializer.getReturnedBins(res);
                assertTrue(ASTestUtils.compareMapStringObj(resObject, binMap));
            }
        } finally {
            client.delete(null, key);
        }
    }

    @ParameterizedTest
    @MethodSource("mappers")
    public void GetString(RecordDeserializer recordDeserializer, String currentMediaType, boolean useSet, String expParameter) throws Exception {
        Key key = testKey(useSet);
        String noBinEndpt = noBinEndpoint(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            Bin intBin = new Bin("string", "aerospike");

            binMap.put("string", "aerospike");

            client.put(null, key, intBin);
            List<String> exps = Arrays.asList("string == aerospike",
                    "not(string == hello) and STRING_REGEX(string, \"[\\s]*\")", "(string == aerospike) or int == 100");
            for (String predexp : exps) {
                String encoded = Base64.getUrlEncoder().encodeToString(predexp.getBytes());
                String endpoint = buildEndpoint(noBinEndpt, expParameter, encoded);
                MockHttpServletResponse res = mockMVC.perform(
                                get(endpoint).contentType(MediaType.APPLICATION_JSON).accept(currentMediaType))
                        .andExpect(status().isOk())
                        .andReturn()
                        .getResponse();
                Map<String, Object> resObject = recordDeserializer.getReturnedBins(res);
                assertTrue(ASTestUtils.compareMapStringObj(resObject, binMap));
            }
        } finally {
            client.delete(null, key);
        }
    }

    @ParameterizedTest
    @MethodSource("mappers")
    public void GetList(RecordDeserializer recordDeserializer, String currentMediaType, boolean useSet, String expParameter) throws Exception {
        Key key = testKey(useSet);
        String noBinEndpt = noBinEndpoint(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            List<String> putList = Arrays.asList("a", "e", "r", "o");
            Bin intBin = new Bin("list", putList);

            binMap.put("list", putList);

            client.put(null, key, intBin);
            List<String> exps = Arrays.asList("LIST_ITERATE_OR(list, ==, r)", "LIST_ITERATE_AND(list, !=, b)",
                    "(LIST_ITERATE_OR(list, ==, r) and LIST_ITERATE_AND(list, !=, b)) or str == hello");
            for (String predexp : exps) {
                String encoded = Base64.getUrlEncoder().encodeToString(predexp.getBytes());
                String endpoint = buildEndpoint(noBinEndpt, expParameter, encoded);
                MockHttpServletResponse res = mockMVC.perform(
                                get(endpoint).contentType(MediaType.APPLICATION_JSON).accept(currentMediaType))
                        .andExpect(status().isOk())
                        .andReturn()
                        .getResponse();
                Map<String, Object> resObject = recordDeserializer.getReturnedBins(res);
                assertTrue(ASTestUtils.compareMapStringObj(resObject, binMap));
            }
        } finally {
            client.delete(null, key);
        }
    }

    @ParameterizedTest
    @MethodSource("mappers")
    public void GetMap(RecordDeserializer recordDeserializer, String currentMediaType, boolean useSet, String expParameter) throws Exception {
        Key key = testKey(useSet);
        String noBinEndpt = noBinEndpoint(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            Map<Object, Object> putMap = new HashMap<>();
            putMap.put("aero", "spike");
            putMap.put("int", 5);

            Bin intBin = new Bin("map", putMap);

            binMap.put("map", putMap);

            client.put(null, key, intBin);
            List<String> exps = Arrays.asList("MAPKEY_ITERATE_OR(map, ==, aero)", "MAPVAL_ITERATE_OR(map, ==, 5)",
                    "MAPKEY_ITERATE_AND(map, !=, z) or MAPVAL_ITERATE_AND(map, !=, 500)");
            for (String predexp : exps) {
                String encoded = Base64.getUrlEncoder().encodeToString(predexp.getBytes());
                String endpoint = buildEndpoint(noBinEndpt, expParameter, encoded);
                MockHttpServletResponse res = mockMVC.perform(
                                get(endpoint).contentType(MediaType.APPLICATION_JSON).accept(currentMediaType))
                        .andExpect(status().isOk())
                        .andReturn()
                        .getResponse();
                Map<String, Object> resObject = recordDeserializer.getReturnedBins(res);
                assertTrue(ASTestUtils.compareMapStringObj(resObject, binMap));
            }
        } finally {
            client.delete(null, key);
        }
    }
}
