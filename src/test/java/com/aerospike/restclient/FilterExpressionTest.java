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
import com.aerospike.client.exp.Exp;
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

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
public class FilterExpressionTest {

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    private MockMvc mockMVC;

    static Stream<Arguments> mappers() {
        return Stream.of(
                Arguments.of(new JSONRestRecordDeserializer(), MediaType.APPLICATION_JSON.toString(), true),
                Arguments.of(new MsgPackRestRecordDeserializer(), "application/msgpack", true),
                Arguments.of(new JSONRestRecordDeserializer(), MediaType.APPLICATION_JSON.toString(), false),
                Arguments.of(new MsgPackRestRecordDeserializer(), "application/msgpack", false)
        );
    }

    private static Key testKey(boolean useSet) {
        return useSet ? new Key("test", "junit", "getput") : new Key("test", null, "getput");
    }

    private static String noBinEndpoint(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "junit", "getput") : ASTestUtils.buildEndpointV1("kvs", "test", "getput");
    }

    private static String buildEndpoint(String noBinEndpoint, String encoded) {
        return noBinEndpoint + "?filterexp=" + encoded;
    }

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @ParameterizedTest
    @MethodSource("mappers")
    public void GetInteger(RecordDeserializer recordDeserializer, String mediaType, boolean useSet) throws Exception {
        Key key = testKey(useSet);
        String noBinEndpt = noBinEndpoint(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();

            Bin intBin = new Bin("integer", 10);
            binMap.put(intBin.name, intBin.value.toInteger());

            client.put(null, key, intBin);

            byte[] filterBytes = Exp.build(Exp.gt(Exp.bin("integer", Exp.Type.INT), Exp.val(1))).getBytes();

            String encoded = Base64.getUrlEncoder().encodeToString(filterBytes);
            String endpoint = buildEndpoint(noBinEndpt, encoded);
            MockHttpServletResponse res = mockMVC.perform(
                            get(endpoint).contentType(MediaType.APPLICATION_JSON).accept(mediaType))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse();
            Map<String, Object> resObject = recordDeserializer.getReturnedBins(res);
            assertTrue(ASTestUtils.compareMapStringObj(resObject, binMap));
        } finally {
            client.delete(null, key);
        }
    }

    @ParameterizedTest
    @MethodSource("mappers")
    public void GetNoInteger(RecordDeserializer recordDeserializer, String mediaType, boolean useSet) throws Exception {
        Key key = testKey(useSet);
        String noBinEndpt = noBinEndpoint(useSet);
        try {
            Bin intBin = new Bin("integer", 10);
            client.put(null, key, intBin);

            byte[] filterBytes = Exp.build(Exp.le(Exp.bin("integer", Exp.Type.INT), Exp.val(1))).getBytes();

            String encoded = Base64.getUrlEncoder().encodeToString(filterBytes);
            String endpoint = buildEndpoint(noBinEndpt, encoded);
            mockMVC.perform(get(endpoint).contentType(MediaType.APPLICATION_JSON).accept(mediaType))
                    .andExpect(status().isNotFound());
        } finally {
            client.delete(null, key);
        }
    }

    @ParameterizedTest
    @MethodSource("mappers")
    public void GetString(RecordDeserializer recordDeserializer, String mediaType, boolean useSet) throws Exception {
        Key key = testKey(useSet);
        String noBinEndpt = noBinEndpoint(useSet);
        try {
            Map<String, Object> binMap = new HashMap<>();
            Bin intBin = new Bin("string", "aerospike");

            binMap.put("string", "aerospike");

            client.put(null, key, intBin);

            byte[] filterBytes = Exp.build(Exp.eq(Exp.bin("string", Exp.Type.STRING), Exp.val("aerospike"))).getBytes();

            String encoded = Base64.getUrlEncoder().encodeToString(filterBytes);
            String endpoint = buildEndpoint(noBinEndpt, encoded);
            MockHttpServletResponse res = mockMVC.perform(
                            get(endpoint).contentType(MediaType.APPLICATION_JSON).accept(mediaType))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse();
            Map<String, Object> resObject = recordDeserializer.getReturnedBins(res);
            assertTrue(ASTestUtils.compareMapStringObj(resObject, binMap));
        } finally {
            client.delete(null, key);
        }
    }

    @ParameterizedTest
    @MethodSource("mappers")
    public void GetNoString(RecordDeserializer recordDeserializer, String mediaType, boolean useSet) throws Exception {
        Key key = testKey(useSet);
        String noBinEndpt = noBinEndpoint(useSet);
        try {
            Bin intBin = new Bin("string", "aerospike");
            client.put(null, key, intBin);

            byte[] filterBytes = Exp.build(Exp.eq(Exp.bin("string", Exp.Type.STRING), Exp.val("aero"))).getBytes();

            String encoded = Base64.getUrlEncoder().encodeToString(filterBytes);
            String endpoint = buildEndpoint(noBinEndpt, encoded);
            mockMVC.perform(get(endpoint).contentType(MediaType.APPLICATION_JSON).accept(mediaType))
                    .andExpect(status().isNotFound());
        } finally {
            client.delete(null, key);
        }
    }
}
