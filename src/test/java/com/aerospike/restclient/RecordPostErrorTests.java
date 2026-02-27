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
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
public class RecordPostErrorTests {

    @Autowired
    private ObjectMapper objectMapper;

    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(true),
                Arguments.of(false)
        );
    }

    private static String nonExistentNSEndpoint(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "fakeNS", "demo", "1") : ASTestUtils.buildEndpointV1("kvs", "fakeNS", "1");
    }

    private static String existingRecordEndpoint(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "junit", "getput") : ASTestUtils.buildEndpointV1("kvs", "test", "getput");
    }

    private static Key testKey(boolean useSet) {
        return useSet ? new Key("test", "junit", "getput") : new Key("test", null, "getput");
    }

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PostRecordToInvalidNamespace(boolean useSet) throws Exception {
        Map<String, Object> binMap = new HashMap<>();
        binMap.put("integer", 12345);

        mockMVC.perform(post(nonExistentNSEndpoint(useSet)).contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(binMap))).andExpect(status().isNotFound());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void PostRecordToExistingKey(boolean useSet) throws Exception {
        Key key = testKey(useSet);
        try {
            Bin baseBin = new Bin("initial", "bin");
            client.put(null, key, baseBin);

            Map<String, Object> binMap = new HashMap<>();
            binMap.put("string", "Aerospike");

            mockMVC.perform(post(existingRecordEndpoint(useSet)).contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(binMap))).andExpect(status().isConflict());
        } finally {
            client.delete(null, key);
        }
    }

}
