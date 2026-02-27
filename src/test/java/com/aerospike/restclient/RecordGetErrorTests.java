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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
public class RecordGetErrorTests {

    static Stream<Arguments> getParams() {
        return Stream.of(Arguments.of(true), Arguments.of(false));
    }

    private static String nonExistentNSendpoint(boolean useSet) {
        if (useSet) {
            return ASTestUtils.buildEndpointV1("kvs", "fakeNS", "demo", "1");
        } else {
            return ASTestUtils.buildEndpointV1("kvs", "fakeNS", "1");
        }
    }

    private static String nonExistentRecordEndpoint(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "demo", "thisisnotarealkeyforarecord") : ASTestUtils.buildEndpointV1("kvs", "test", "thisisnotarealkeyforarecord");
    }

    private static String invalidKeytypeEndpoint(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "demo", "1") + "?keytype=skeleton" : ASTestUtils.buildEndpointV1("kvs", "test", "1") + "?keytype=skeleton";
    }

    private static String invalidIntegerEndpoint(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "demo", "key") + "?keytype=INTEGER" : ASTestUtils.buildEndpointV1("kvs", "test", "key") + "?keytype=INTEGER";
    }

    private static String invalidBytesEndpoint(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "demo", "/=") + "?keytype=BYTES" : ASTestUtils.buildEndpointV1("kvs", "test", "/=") + "?keytype=BYTES";
    }

    private static String invalidDigestEndpoint(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "demo", "key") + "?keytype=DIGEST" : ASTestUtils.buildEndpointV1("kvs", "test", "key") + "?keytype=DIGEST";
    }

    @Autowired
    private ObjectMapper objectMapper;

    private MockMvc mockMVC;

    @Autowired
    private WebApplicationContext wac;

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void GetFromNonExistentNS(boolean useSet) throws Exception {
        MvcResult result = mockMVC.perform(get(nonExistentNSendpoint(useSet))).andExpect(status().isNotFound()).andReturn();

        MockHttpServletResponse res = result.getResponse();
        String resJson = res.getContentAsString();
        TypeReference<Map<String, Object>> sOMapType = new TypeReference<Map<String, Object>>() {
        };
        Map<String, Object> resObject = objectMapper.readValue(resJson, sOMapType);

        assertFalse((boolean) resObject.get("inDoubt"));
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void GetNonExistentRecord(boolean useSet) throws Exception {
        MvcResult result = mockMVC.perform(get(nonExistentRecordEndpoint(useSet))).andExpect(status().isNotFound()).andReturn();

        MockHttpServletResponse res = result.getResponse();
        String resJson = res.getContentAsString();
        TypeReference<Map<String, Object>> sOMapType = new TypeReference<Map<String, Object>>() {
        };
        Map<String, Object> resObject = objectMapper.readValue(resJson, sOMapType);

        assertFalse((boolean) resObject.get("inDoubt"));
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void GetWithInvalidKeyType(boolean useSet) throws Exception {
        mockMVC.perform(get(invalidKeytypeEndpoint(useSet))).andExpect(status().isBadRequest());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void GetWithInvalidIntegerKey(boolean useSet) throws Exception {
        mockMVC.perform(get(invalidIntegerEndpoint(useSet))).andExpect(status().isBadRequest());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void GetWithInvalidBytesKey(boolean useSet) throws Exception {
        mockMVC.perform(get(invalidBytesEndpoint(useSet)))
                .andExpect(status().isBadRequest());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void GetWithInvalidDigestKey(boolean useSet) throws Exception {
        mockMVC.perform(get(invalidDigestEndpoint(useSet))).andExpect(status().isBadRequest());
    }
}
