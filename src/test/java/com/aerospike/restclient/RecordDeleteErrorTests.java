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
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
public class RecordDeleteErrorTests {

    @Autowired
    private ObjectMapper objectMapper;

    private MockMvc mockMVC;

    @Autowired
    private WebApplicationContext wac;

    static Stream<Arguments> getParams() {
        return Stream.of(Arguments.of(true), Arguments.of(false));
    }

    private static String nonExistentNSEndpoint(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "fakeNS", "demo", "1") : ASTestUtils.buildEndpointV1("kvs", "fakeNS", "1");
    }

    private static String nonExistentRecordEndpoint(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "demo", "thisisnotarealkeyforarecord") : ASTestUtils.buildEndpointV1("kvs", "test", "thisisnotarealkeyforarecord");
    }

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void DeleteFromNonExistentNS(boolean useSet) throws Exception {
        MvcResult result = mockMVC.perform(delete(nonExistentNSEndpoint(useSet)).accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound())
                .andReturn();

        MockHttpServletResponse res = result.getResponse();
        String resJson = res.getContentAsString();
        TypeReference<Map<String, Object>> sOMapType = new TypeReference<Map<String, Object>>() {
        };
        Map<String, Object> resObject = objectMapper.readValue(resJson, sOMapType);

        assertFalse((boolean) resObject.get("inDoubt"));
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void DeleteNonExistentRecord(boolean useSet) throws Exception {
        MvcResult result = mockMVC.perform(delete(nonExistentRecordEndpoint(useSet)).accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound())
                .andReturn();

        MockHttpServletResponse res = result.getResponse();
        String resJson = res.getContentAsString();
        TypeReference<Map<String, Object>> sOMapType = new TypeReference<Map<String, Object>>() {
        };
        Map<String, Object> resObject = objectMapper.readValue(resJson, sOMapType);

        assertFalse((boolean) resObject.get("inDoubt"));
    }

}
