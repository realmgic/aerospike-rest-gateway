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
import com.aerospike.client.Value.BytesValue;
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

import java.util.Base64;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
public class RecordDeleteCorrectTests {

    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    static Stream<Arguments> getParams() {
        return Stream.of(Arguments.of(true), Arguments.of(false));
    }

    private static Key keyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", "getput") : new Key("test", null, "getput");
    }

    private static Key intKeyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", 1) : new Key("test", null, 1);
    }

    private static Key bytesKeyFor(boolean useSet) {
        return useSet ? new Key("test", "junit", new byte[]{1, 127, 127, 1}) : new Key("test", null, new byte[]{1, 127, 127, 1});
    }

    private static String testEndpoint(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "junit", "getput") : ASTestUtils.buildEndpointV1("kvs", "test", "getput");
    }

    private static String intEndpoint(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "junit", "1") + "?keytype=INTEGER" : ASTestUtils.buildEndpointV1("kvs", "test", "1") + "?keytype=INTEGER";
    }

    private static String bytesEndpoint(boolean useSet) {
        Key bk = bytesKeyFor(useSet);
        String urlBytes = Base64.getUrlEncoder().encodeToString((byte[]) ((BytesValue) bk.userKey).getObject());
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "junit", urlBytes) + "?keytype=BYTES" : ASTestUtils.buildEndpointV1("kvs", "test", urlBytes) + "?keytype=BYTES";
    }

    private static String digestEndpoint(boolean useSet) {
        Key tk = keyFor(useSet);
        String digestBytes = Base64.getUrlEncoder().encodeToString(tk.digest);
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "junit", digestBytes) + "?keytype=DIGEST" : ASTestUtils.buildEndpointV1("kvs", "test", digestBytes) + "?keytype=DIGEST";
    }

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void DeleteRecord(boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        try {
            Bin baseBin = new Bin("initial", "bin");
            client.put(null, testKey, baseBin);
            client.put(null, intKey, baseBin);
            client.put(null, bytesKey, baseBin);

            mockMVC.perform(delete(testEndpoint(useSet)).contentType(MediaType.APPLICATION_JSON)).andExpect(status().isNoContent());

            Record record = client.get(null, testKey);
            assertNull(record);
        } finally {
            client.delete(null, testKey);
            client.delete(null, intKey);
            client.delete(null, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void DeleteRecordWithIntKey(boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        try {
            Bin baseBin = new Bin("initial", "bin");
            client.put(null, testKey, baseBin);
            client.put(null, intKey, baseBin);
            client.put(null, bytesKey, baseBin);

            mockMVC.perform(delete(intEndpoint(useSet)).contentType(MediaType.APPLICATION_JSON)).andExpect(status().isNoContent());

            Record record = client.get(null, intKey);
            assertNull(record);
        } finally {
            client.delete(null, testKey);
            client.delete(null, intKey);
            client.delete(null, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void DeleteRecordWithBytesKey(boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        try {
            Bin baseBin = new Bin("initial", "bin");
            client.put(null, testKey, baseBin);
            client.put(null, intKey, baseBin);
            client.put(null, bytesKey, baseBin);

            mockMVC.perform(delete(bytesEndpoint(useSet)).contentType(MediaType.APPLICATION_JSON))
                    .andExpect(status().isNoContent());

            Record record = client.get(null, bytesKey);
            assertNull(record);
        } finally {
            client.delete(null, testKey);
            client.delete(null, intKey);
            client.delete(null, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void DeleteRecordWithDigestKey(boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        try {
            Bin baseBin = new Bin("initial", "bin");
            client.put(null, testKey, baseBin);
            client.put(null, intKey, baseBin);
            client.put(null, bytesKey, baseBin);

            mockMVC.perform(delete(digestEndpoint(useSet)).contentType(MediaType.APPLICATION_JSON))
                    .andExpect(status().isNoContent());

            Record record = client.get(null, testKey);
            assertNull(record);
        } finally {
            client.delete(null, testKey);
            client.delete(null, intKey);
            client.delete(null, bytesKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void DeleteRecordWithGenerationMismatch(boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        Key intKey = intKeyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        try {
            Bin baseBin = new Bin("initial", "bin");
            client.put(null, testKey, baseBin);
            client.put(null, intKey, baseBin);
            client.put(null, bytesKey, baseBin);

            String extraParams = "?generation=150&generationPolicy=EXPECT_GEN_EQUAL";
            mockMVC.perform(delete(testEndpoint(useSet) + extraParams).contentType(MediaType.APPLICATION_JSON))
                    .andExpect(status().isConflict());

            Record record = client.get(null, testKey);
            assertNotNull(record);
        } finally {
            client.delete(null, testKey);
            client.delete(null, intKey);
            client.delete(null, bytesKey);
        }
    }
}
