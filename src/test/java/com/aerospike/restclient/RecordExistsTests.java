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
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.Base64;
import java.util.stream.Stream;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
public class RecordExistsTests {

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    private MockMvc mockMVC;

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

    private static String nonExistentEndpoint(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "junit", "notreal") : ASTestUtils.buildEndpointV1("kvs", "test", "notreal");
    }

    private static String digestEndpoint(boolean useSet) {
        Key tk = keyFor(useSet);
        String keyDigest = Base64.getUrlEncoder().encodeToString(tk.digest);
        if (useSet) {
            return ASTestUtils.buildEndpointV1("kvs", tk.namespace, tk.setName, keyDigest) + "?keytype=DIGEST";
        } else {
            return ASTestUtils.buildEndpointV1("kvs", tk.namespace, keyDigest) + "?keytype=DIGEST";
        }
    }

    private static String intEndpoint(boolean useSet) {
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "junit", "1") + "?keytype=INTEGER" : ASTestUtils.buildEndpointV1("kvs", "test", "1") + "?keytype=INTEGER";
    }

    private static String bytesEndpoint(boolean useSet) {
        Key bk = bytesKeyFor(useSet);
        String urlBytes = Base64.getUrlEncoder().encodeToString((byte[]) bk.userKey.getObject());
        return useSet ? ASTestUtils.buildEndpointV1("kvs", "test", "junit", urlBytes) + "?keytype=BYTES" : ASTestUtils.buildEndpointV1("kvs", "test", urlBytes) + "?keytype=BYTES";
    }

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testStringKeyExists(boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        try {
            client.put(null, testKey, new Bin("a", "b"));
            mockMVC.perform(head(testEndpoint(useSet))).andExpect(status().isOk());
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testWithNonExistentRecord(boolean useSet) throws Exception {
        mockMVC.perform(head(nonExistentEndpoint(useSet))).andExpect(status().isNotFound());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void TestIntegerKey(boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        Key intKey = intKeyFor(useSet);
        try {
            client.put(null, testKey, new Bin("a", "b"));
            Bin idBin = new Bin("id", "integer");
            client.put(null, intKey, idBin);
            mockMVC.perform(head(intEndpoint(useSet))).andExpect(status().isOk());
        } finally {
            client.delete(null, testKey);
            client.delete(null, intKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void ExistsWithDigest(boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        try {
            client.put(null, testKey, new Bin("a", "b"));
            Bin intBin = new Bin("integer", 10);
            client.put(null, testKey, intBin);
            mockMVC.perform(head(digestEndpoint(useSet))).andExpect(status().isOk());
        } finally {
            client.delete(null, testKey);
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void TestBytesKey(boolean useSet) throws Exception {
        Key testKey = keyFor(useSet);
        Key bytesKey = bytesKeyFor(useSet);
        try {
            client.put(null, testKey, new Bin("a", "b"));
            Bin idBin = new Bin("id", "bytes");
            client.put(null, bytesKey, idBin);
            mockMVC.perform(head(bytesEndpoint(useSet))).andExpect(status().isOk());
        } finally {
            client.delete(null, testKey);
            client.delete(null, bytesKey);
        }
    }

}
