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
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.restclient.domain.RestClientKeyRecord;
import com.aerospike.restclient.domain.scanmodels.RestClientScanResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
public class ScanTest {

    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    private static final int numberOfRecords = 101;
    private static final String namespace = "test";
    private static final String setName = "scanSet";

    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JSONResponseDeserializer(), MediaType.APPLICATION_JSON.toString()),
                Arguments.of(new MsgPackResponseDeserializer(), "application/msgpack")
        );
    }

    private static Key[] testKeys() {
        Key[] keys = new Key[numberOfRecords];
        for (int i = 0; i < numberOfRecords; i++) {
            keys[i] = new Key(namespace, setName, "key_" + i);
        }
        return keys;
    }

    private static String testEndpoint() {
        return "/v1/scan/" + namespace + "/" + setName;
    }

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testScanAll(ResponseDeserializer responseDeserializer, String currentMediaType) throws Exception {
        Key[] keys = testKeys();
        try {
            WritePolicy writePolicy = new WritePolicy();
            writePolicy.sendKey = true;
            for (int i = 0; i < numberOfRecords; i++) {
                Bin bin = new Bin("binInt", i);
                client.add(writePolicy, keys[i], bin);
            }

            MockHttpServletResponse response = mockMVC.perform(get(testEndpoint()).accept(currentMediaType))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse();

            RestClientScanResponse res = responseDeserializer.getResponse(response, RestClientScanResponse.class);
            assertEquals(res.getPagination().getTotalRecords(), numberOfRecords);
        } finally {
            for (Key key : keys) {
                client.delete(null, key);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testScanPaginated(ResponseDeserializer responseDeserializer, String currentMediaType) throws Exception {
        Key[] keys = testKeys();
        try {
            WritePolicy writePolicy = new WritePolicy();
            writePolicy.sendKey = true;
            for (int i = 0; i < numberOfRecords; i++) {
                Bin bin = new Bin("binInt", i);
                client.add(writePolicy, keys[i], bin);
            }

            int pageSize = 10;
            int scanRequests = 0;
            int total = 0;
            RestClientScanResponse res = null;
            Set<Integer> binValues = new HashSet<>();
            String endpoint = testEndpoint() + "?maxRecords=" + pageSize;
            String baseEndpoint = endpoint;
            while (total < numberOfRecords) {
                MockHttpServletResponse response = mockMVC.perform(get(endpoint).accept(currentMediaType))
                        .andExpect(status().isOk())
                        .andReturn()
                        .getResponse();

                res = responseDeserializer.getResponse(response, RestClientScanResponse.class);
                for (RestClientKeyRecord r : res.getRecords()) {
                    binValues.add((int) r.bins.get("binInt"));
                }
                total += res.getPagination().getTotalRecords();
                scanRequests++;
                endpoint = baseEndpoint + "&from=" + res.getPagination().getNextToken();
            }

            assertEquals(total, numberOfRecords);
            assertEquals(binValues.size(), numberOfRecords);
            assertEquals(scanRequests, numberOfRecords / pageSize + 1);
            assertNull(res.getPagination().getNextToken());
        } finally {
            for (Key key : keys) {
                client.delete(null, key);
            }
        }
    }
}

interface ResponseDeserializer {
    <T> T getResponse(MockHttpServletResponse res, Class<T> clazz);
}

class MsgPackResponseDeserializer implements ResponseDeserializer {

    ObjectMapper msgPackMapper;

    public MsgPackResponseDeserializer() {
        msgPackMapper = new ObjectMapper(new MessagePackFactory());
    }

    @Override
    public <T> T getResponse(MockHttpServletResponse res, Class<T> clazz) {
        T response = null;
        try {
            byte[] responseAsByteArray = res.getContentAsByteArray();
            response = msgPackMapper.readValue(responseAsByteArray, clazz);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }
}

class JSONResponseDeserializer implements ResponseDeserializer {

    ObjectMapper mapper;

    public JSONResponseDeserializer() {
        this.mapper = new ObjectMapper();
    }

    @Override
    public <T> T getResponse(MockHttpServletResponse res, Class<T> clazz) {
        T response = null;
        try {
            String responseAsString = res.getContentAsString();
            response = mapper.readValue(responseAsString, clazz);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }
}
