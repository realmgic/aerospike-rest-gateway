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
import com.aerospike.restclient.domain.executemodels.RestClientExecuteTask;
import com.aerospike.restclient.domain.executemodels.RestClientExecuteTaskStatus;
import com.aerospike.restclient.util.AerospikeOperation;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
public class ExecuteV2Tests {

    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    private static final String namespace = "test";
    private static final int numberOfRecords = 10;

    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new ObjectMapper(), new JSONResponseDeserializer(), MediaType.APPLICATION_JSON.toString(), true),
                Arguments.of(new ObjectMapper(new MessagePackFactory()), new MsgPackResponseDeserializer(), "application/msgpack", true),
                Arguments.of(new ObjectMapper(), new JSONResponseDeserializer(), MediaType.APPLICATION_JSON.toString(), false),
                Arguments.of(new ObjectMapper(new MessagePackFactory()), new MsgPackResponseDeserializer(), "application/msgpack", false)
        );
    }

    private static Key[] setKeys(boolean useSet) {
        String setName = useSet ? "executeSet" : null;
        Key[] keys = new Key[numberOfRecords];
        for (int i = 0; i < numberOfRecords; i++) {
            keys[i] = new Key(namespace, setName, "exec_" + i);
        }
        return keys;
    }

    private static String testEndpoint(boolean useSet) {
        return useSet ? "/v2/execute/scan/" + namespace + "/executeSet" : "/v2/execute/scan/" + namespace;
    }

    private static final String queryStatusEndpoint = "/v2/execute/scan/status/";

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testExecuteScan(ObjectMapper objectMapper, ResponseDeserializer responseDeserializer, String currentMediaType, boolean useSet) throws Exception {
        Key[] testKeys = setKeys(useSet);
        String testEndpoint = testEndpoint(useSet);
        try {
            for (int i = 0; i < testKeys.length; i++) {
                Bin bin = new Bin("binInt", i);
                client.add(null, testKeys[i], bin);
            }

            Map<String, Object> opRequest = new HashMap<>();
            List<Map<String, Object>> opList = new ArrayList<>();
            Map<String, Object> opMap = new HashMap<>();

            opMap.put("binName", "binInt");
            opMap.put("incr", 1);
            String OPERATION_FIELD_TYPE = "type";
            opMap.put(OPERATION_FIELD_TYPE, AerospikeOperation.ADD);
            opList.add(opMap);
            opRequest.put("opsList", opList);

            byte[] payload = objectMapper.writeValueAsBytes(opRequest);
            MockHttpServletResponse response = mockMVC.perform(
                            post(testEndpoint).contentType(currentMediaType).content(payload).accept(currentMediaType))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse();

            RestClientExecuteTask task = responseDeserializer.getResponse(response, RestClientExecuteTask.class);

            Thread.sleep(200);
            String endpoint = queryStatusEndpoint + task.getTaskId();
            MockHttpServletResponse statusResponse = mockMVC.perform(get(endpoint).accept(currentMediaType))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse();

            RestClientExecuteTaskStatus status = responseDeserializer.getResponse(statusResponse,
                    RestClientExecuteTaskStatus.class);

            assertEquals("COMPLETE", status.getStatus());

            for (int i = 0; i < numberOfRecords; i++) {
                long binValue = (long) client.get(null, testKeys[i]).bins.get("binInt");
                assertEquals(binValue, i + 1);
            }
        } finally {
            for (Key testKey : testKeys) {
                client.delete(null, testKey);
            }
        }
    }
}
