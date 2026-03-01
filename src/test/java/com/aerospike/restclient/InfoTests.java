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
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
public class InfoTests {

    public static TypeReference<Map<String, String>> infoResponseType = new TypeReference<Map<String, String>>() {
    };

    @Autowired
    private AerospikeClient client;

    @Autowired
    WebApplicationContext wac;

    private MockMvc mockMVC = null;

    private static final String endpoint = "/v1/info";

    private Node testNode;

    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JSONInfoPerformer()),
                Arguments.of(new MsgPackInfoPerformer())
        );
    }

    @BeforeEach
    public void setup() {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
        testNode = client.getNodes()[0];
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testSingleInfoCommand(InfoPerformer performer) throws Exception {
        String command = "edition";
        List<String> commands = Arrays.asList(command);
        Map<String, String> responses = performer.performInfoAndReturn(endpoint, commands, mockMVC);

        assertTrue(responses.containsKey(command));
        String restClientResponse = responses.get(command);

        String clientResponse = Info.request(null, testNode, command);

        assertEquals(restClientResponse, clientResponse);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testMultipleInfoCommands(InfoPerformer performer) throws Exception {
        String command1 = "edition";
        String command2 = "build";
        List<String> commands = Arrays.asList(command1, command2);
        Map<String, String> responses = performer.performInfoAndReturn(endpoint, commands, mockMVC);

        assertTrue(responses.containsKey(command1));
        assertTrue(responses.containsKey(command2));

        Map<String, String> clientResponses = Info.request(null, testNode, command1, command2);

        assertTrue(ASTestUtils.compareStringMap(responses, clientResponses));
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testNonExistentInfoCommand(InfoPerformer performer) throws Exception {
        String realCommand = "edition";
        String fakeCommand = "asdfasdfasdf";
        List<String> commands = Arrays.asList(realCommand, fakeCommand);
        Map<String, String> responses = performer.performInfoAndReturn(endpoint, commands, mockMVC);

        // System.out.println("responses: " + responses);
        assertTrue(responses.containsKey(realCommand));
        // fakecommand may be absent from response or present with ERROR; both are acceptable
        if (responses.containsKey(fakeCommand)) {
            assertTrue(responses.get(fakeCommand).contains("ERROR"),
                    "Fake command response should contain 'ERROR': " + responses.get(fakeCommand));
        }

        Map<String, String> clientResponses = Info.request(null, testNode, realCommand, fakeCommand);

        assertTrue(ASTestUtils.compareStringMap(responses, clientResponses), 
            "Responses should be equal: " + responses + " vs " + clientResponses);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testSendCommandToSingleNode(InfoPerformer performer) throws Exception {
        String nameCommand = "name";
        String nodeName = testNode.getName();
        List<String> commands = Arrays.asList(nameCommand);
        String singleNodeEndpoint = endpoint + "/" + nodeName;
        Map<String, String> responses = performer.performInfoAndReturn(singleNodeEndpoint, commands, mockMVC);
        String rcNodeName = responses.get(nameCommand);

        String clientNodeName = Info.request(null, testNode, nameCommand);

        assertEquals(rcNodeName, clientNodeName);
    }

}

interface InfoPerformer {
    Map<String, String> performInfoAndReturn(String endpoint, List<String> commands, MockMvc mockMVC) throws Exception;
}

class JSONInfoPerformer implements InfoPerformer {
    private final ObjectMapper mapper;

    public JSONInfoPerformer() {
        mapper = new ObjectMapper();
    }

    @Override
    public Map<String, String> performInfoAndReturn(String endpoint, List<String> commands,
                                                    MockMvc mockMVC) throws Exception {
        String jsonCommands = mapper.writeValueAsString(commands);
        String results = mockMVC.perform(post(endpoint).accept(MediaType.APPLICATION_JSON)
                .contentType(MediaType.APPLICATION_JSON)
                .content(jsonCommands)).andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        return mapper.readValue(results, InfoTests.infoResponseType);
    }
}

class MsgPackInfoPerformer implements InfoPerformer {
    private final ObjectMapper mapper;
    private final MediaType mediaType = new MediaType("application", "msgpack");

    public MsgPackInfoPerformer() {
        mapper = new ObjectMapper(new MessagePackFactory());
    }

    @Override
    public Map<String, String> performInfoAndReturn(String endpoint, List<String> commands,
                                                    MockMvc mockMVC) throws Exception {
        byte[] jsonCommands = mapper.writeValueAsBytes(commands);
        byte[] results = mockMVC.perform(post(endpoint).accept(mediaType).contentType(mediaType).content(jsonCommands))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsByteArray();
        return mapper.readValue(results, InfoTests.infoResponseType);
    }
}
