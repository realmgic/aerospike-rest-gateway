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
import com.aerospike.client.query.Filter;
import com.aerospike.restclient.config.JSONMessageConverter;
import com.aerospike.restclient.config.MsgPackConverter;
import com.aerospike.restclient.domain.RestClientError;
import com.aerospike.restclient.domain.querymodels.QueryEqualLongFilter;
import com.aerospike.restclient.domain.querymodels.QueryFilter;
import com.aerospike.restclient.domain.querymodels.QueryRequestBody;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
public class QueryErrorTest {

    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    private static final String namespace = "test";
    private static final String setName = "queryError";

    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JSONQueryErrorHandler(), true),
                Arguments.of(new JSONQueryErrorHandler(), false),
                Arguments.of(new MsgPackQueryErrorHandler(), true),
                Arguments.of(new MsgPackQueryErrorHandler(), false)
        );
    }

    private static String testEndpoint(boolean useSet) {
        return useSet ? String.format("/v1/query/%s/%s", namespace, setName) : String.format("/v1/query/%s", namespace);
    }

    @BeforeEach
    public void setup() throws InterruptedException {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();
        Bin bin = new Bin("binWithNoSIndex", 1);
        client.put(null, new Key(namespace, setName, "key_" + 1), bin);
    }

    @AfterEach
    public void clean() throws InterruptedException {
        client.delete(null, new Key(namespace, setName, "key_" + 1));
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testNonExistentPath(QueryErrorHandler queryHandler, boolean useSet) throws Exception {
        String endpoint = testEndpoint(useSet).replace(namespace, "nonExistent");
        RestClientError res = queryHandler.perform(mockMVC, endpoint, new QueryRequestBody(), status().isNotFound());
        assertFalse(res.getInDoubt());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testInvalidStartAndCount(QueryErrorHandler queryHandler, boolean useSet) throws Exception {
        String endpoint = String.join("/", testEndpoint(useSet), "200", "90832");
        RestClientError res = queryHandler.perform(mockMVC, endpoint, new QueryRequestBody(), status().isBadRequest());
        assertFalse(res.getInDoubt());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testInvalidFilterType(QueryErrorHandler queryHandler, boolean useSet) throws Exception {
        class unknownFilter extends QueryFilter {
            final String type = "UNKNOWN_FILTER_TYPE";

            @Override
            public Filter toFilter() {
                return null;
            }
        }

        QueryRequestBody requestBody = new QueryRequestBody();
        requestBody.filter = new unknownFilter();
        RestClientError res = queryHandler.perform(mockMVC, testEndpoint(useSet), requestBody, status().isBadRequest());
        assertFalse(res.getInDoubt());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testFilterWithNoSIndex(QueryErrorHandler queryHandler, boolean useSet) throws Exception {
        QueryRequestBody requestBody = new QueryRequestBody();
        QueryEqualLongFilter filter = new QueryEqualLongFilter();
        filter.value = 6L;
        filter.binName = "binWithNoSIndex";
        requestBody.filter = filter;
        RestClientError res = queryHandler.perform(mockMVC, testEndpoint(useSet), requestBody, status().isNotFound());
        assertFalse(res.getInDoubt());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testFilterNonExistentBin(QueryErrorHandler queryHandler, boolean useSet) throws Exception {
        QueryRequestBody requestBody = new QueryRequestBody();
        QueryEqualLongFilter filter = new QueryEqualLongFilter();
        filter.value = 6L;
        filter.binName = "nonExistentBin";
        requestBody.filter = filter;
        // Not found because sindex is checked first
        RestClientError res = queryHandler.perform(mockMVC, testEndpoint(useSet), requestBody, status().isNotFound());
        assertFalse(res.getInDoubt());
    }
}

/*
 * The handler interface performs a query request via a json string, and returns a List<Map<String, Object>>
 * Implementations are provided for specifying JSON and MsgPack as return formats
 */
interface QueryErrorHandler {
    RestClientError perform(MockMvc mockMVC, String testEndpoint, QueryRequestBody payload,
                            ResultMatcher matcher) throws Exception;
}

class MsgPackQueryErrorHandler implements QueryErrorHandler {

    ASTestMapper msgPackMapper;

    public MsgPackQueryErrorHandler() {
        msgPackMapper = new ASTestMapper(MsgPackConverter.getASMsgPackObjectMapper(), RestClientError.class);
    }

    private RestClientError getQueryResponse(byte[] response) {
        RestClientError queryResponse = null;
        try {
            queryResponse = (RestClientError) msgPackMapper.bytesToObject(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queryResponse;
    }

    @Override
    public RestClientError perform(MockMvc mockMVC, String testEndpoint, QueryRequestBody payload,
                                   ResultMatcher matcher) throws Exception {

        byte[] response = ASTestUtils.performOperationAndExpect(mockMVC, testEndpoint,
                msgPackMapper.objectToBytes(payload), matcher);

        return getQueryResponse(response);
    }

}

class JSONQueryErrorHandler implements QueryErrorHandler {

    ASTestMapper msgPackMapper;

    public JSONQueryErrorHandler() {
        msgPackMapper = new ASTestMapper(JSONMessageConverter.getJSONObjectMapper(), RestClientError.class);
    }

    private RestClientError getQueryResponse(byte[] response) {
        RestClientError queryResponse = null;
        try {
            queryResponse = (RestClientError) msgPackMapper.bytesToObject(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queryResponse;
    }

    @Override
    public RestClientError perform(MockMvc mockMVC, String testEndpoint, QueryRequestBody payload,
                                   ResultMatcher matcher) throws Exception {
        byte[] response = ASTestUtils.performOperationAndExpect(mockMVC, testEndpoint,
                        new String(msgPackMapper.objectToBytes(payload), StandardCharsets.UTF_8), matcher)
                .getBytes(StandardCharsets.UTF_8);
        return getQueryResponse(response);
    }
}
