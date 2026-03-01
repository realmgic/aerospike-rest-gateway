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
import com.aerospike.client.Value;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.restclient.config.JSONMessageConverter;
import com.aerospike.restclient.config.MsgPackConverter;
import com.aerospike.restclient.domain.RestClientKeyRecord;
import com.aerospike.restclient.domain.geojsonmodels.LngLat;
import com.aerospike.restclient.domain.querymodels.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

@SpringBootTest
public class QueryCorrectTest {

    private MockMvc mockMVC;

    @Autowired
    private AerospikeClient client;

    @Autowired
    private WebApplicationContext wac;

    private static final int numberOfRecords = 501;
    private static final String namespace = "test";
    private static final String setName = "queryset";
    private static final Key[] testKeys = new Key[numberOfRecords];

    static {
        for (int i = 0; i < numberOfRecords; i++) {
            testKeys[i] = new Key(namespace, setName, "key_" + i);
        }
    }

    private static boolean first = true;

    public static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JSONQueryHandler(), true),
                Arguments.of(new JSONQueryHandler(), false),
                Arguments.of(new MsgPackQueryHandler(), true),
                Arguments.of(new MsgPackQueryHandler(), false)
        );
    }

    private static String testEndpointFor(boolean useSet) {
        return useSet ? "/v1/query/" + namespace + "/" + setName : "/v1/query/" + namespace;
    }

    @BeforeEach
    public void setup() throws InterruptedException {
        mockMVC = MockMvcBuilders.webAppContextSetup(wac).build();

        if (first) {
            first = false;
            client.truncate(null, "test", null, null);
            WritePolicy writePolicy = new WritePolicy();
            writePolicy.sendKey = true;
            writePolicy.totalTimeout = 0;

            try {
                client.dropIndex(writePolicy, namespace, setName, "binInt-set-index").waitTillComplete();
                client.dropIndex(writePolicy, namespace, setName, "binIntMod-set-index").waitTillComplete();
                client.dropIndex(writePolicy, namespace, setName, "binStr-set-index").waitTillComplete();
                client.dropIndex(writePolicy, namespace, setName, "binGeo-set-index").waitTillComplete();
                client.dropIndex(writePolicy, namespace, null, "binInt-index").waitTillComplete();
                client.dropIndex(writePolicy, namespace, null, "binStr-index").waitTillComplete();
                client.dropIndex(writePolicy, namespace, null, "binIntMod-index").waitTillComplete();
                client.dropIndex(writePolicy, namespace, null, "binGeo-index").waitTillComplete();
            } catch (Exception ignored) {
            }

            client.createIndex(writePolicy, namespace, setName, "binInt-set-index", "binInt", IndexType.NUMERIC)
                    .waitTillComplete();
            client.createIndex(writePolicy, namespace, setName, "binIntMod-set-index", "binIntMod", IndexType.NUMERIC)
                    .waitTillComplete();
            client.createIndex(writePolicy, namespace, setName, "binStr-set-index", "binStr", IndexType.STRING)
                    .waitTillComplete();
            client.createIndex(writePolicy, namespace, setName, "binGeo-set-index", "binGeo", IndexType.GEO2DSPHERE)
                    .waitTillComplete();
            client.createIndex(writePolicy, namespace, null, "binInt-index", "binInt", IndexType.NUMERIC)
                    .waitTillComplete();
            client.createIndex(writePolicy, namespace, null, "binIntMod-index", "binIntMod", IndexType.NUMERIC)
                    .waitTillComplete();
            client.createIndex(writePolicy, namespace, null, "binStr-index", "binStr", IndexType.STRING)
                    .waitTillComplete();
            client.createIndex(writePolicy, namespace, null, "binGeo-index", "binGeo", IndexType.GEO2DSPHERE)
                    .waitTillComplete();

            for (int i = 0; i < numberOfRecords; i++) {
                Bin intBin = new Bin("binInt", i);
                Bin intModBin = new Bin("binIntMod", i % 3);
                Bin strBin = new Bin("binStr", Integer.toString(i));
                client.put(writePolicy, testKeys[i], intBin, intModBin, strBin);
            }

            for (int i = 0; i < 5; i++) {
                Bin geoBin = new Bin("binGeo", new Value.GeoJSONValue(
                        "{\"type\": \"Polygon\", \"coordinates\": [[[0,0], [0, 10], [10, 10], [10, 0], [0,0]]]}"));
                client.put(writePolicy, testKeys[i], geoBin);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testPIQueryAllPartitions(QueryHandler queryHandler, boolean useSet) throws Exception {
        String testEndpoint = testEndpointFor(useSet);
        QueryResponseBody res = queryHandler.perform(mockMVC, testEndpoint, new QueryRequestBody());
        Assertions.assertEquals(numberOfRecords, res.getPagination().getTotalRecords());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testPIQueryPartitionRange(QueryHandler queryHandler, boolean useSet) throws Exception {
        String testEndpoint = testEndpointFor(useSet);
        int startPartitions = 100;
        int partitionCount = 2048;
        String endpoint = String.join("/", testEndpoint, String.valueOf(startPartitions),
                String.valueOf(partitionCount));
        QueryResponseBody res = queryHandler.perform(mockMVC, endpoint, new QueryRequestBody());
        Assertions.assertTrue(res.getPagination().getTotalRecords() < (numberOfRecords / 2) + 50);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testPIQueryAllPaginated(QueryHandler queryHandler, boolean useSet) throws Exception {
        String testEndpoint = testEndpointFor(useSet);
        int pageSize = 100;
        int queryRequests = 0;
        int total = 0;
        QueryResponseBody res = null;
        Set<Integer> binValues = new HashSet<>();
        String endpoint = testEndpoint + "?maxRecords=" + pageSize + "&getToken=True";
        String fromToken = null;

        while (total < numberOfRecords) {
            QueryRequestBody requestBody = new QueryRequestBody();
            requestBody.from = fromToken;
            res = queryHandler.perform(mockMVC, endpoint, requestBody);
            for (RestClientKeyRecord r : res.getRecords()) {
                binValues.add((int) r.bins.get("binInt"));
            }
            total += res.getPagination().getTotalRecords();
            queryRequests++;
            fromToken = res.getPagination().getNextToken();
        }

        Assertions.assertEquals(numberOfRecords, binValues.size());
        Assertions.assertEquals(numberOfRecords, total);
        Assertions.assertEquals(numberOfRecords / pageSize + 1, queryRequests);
        Assertions.assertNull(res.getPagination().getNextToken());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testPIQueryPartitionRangePaginated(QueryHandler queryHandler, boolean useSet) throws Exception {
        String testEndpoint = testEndpointFor(useSet);
        int startPartitions = 100;
        int partitionCount = 2048;
        int pageSize = 100;
        int queryRequests = 0;
        int total = 0;
        QueryResponseBody res;
        Set<Integer> binValues = new HashSet<>();
        String endpoint = String.join("/", testEndpoint, String.valueOf(startPartitions),
                String.valueOf(partitionCount) + "?maxRecords=" + pageSize + "&getToken=True");
        String fromToken = null;

        do {
            QueryRequestBody requestBody = new QueryRequestBody();
            requestBody.from = fromToken;
            res = queryHandler.perform(mockMVC, endpoint, requestBody);
            for (RestClientKeyRecord r : res.getRecords()) {
                binValues.add((int) r.bins.get("binInt"));
            }
            total += res.getPagination().getTotalRecords();
            queryRequests++;
            fromToken = res.getPagination().getNextToken();
        } while (fromToken != null);

        Assertions.assertEquals((int) Math.ceil((double) (numberOfRecords / 2) / pageSize), queryRequests);
        Assertions.assertNull(res.getPagination().getNextToken());
        Assertions.assertTrue(total < (numberOfRecords / 2) + 100);  // estimate of number of records
        Assertions.assertTrue(binValues.size() < (numberOfRecords / 2) + 100); // estimate of number of records
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testSIQueryAllEqualFilteredPaginated(QueryHandler queryHandler, boolean useSet) throws Exception {
        String testEndpoint = testEndpointFor(useSet);
        int pageSize = 100;
        int queryRequests = 0;
        int total = 0;
        QueryResponseBody res;
        Set<String> keyValues = new HashSet<>();
        String endpoint = testEndpoint + "?maxRecords=" + pageSize + "&getToken=True";
        String fromToken = null;

        do {
            QueryRequestBody requestBody = new QueryRequestBody();
            QueryEqualLongFilter filter = new QueryEqualLongFilter();
            filter.binName = "binIntMod";
            filter.value = 2L;
            requestBody.filter = filter;
            requestBody.from = fromToken;
            res = queryHandler.perform(mockMVC, endpoint, requestBody);
            for (RestClientKeyRecord r : res.getRecords()) {
                keyValues.add((String) r.userKey);
            }
            total += res.getPagination().getTotalRecords();
            queryRequests++;
            fromToken = res.getPagination().getNextToken();
        } while (fromToken != null);

        Assertions.assertEquals((int) Math.ceil((double) numberOfRecords / 3 / pageSize), queryRequests);
        Assertions.assertNull(res.getPagination().getNextToken());
        Assertions.assertEquals((numberOfRecords / 3), total);  // estimate of number of records
        Assertions.assertEquals(total, keyValues.size()); // estimate of number of records
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testSIQueryAllIntEqualFiltered(QueryHandler queryHandler, boolean useSet) throws Exception {
        String testEndpoint = testEndpointFor(useSet);
        QueryRequestBody queryBody = new QueryRequestBody();
        QueryEqualLongFilter filter = new QueryEqualLongFilter();
        filter.binName = "binInt";
        filter.value = 100L;
        queryBody.filter = filter;
        QueryResponseBody res = queryHandler.perform(mockMVC, testEndpoint, queryBody);
        Assertions.assertEquals(1, res.getPagination().getTotalRecords());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testSIQueryAllStringEqualFiltered(QueryHandler queryHandler, boolean useSet) throws Exception {
        String testEndpoint = testEndpointFor(useSet);
        QueryRequestBody queryBody = new QueryRequestBody();
        QueryEqualsStringFilter filter = new QueryEqualsStringFilter();
        filter.binName = "binStr";
        filter.value = "100";
        queryBody.filter = filter;
        QueryResponseBody res = queryHandler.perform(mockMVC, testEndpoint, queryBody);
        Assertions.assertEquals(1, res.getPagination().getTotalRecords());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testSIQueryAllGeoContainsFiltered(QueryHandler queryHandler, boolean useSet) throws Exception {
        String testEndpoint = testEndpointFor(useSet);
        QueryRequestBody queryBody = new QueryRequestBody();
        QueryGeoContainsPointFilter filter = new QueryGeoContainsPointFilter();
        filter.binName = "binGeo";
        filter.point = new LngLat(1, 1);
        queryBody.filter = filter;
        QueryResponseBody res = queryHandler.perform(mockMVC, testEndpoint, queryBody);
        Assertions.assertEquals(5, res.getPagination().getTotalRecords());
    }
}

/*
 * The handler interface performs a query request via a json string, and returns a List<Map<String, Object>>
 * Implementations are provided for specifying JSON and MsgPack as return formats
 */
interface QueryHandler {
    QueryResponseBody perform(MockMvc mockMVC, String testEndpoint, QueryRequestBody payload) throws Exception;
}

class MsgPackQueryHandler implements QueryHandler {

    ASTestMapper msgPackMapper;

    public MsgPackQueryHandler() {
        msgPackMapper = new ASTestMapper(MsgPackConverter.getASMsgPackObjectMapper(), QueryResponseBody.class);
    }

    private QueryResponseBody getQueryResponse(byte[] response) {
        QueryResponseBody queryResponse = null;
        try {
            queryResponse = (QueryResponseBody) msgPackMapper.bytesToObject(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queryResponse;
    }

    @Override
    public QueryResponseBody perform(MockMvc mockMVC, String testEndpoint, QueryRequestBody payload) throws Exception {

        byte[] response = ASTestUtils.performOperationAndReturn(mockMVC, testEndpoint,
                msgPackMapper.objectToBytes(payload));

        return getQueryResponse(response);
    }

}

class JSONQueryHandler implements QueryHandler {

    ASTestMapper msgPackMapper;

    public JSONQueryHandler() {
        msgPackMapper = new ASTestMapper(JSONMessageConverter.getJSONObjectMapper(), QueryResponseBody.class);
    }

    private QueryResponseBody getQueryResponse(byte[] response) {
        QueryResponseBody queryResponse = null;
        try {
            queryResponse = (QueryResponseBody) msgPackMapper.bytesToObject(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queryResponse;
    }

    @Override
    public QueryResponseBody perform(MockMvc mockMVC, String testEndpoint, QueryRequestBody payload) throws Exception {
        byte[] response = ASTestUtils.performOperationAndReturn(mockMVC, testEndpoint,
                        new String(msgPackMapper.objectToBytes(payload), StandardCharsets.UTF_8))
                .getBytes(StandardCharsets.UTF_8);
        return getQueryResponse(response);
    }
}