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
package com.aerospike.restclient.domain.querymodels;

import com.aerospike.restclient.ASTestMapper;
import com.aerospike.restclient.ASTestUtils;
import com.aerospike.restclient.config.JSONMessageConverter;
import com.aerospike.restclient.config.MsgPackConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;


import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.aerospike.restclient.util.AerospikeAPIConstants.QueryFilterTypes.EQUAL_LONG;

public class QueryRequestBodyTest {

    private static final String ns = "test";
    private static final String set = "ctx";

    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JsonQueryBodyMapper()),
                Arguments.of(new MsgPackQueryBodyMapper())
        );
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testEmptyMapToRestClientQueryBody(ASTestMapper mapper) {
        Map<String, Object> ctxMap = new HashMap<>();

        try {
            mapper.bytesToObject(mapper.objectToBytes(ctxMap));
        } catch (Exception e) {
            Assertions.fail("Should have mapped to RestClientQueryBody");
            // Success
        }
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testMapsToRestClientQueryBody(ASTestMapper mapper) {
        Map<String, Object> restMap = new HashMap<>();
        restMap.put("from", "from-pagination-token");
        Map<String, Object> filter = new HashMap<>();
        filter.put("type", EQUAL_LONG);
        filter.put("value", 1L);
        restMap.put("filter", filter);

        QueryEqualLongFilter expectedFilter = new QueryEqualLongFilter();
        expectedFilter.value = 1L;

        try {
            QueryRequestBody restQuery = (QueryRequestBody) mapper.bytesToObject(mapper.objectToBytes(restMap));
            Assertions.assertEquals(restQuery.from, "from-pagination-token");
            ASTestUtils.compareFilter(expectedFilter.toFilter(), restQuery.filter.toFilter());
        } catch (Exception e) {
            Assertions.fail(String.format("Should have mapped to RestClientQueryBody %s", e));
        }
    }
}

class JsonQueryBodyMapper extends ASTestMapper {

    public JsonQueryBodyMapper() {
        super(JSONMessageConverter.getJSONObjectMapper(), QueryRequestBody.class);
    }
}

class MsgPackQueryBodyMapper extends ASTestMapper {

    public MsgPackQueryBodyMapper() {
        super(MsgPackConverter.getASMsgPackObjectMapper(), QueryRequestBody.class);
    }
}
