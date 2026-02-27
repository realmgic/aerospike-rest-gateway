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

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.restclient.ASTestMapper;
import com.aerospike.restclient.config.JSONMessageConverter;
import com.aerospike.restclient.config.MsgPackConverter;
import com.aerospike.restclient.domain.RestClientKeyRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;


import java.util.List;
import java.util.stream.Stream;

public class QueryResponseBodyTest {

    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JsonQueryResponseMapper()),
                Arguments.of(new MsgPackQueryResponseMapper())
        );
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testRestClientQueryResponseAddRecords(ASTestMapper mapper) {
        QueryResponseBody restQuery = new QueryResponseBody(10);

        for (int i = 0; i < 10; i++) {
            restQuery.addRecord(new RestClientKeyRecord(new Key("ns", "set", "key" + i), new Record(null, 0, 0)));
        }

        Assertions.assertEquals(10, restQuery.size());

        List<RestClientKeyRecord> actualRecs = restQuery.getRecords();

        for (int i = 0; i < 10; i++) {
            Assertions.assertEquals("key" + i, actualRecs.get(i).userKey);
        }
    }
}

class JsonQueryResponseMapper extends ASTestMapper {

    public JsonQueryResponseMapper() {
        super(JSONMessageConverter.getJSONObjectMapper(), QueryResponseBody.class);
    }
}

class MsgPackQueryResponseMapper extends ASTestMapper {

    public MsgPackQueryResponseMapper() {
        super(MsgPackConverter.getASMsgPackObjectMapper(), QueryResponseBody.class);
    }
}
