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
package com.aerospike.restclient.domain.batchmodels;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.ReadModeAP;
import com.aerospike.client.policy.ReadModeSC;
import com.aerospike.restclient.ASTestMapper;
import com.aerospike.restclient.IASTestMapper;
import com.aerospike.restclient.config.JSONMessageConverter;
import com.aerospike.restclient.config.MsgPackConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;


import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class RestClientBatchReadPolicyTest {
    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JsonRestClientBatchReadPolicyMapper()),
                Arguments.of(new MsgPackRestClientBatchReadPolicyMapper())
        );
    }

    @Test
    public void testNoArgConstructor() {
        new BatchWritePolicy();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchReadConstructionStringKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> policyMap = new HashMap<>();
        policyMap.put("filterExp", "a filter");
        policyMap.put("readModeAP", "ALL");
        policyMap.put("readModeSC", "ALLOW_REPLICA");

        BatchReadPolicy mappedBody = (BatchReadPolicy) mapper.bytesToObject(mapper.objectToBytes(policyMap));

        Assertions.assertEquals("a filter", mappedBody.filterExp);
        Assertions.assertEquals(ReadModeAP.ALL, mappedBody.readModeAP);
        Assertions.assertEquals(ReadModeSC.ALLOW_REPLICA, mappedBody.readModeSC);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testToBatchReadPolicy(IASTestMapper mapper) {
        Expression expectedExp = Exp.build(Exp.ge(Exp.bin("a", Exp.Type.INT), Exp.bin("b", Exp.Type.INT)));
        String expectedExpStr = expectedExp.getBase64();
        BatchReadPolicy restPolicy = new BatchReadPolicy();
        restPolicy.filterExp = expectedExpStr;
        restPolicy.readModeAP = ReadModeAP.ALL;
        restPolicy.readModeSC = ReadModeSC.ALLOW_REPLICA;

        com.aerospike.client.policy.BatchReadPolicy actualPolicy = restPolicy.toBatchReadPolicy();

        Assertions.assertEquals(expectedExp, actualPolicy.filterExp);
        Assertions.assertEquals(ReadModeAP.ALL, actualPolicy.readModeAP);
        Assertions.assertEquals(ReadModeSC.ALLOW_REPLICA, actualPolicy.readModeSC);
    }
}

class JsonRestClientBatchReadPolicyMapper extends ASTestMapper {

    public JsonRestClientBatchReadPolicyMapper() {
        super(JSONMessageConverter.getJSONObjectMapper(), BatchReadPolicy.class);
    }
}

class MsgPackRestClientBatchReadPolicyMapper extends ASTestMapper {

    public MsgPackRestClientBatchReadPolicyMapper() {
        super(MsgPackConverter.getASMsgPackObjectMapper(), BatchReadPolicy.class);
    }
}
