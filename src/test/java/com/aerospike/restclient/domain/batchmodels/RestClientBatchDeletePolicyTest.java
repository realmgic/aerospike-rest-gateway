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
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.GenerationPolicy;
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

public class RestClientBatchDeletePolicyTest {
    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JsonBatchDeletePolicyMapper()),
                Arguments.of(new MsgPackBatchDeletePolicyMapper())
        );
    }

    @Test
    public void testNoArgConstructor() {
        new BatchDeletePolicy();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchDeleteConstructionStringKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> policyMap = new HashMap<>();
        policyMap.put("filterExp", "a filter");
        policyMap.put("commitLevel", "COMMIT_MASTER");
        policyMap.put("generationPolicy", "EXPECT_GEN_EQUAL");
        policyMap.put("generation", 101);
        policyMap.put("durableDelete", true);
        policyMap.put("sendKey", true);

        BatchDeletePolicy mappedBody = (BatchDeletePolicy) mapper.bytesToObject(mapper.objectToBytes(policyMap));

        Assertions.assertEquals("a filter", mappedBody.filterExp);
        Assertions.assertEquals(CommitLevel.COMMIT_MASTER, mappedBody.commitLevel);
        Assertions.assertEquals(GenerationPolicy.EXPECT_GEN_EQUAL, mappedBody.generationPolicy);
        Assertions.assertEquals(101, mappedBody.generation);
        Assertions.assertTrue(mappedBody.durableDelete);
        Assertions.assertTrue(mappedBody.sendKey);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testToBatchDeletePolicy(IASTestMapper mapper) {
        Expression expectedExp = Exp.build(Exp.ge(Exp.bin("a", Exp.Type.INT), Exp.bin("b", Exp.Type.INT)));
        String expectedExpStr = expectedExp.getBase64();
        BatchDeletePolicy restPolicy = new BatchDeletePolicy();
        restPolicy.filterExp = expectedExpStr;
        restPolicy.commitLevel = CommitLevel.COMMIT_MASTER;
        restPolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        restPolicy.generation = 101;
        restPolicy.durableDelete = true;
        restPolicy.sendKey = true;

        com.aerospike.client.policy.BatchDeletePolicy actualPolicy = restPolicy.toBatchDeletePolicy();

        Assertions.assertEquals(expectedExp, actualPolicy.filterExp);
        Assertions.assertEquals(CommitLevel.COMMIT_MASTER, actualPolicy.commitLevel);
        Assertions.assertEquals(GenerationPolicy.EXPECT_GEN_EQUAL, actualPolicy.generationPolicy);
        Assertions.assertEquals(101, actualPolicy.generation);
        Assertions.assertTrue(actualPolicy.durableDelete);
        Assertions.assertTrue(actualPolicy.sendKey);
    }
}

class JsonBatchDeletePolicyMapper extends ASTestMapper {

    public JsonBatchDeletePolicyMapper() {
        super(JSONMessageConverter.getJSONObjectMapper(), BatchDeletePolicy.class);
    }
}

class MsgPackBatchDeletePolicyMapper extends ASTestMapper {

    public MsgPackBatchDeletePolicyMapper() {
        super(MsgPackConverter.getASMsgPackObjectMapper(), BatchDeletePolicy.class);
    }
}
