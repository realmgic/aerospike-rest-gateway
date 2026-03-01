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
package com.aerospike.restclient.controller;

import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.restclient.controllers.KeyValueController;
import com.aerospike.restclient.service.AerospikeRecordService;
import com.aerospike.restclient.util.AerospikeAPIConstants;
import com.aerospike.restclient.util.AerospikeAPIConstants.RecordKeyType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;

@SpringBootTest
public class KVControllerV1KeyTypeTests {

    @Autowired
    KeyValueController controller;
    @MockitoBean
    AerospikeRecordService recordService;

    private static final String ns = "test";
    private static final String set = "set";
    private static final String key = "key";

    private Map<String, Object> dummyBins;
    private byte[] msgpackBins;
    private final ObjectMapper mpMapper = new ObjectMapper(new MessagePackFactory());

    static Stream<Arguments> keyType() {
        return Stream.of(
                Arguments.of(RecordKeyType.STRING),
                Arguments.of(RecordKeyType.BYTES),
                Arguments.of(RecordKeyType.DIGEST),
                Arguments.of(RecordKeyType.INTEGER),
                Arguments.of((RecordKeyType) null)
        );
    }

    @BeforeEach
    public void setup() throws JsonProcessingException {
        dummyBins = new HashMap<>();
        dummyBins.put("bin", "a");
        msgpackBins = mpMapper.writeValueAsBytes(dummyBins);
    }

    private static Map<String, String> queryParamsFor(RecordKeyType expectedKeyType) {
        Map<String, String> queryParams = new HashMap<>();
        if (expectedKeyType != null) {
            queryParams.put(AerospikeAPIConstants.KEY_TYPE, expectedKeyType.toString());
        }
        return queryParams;
    }

    private static MultiValueMap<String, String> multiQueryParamsFor(RecordKeyType expectedKeyType) {
        MultiValueMap<String, String> multiQueryParams = new LinkedMultiValueMap<>();
        if (expectedKeyType != null) {
            multiQueryParams.put(AerospikeAPIConstants.KEY_TYPE, Collections.singletonList(expectedKeyType.toString()));
        }
        return multiQueryParams;
    }

    /* UPDATE */
    @ParameterizedTest
    @MethodSource("keyType")
    public void testKeyTypeForUpdateNSSetKey(RecordKeyType expectedKeyType) {
        Map<String, String> queryParams = queryParamsFor(expectedKeyType);
        controller.updateRecordNamespaceSetKey(ns, set, key, dummyBins, queryParams, null);

        verify(recordService, Mockito.only()).storeRecord(isNull(), eq(ns), eq(set), eq(key), eq(dummyBins),
                eq(expectedKeyType), isA(WritePolicy.class));
    }

    @ParameterizedTest
    @MethodSource("keyType")
    public void testRecordKeyTypeForUpdateNSKey(RecordKeyType expectedKeyType) {
        Map<String, String> queryParams = queryParamsFor(expectedKeyType);
        controller.updateRecordNamespaceKey(ns, key, dummyBins, queryParams, null);

        verify(recordService, Mockito.only()).storeRecord(isNull(), eq(ns), isNull(), eq((key)), eq(dummyBins),
                eq(expectedKeyType), isA(WritePolicy.class));
    }

    @ParameterizedTest
    @MethodSource("keyType")
    public void testRecordKeyTypeForUpdateNSSetKeyMP(RecordKeyType expectedKeyType) {
        Map<String, String> queryParams = queryParamsFor(expectedKeyType);
        controller.updateRecordNamespaceSetKeyMP(ns, set, key, new ByteArrayInputStream(msgpackBins), queryParams,
                null);

        verify(recordService, Mockito.only()).storeRecord(isNull(), eq(ns), eq(set), eq(key), eq(dummyBins),
                eq(expectedKeyType), isA(WritePolicy.class));
    }

    @ParameterizedTest
    @MethodSource("keyType")
    public void testRecordKeyTypeForUpdateNSKeyMP(RecordKeyType expectedKeyType) {
        Map<String, String> queryParams = queryParamsFor(expectedKeyType);
        controller.updateRecordNamespaceKeyMP(ns, key, new ByteArrayInputStream(msgpackBins), queryParams, null);

        verify(recordService, Mockito.only()).storeRecord(isNull(), eq(ns), isNull(), eq((key)), eq(dummyBins),
                eq(expectedKeyType), isA(WritePolicy.class));
    }

    /* DELETE */
    @ParameterizedTest
    @MethodSource("keyType")
    public void testKeyTypeDeleteNSSetKey(RecordKeyType expectedKeyType) {
        Map<String, String> queryParams = queryParamsFor(expectedKeyType);
        controller.deleteRecordNamespaceSetKey(ns, set, key, queryParams, null);
        verify(recordService, Mockito.only()).deleteRecord(isNull(), any(String.class), any(String.class),
                any(String.class), eq(expectedKeyType), any(WritePolicy.class));
    }

    @ParameterizedTest
    @MethodSource("keyType")
    public void testKeyTypeDeleteNSKey(RecordKeyType expectedKeyType) {
        Map<String, String> queryParams = queryParamsFor(expectedKeyType);
        controller.deleteRecordNamespaceKey(ns, key, queryParams, null);
        verify(recordService, Mockito.only()).deleteRecord(isNull(), any(String.class), isNull(), any(String.class),
                eq(expectedKeyType), any(WritePolicy.class));
    }

    /* CREATE */
    @ParameterizedTest
    @MethodSource("keyType")
    public void testKeyTypeCreateNSSetKey(RecordKeyType expectedKeyType) {
        Map<String, String> queryParams = queryParamsFor(expectedKeyType);
        controller.createRecordNamespaceSetKey(ns, set, key, dummyBins, queryParams, null);

        verify(recordService, Mockito.only()).storeRecord(isNull(), eq(ns), eq(set), eq(key), eq(dummyBins),
                eq(expectedKeyType), isA(WritePolicy.class));
    }

    @ParameterizedTest
    @MethodSource("keyType")
    public void testRecordKeyTypeCreateNSKey(RecordKeyType expectedKeyType) {
        Map<String, String> queryParams = queryParamsFor(expectedKeyType);
        controller.createRecordNamespaceKey(ns, key, dummyBins, queryParams, null);

        verify(recordService, Mockito.only()).storeRecord(isNull(), eq(ns), isNull(), eq((key)), eq(dummyBins),
                eq(expectedKeyType), isA(WritePolicy.class));
    }

    @ParameterizedTest
    @MethodSource("keyType")
    public void testRecordKeyTypeCreateNSSetKeyMP(RecordKeyType expectedKeyType) {
        Map<String, String> queryParams = queryParamsFor(expectedKeyType);
        controller.createRecordNamespaceSetKeyMP(ns, set, key, new ByteArrayInputStream(msgpackBins), queryParams,
                null);

        verify(recordService, Mockito.only()).storeRecord(isNull(), eq(ns), eq(set), eq(key), eq(dummyBins),
                eq(expectedKeyType), isA(WritePolicy.class));
    }

    @ParameterizedTest
    @MethodSource("keyType")
    public void testRecordKeyTypeCreateNSKeyMP(RecordKeyType expectedKeyType) {
        Map<String, String> queryParams = queryParamsFor(expectedKeyType);
        controller.createRecordNamespaceKeyMP(ns, key, new ByteArrayInputStream(msgpackBins), queryParams, null);

        verify(recordService, Mockito.only()).storeRecord(isNull(), eq(ns), isNull(), eq((key)), eq(dummyBins),
                eq(expectedKeyType), isA(WritePolicy.class));
    }

    /* REPLACE */
    @ParameterizedTest
    @MethodSource("keyType")
    public void testKeyTypeReplaceNSSetKey(RecordKeyType expectedKeyType) {
        Map<String, String> queryParams = queryParamsFor(expectedKeyType);
        controller.replaceRecordNamespaceSetKey(ns, set, key, dummyBins, queryParams, null);

        verify(recordService, Mockito.only()).storeRecord(isNull(), eq(ns), eq(set), eq(key), eq(dummyBins),
                eq(expectedKeyType), isA(WritePolicy.class));
    }

    @ParameterizedTest
    @MethodSource("keyType")
    public void testRecordKeyTypeReplaceNSKey(RecordKeyType expectedKeyType) {
        Map<String, String> queryParams = queryParamsFor(expectedKeyType);
        controller.replaceRecordNamespaceKey(ns, key, dummyBins, queryParams, null);

        verify(recordService, Mockito.only()).storeRecord(isNull(), eq(ns), isNull(), eq((key)), eq(dummyBins),
                eq(expectedKeyType), isA(WritePolicy.class));
    }

    @ParameterizedTest
    @MethodSource("keyType")
    public void testRecordKeyTypeReplaceNSSetKeyMP(RecordKeyType expectedKeyType) {
        Map<String, String> queryParams = queryParamsFor(expectedKeyType);
        controller.replaceRecordNamespaceSetKeyMP(ns, set, key, new ByteArrayInputStream(msgpackBins), queryParams,
                null);

        verify(recordService, Mockito.only()).storeRecord(isNull(), eq(ns), eq(set), eq(key), eq(dummyBins),
                eq(expectedKeyType), isA(WritePolicy.class));
    }

    @ParameterizedTest
    @MethodSource("keyType")
    public void testRecordKeyTypeReplaceNSKeyMP(RecordKeyType expectedKeyType) {
        Map<String, String> queryParams = queryParamsFor(expectedKeyType);
        controller.replaceRecordNamespaceKeyMP(ns, key, new ByteArrayInputStream(msgpackBins), queryParams, null);

        verify(recordService, Mockito.only()).storeRecord(isNull(), eq(ns), isNull(), eq((key)), eq(dummyBins),
                eq(expectedKeyType), isA(WritePolicy.class));
    }

    /*GET */
    @ParameterizedTest
    @MethodSource("keyType")
    public void testKeyTypeForNSSetKey(RecordKeyType expectedKeyType) {
        MultiValueMap<String, String> multiQueryParams = multiQueryParamsFor(expectedKeyType);
        controller.getRecordNamespaceSetKey(ns, set, key, multiQueryParams, null);
        verify(recordService, Mockito.only()).fetchRecord(isNull(), any(String.class), any(String.class),
                any(String.class), any(String[].class), eq(expectedKeyType), isA(Policy.class));
    }

    @ParameterizedTest
    @MethodSource("keyType")
    public void testKeyTypeForNSKey(RecordKeyType expectedKeyType) {
        MultiValueMap<String, String> multiQueryParams = multiQueryParamsFor(expectedKeyType);
        controller.getRecordNamespaceKey(ns, key, multiQueryParams, null);
        verify(recordService, Mockito.only()).fetchRecord(isNull(), any(String.class), isNull(), any(String.class),
                any(String[].class), eq(expectedKeyType), isA(Policy.class));
    }
}
