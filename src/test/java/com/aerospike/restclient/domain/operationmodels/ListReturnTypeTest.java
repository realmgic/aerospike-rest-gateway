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
package com.aerospike.restclient.domain.operationmodels;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ListReturnTypeTest {

    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(ListReturnType.COUNT, false, com.aerospike.client.cdt.ListReturnType.COUNT),
                Arguments.of(ListReturnType.COUNT, true, com.aerospike.client.cdt.ListReturnType.COUNT | com.aerospike.client.cdt.ListReturnType.INVERTED),
                Arguments.of(ListReturnType.INDEX, false, com.aerospike.client.cdt.ListReturnType.INDEX),
                Arguments.of(ListReturnType.INDEX, true, com.aerospike.client.cdt.ListReturnType.INDEX | com.aerospike.client.cdt.ListReturnType.INVERTED),
                Arguments.of(ListReturnType.EXISTS, false, com.aerospike.client.cdt.ListReturnType.EXISTS),
                Arguments.of(ListReturnType.EXISTS, true, com.aerospike.client.cdt.ListReturnType.EXISTS | com.aerospike.client.cdt.ListReturnType.INVERTED),
                Arguments.of(ListReturnType.NONE, false, com.aerospike.client.cdt.ListReturnType.NONE),
                Arguments.of(ListReturnType.NONE, true, com.aerospike.client.cdt.ListReturnType.NONE | com.aerospike.client.cdt.ListReturnType.INVERTED),
                Arguments.of(ListReturnType.RANK, false, com.aerospike.client.cdt.ListReturnType.RANK),
                Arguments.of(ListReturnType.RANK, true, com.aerospike.client.cdt.ListReturnType.RANK | com.aerospike.client.cdt.ListReturnType.INVERTED),
                Arguments.of(ListReturnType.REVERSE_INDEX, false, com.aerospike.client.cdt.ListReturnType.REVERSE_INDEX),
                Arguments.of(ListReturnType.REVERSE_INDEX, true, com.aerospike.client.cdt.ListReturnType.REVERSE_INDEX | com.aerospike.client.cdt.ListReturnType.INVERTED),
                Arguments.of(ListReturnType.REVERSE_RANK, false, com.aerospike.client.cdt.ListReturnType.REVERSE_RANK),
                Arguments.of(ListReturnType.REVERSE_RANK, true, com.aerospike.client.cdt.ListReturnType.REVERSE_RANK | com.aerospike.client.cdt.ListReturnType.INVERTED),
                Arguments.of(ListReturnType.VALUE, false, com.aerospike.client.cdt.ListReturnType.VALUE),
                Arguments.of(ListReturnType.VALUE, true, com.aerospike.client.cdt.ListReturnType.VALUE | com.aerospike.client.cdt.ListReturnType.INVERTED)
        );
    }

    @ParameterizedTest
    @MethodSource("getParams")
    void toListReturnType(ListReturnType enum_, boolean inverted, int flag) {
        assertEquals(enum_.toListReturnType(inverted), flag);
    }
}
