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

public class MapReturnTypeTest {

    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(MapReturnType.COUNT, false, com.aerospike.client.cdt.MapReturnType.COUNT),
                Arguments.of(MapReturnType.COUNT, true, com.aerospike.client.cdt.MapReturnType.COUNT | com.aerospike.client.cdt.MapReturnType.INVERTED),
                Arguments.of(MapReturnType.INDEX, false, com.aerospike.client.cdt.MapReturnType.INDEX),
                Arguments.of(MapReturnType.INDEX, true, com.aerospike.client.cdt.MapReturnType.INDEX | com.aerospike.client.cdt.MapReturnType.INVERTED),
                Arguments.of(MapReturnType.EXISTS, false, com.aerospike.client.cdt.MapReturnType.EXISTS),
                Arguments.of(MapReturnType.EXISTS, true, com.aerospike.client.cdt.MapReturnType.EXISTS | com.aerospike.client.cdt.MapReturnType.INVERTED),
                Arguments.of(MapReturnType.NONE, false, com.aerospike.client.cdt.MapReturnType.NONE),
                Arguments.of(MapReturnType.NONE, true, com.aerospike.client.cdt.MapReturnType.NONE | com.aerospike.client.cdt.MapReturnType.INVERTED),
                Arguments.of(MapReturnType.RANK, false, com.aerospike.client.cdt.MapReturnType.RANK),
                Arguments.of(MapReturnType.RANK, true, com.aerospike.client.cdt.MapReturnType.RANK | com.aerospike.client.cdt.MapReturnType.INVERTED),
                Arguments.of(MapReturnType.REVERSE_INDEX, false, com.aerospike.client.cdt.MapReturnType.REVERSE_INDEX),
                Arguments.of(MapReturnType.REVERSE_INDEX, true, com.aerospike.client.cdt.MapReturnType.REVERSE_INDEX | com.aerospike.client.cdt.MapReturnType.INVERTED),
                Arguments.of(MapReturnType.REVERSE_RANK, false, com.aerospike.client.cdt.MapReturnType.REVERSE_RANK),
                Arguments.of(MapReturnType.REVERSE_RANK, true, com.aerospike.client.cdt.MapReturnType.REVERSE_RANK | com.aerospike.client.cdt.MapReturnType.INVERTED),
                Arguments.of(MapReturnType.VALUE, false, com.aerospike.client.cdt.MapReturnType.VALUE),
                Arguments.of(MapReturnType.VALUE, true, com.aerospike.client.cdt.MapReturnType.VALUE | com.aerospike.client.cdt.MapReturnType.INVERTED),
                Arguments.of(MapReturnType.KEY, true, com.aerospike.client.cdt.MapReturnType.KEY | com.aerospike.client.cdt.MapReturnType.INVERTED),
                Arguments.of(MapReturnType.EXISTS, true, com.aerospike.client.cdt.MapReturnType.EXISTS | com.aerospike.client.cdt.MapReturnType.INVERTED)
        );
    }

    @ParameterizedTest
    @MethodSource("getParams")
    void toMapReturnType(MapReturnType enum_, boolean inverted, int flag) {
        assertEquals(enum_.toMapReturnType(inverted), flag);
    }
}
