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

import com.aerospike.client.cdt.MapWriteFlags;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MapWriteFlagTest {

    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(MapWriteFlag.DEFAULT, MapWriteFlags.DEFAULT),
                Arguments.of(MapWriteFlag.CREATE_ONLY, MapWriteFlags.CREATE_ONLY),
                Arguments.of(MapWriteFlag.UPDATE_ONLY, MapWriteFlags.UPDATE_ONLY),
                Arguments.of(MapWriteFlag.NO_FAIL, MapWriteFlags.NO_FAIL),
                Arguments.of(MapWriteFlag.PARTIAL, MapWriteFlags.PARTIAL)
        );
    }

    @ParameterizedTest
    @MethodSource("getParams")
    void testFlag(MapWriteFlag enum_, int flag) {
        assertEquals(enum_.flag, flag);
    }
}
