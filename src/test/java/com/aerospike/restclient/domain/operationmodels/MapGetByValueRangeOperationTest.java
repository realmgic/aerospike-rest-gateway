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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MapGetByValueRangeOperationTest {
    @Test
    public void isInverted() {
        MapGetByValueRangeOperation op = new MapGetByValueRangeOperation("bin", MapReturnType.RANK);
        Assertions.assertFalse(op.isInverted());
        op.setInverted(true);
        Assertions.assertTrue(op.isInverted());
    }

    @Test
    public void getValueBegin() {
        MapGetByValueRangeOperation op = new MapGetByValueRangeOperation("bin", MapReturnType.RANK);
        Assertions.assertNull(op.getValueBegin());
        op.setValueBegin(true);
        Assertions.assertTrue((Boolean) op.getValueBegin());
    }

    @Test
    public void getValueEnd() {
        MapGetByValueRangeOperation op = new MapGetByValueRangeOperation("bin", MapReturnType.RANK);
        Assertions.assertNull(op.getValueEnd());
        op.setValueEnd(true);
        Assertions.assertTrue((Boolean) op.getValueEnd());
    }
}
