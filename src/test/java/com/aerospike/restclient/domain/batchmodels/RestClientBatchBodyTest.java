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

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.restclient.ASTestMapper;
import com.aerospike.restclient.ASTestUtils;
import com.aerospike.restclient.IASTestMapper;
import com.aerospike.restclient.config.JSONMessageConverter;
import com.aerospike.restclient.config.MsgPackConverter;
import com.aerospike.restclient.domain.RestClientKey;
import com.aerospike.restclient.domain.operationmodels.AddOperation;
import com.aerospike.restclient.domain.operationmodels.OperationTypes;
import com.aerospike.restclient.domain.operationmodels.PutOperation;
import com.aerospike.restclient.util.AerospikeAPIConstants;
import com.aerospike.restclient.util.AerospikeAPIConstants.RecordKeyType;
import com.aerospike.restclient.util.RestClientErrors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class RestClientBatchBodyTest {

    private static final String ns = "test";
    private static final String set = "set";

    static Stream<Arguments> getParams() {
        return Stream.of(
                Arguments.of(new JsonRestClientBatchRecordBodyMapper()),
                Arguments.of(new MsgPackRestClientBatchRecordBodyMapper())
        );
    }

    /*
     * BatchRead specific tests
     */
    @Test
    public void testReadNoArgConstructor() {
        new BatchRead();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchReadConstructionStringKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("key", RecordKeyType.STRING);
        Map<String, Object> batchMap = getBatchReadBase();
        batchMap.put("key", keyMap);

        BatchRead mappedBody = (BatchRead) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        Assertions.assertTrue(mappedBody.readAllBins);
        Assertions.assertArrayEquals(mappedBody.binNames, new String[]{});
        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.STRING, rcKey.keyType);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchReadConstructionWithBins(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("key", RecordKeyType.STRING);
        Map<String, Object> batchMap = getBatchReadBase();
        batchMap.put("key", keyMap);
        String[] bins = {"a", "b", "c"};
        batchMap.put("binNames", bins);

        BatchRead mappedBody = (BatchRead) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        Assertions.assertTrue(mappedBody.readAllBins);
        Assertions.assertArrayEquals(mappedBody.binNames, bins);
        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.STRING, rcKey.keyType);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchReadConstructionIntegerKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap(5, RecordKeyType.INTEGER);
        Map<String, Object> batchMap = getBatchReadBase();
        batchMap.put("key", keyMap);

        BatchRead mappedBody = (BatchRead) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        Assertions.assertTrue(mappedBody.readAllBins);
        Assertions.assertArrayEquals(mappedBody.binNames, new String[]{});
        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.INTEGER, rcKey.keyType);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchReadConstructionBytesKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("lookslikebytes", RecordKeyType.BYTES);
        Map<String, Object> batchMap = getBatchReadBase();
        batchMap.put("key", keyMap);

        BatchRead mappedBody = (BatchRead) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        Assertions.assertTrue(mappedBody.readAllBins);
        Assertions.assertArrayEquals(mappedBody.binNames, new String[]{});
        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.BYTES, rcKey.keyType);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchReadConstructionDigestKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("digest", RecordKeyType.DIGEST);
        Map<String, Object> batchMap = getBatchReadBase();
        batchMap.put("key", keyMap);

        BatchRead mappedBody = (BatchRead) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        Assertions.assertTrue(mappedBody.readAllBins);
        Assertions.assertArrayEquals(mappedBody.binNames, new String[]{});
        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.DIGEST, rcKey.keyType);
    }

    @Test
    public void testConversionToBatchReadWithBinNames() {
        Key realKey = new Key(ns, set, "test");
        RestClientKey rcKey = new RestClientKey(realKey);
        String[] bins = {"b1", "b2", "b3"};
        BatchRead rCBRB = new BatchRead();

        rCBRB.binNames = bins;
        rCBRB.key = rcKey;
        rCBRB.readAllBins = false;

        com.aerospike.client.BatchRead convertedBatchRead = rCBRB.toBatchRecord();

        Assertions.assertFalse(convertedBatchRead.readAllBins);
        Assertions.assertTrue(ASTestUtils.compareKeys(realKey, convertedBatchRead.key));
        Assertions.assertArrayEquals(bins, convertedBatchRead.binNames);
    }

    @Test
    public void testConversionToBatchReadWithoutBinNames() {
        Key realKey = new Key(ns, set, "test");
        RestClientKey rcKey = new RestClientKey(realKey);
        String[] bins = {"b1", "b2", "b3"};
        BatchRead rCBRB = new BatchRead();

        rCBRB.binNames = bins;
        rCBRB.key = rcKey;
        rCBRB.readAllBins = true;

        com.aerospike.client.BatchRead convertedBatchRead = rCBRB.toBatchRecord();

        Assertions.assertTrue(convertedBatchRead.readAllBins);
        Assertions.assertTrue(ASTestUtils.compareKeys(realKey, convertedBatchRead.key));
        Assertions.assertNull(convertedBatchRead.binNames);
    }

    @Test
    public void testConversionToBatchReadWithOnlyKeySet() {
        Key realKey = new Key(ns, set, "test");
        RestClientKey rcKey = new RestClientKey(realKey);
        BatchRead rCBRB = new BatchRead();

        rCBRB.key = rcKey;

        com.aerospike.client.BatchRead convertedBatchRead = rCBRB.toBatchRecord();

        Assertions.assertFalse(convertedBatchRead.readAllBins);
        Assertions.assertTrue(ASTestUtils.compareKeys(realKey, convertedBatchRead.key));
        Assertions.assertNull(convertedBatchRead.binNames);
    }

    @Test
    public void testConversionWithNullKey() {
        BatchRead rCBRB = new BatchRead();
        rCBRB.key = null;
        Assertions.assertThrows(RestClientErrors.InvalidKeyError.class, rCBRB::toBatchRecord);
    }

    /*
     * BatchWrite specific tests
     */

    @Test
    public void testWriteNoArgConstructor() {
        new BatchWrite();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchWriteConstructionStringKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("key", RecordKeyType.STRING);
        Map<String, Object> batchMap = getBatchWriteBase();
        batchMap.put("key", keyMap);

        BatchWrite mappedBody = (BatchWrite) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.STRING, rcKey.keyType);
        Assertions.assertEquals(mappedBody.opsList, new ArrayList<>());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchWriteConstructionWithOps(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("key", RecordKeyType.STRING);
        Map<String, Object> expectedBatchMap = getBatchWriteBase();
        List<Map<String, Object>> expectedOpsListMap = new ArrayList<>();

        Map<String, Object> op1 = new HashMap<>();
        op1.put("type", OperationTypes.ADD);
        op1.put("binName", "bin1");
        op1.put("incr", 1);

        Map<String, Object> op2 = new HashMap<>();
        op2.put("type", OperationTypes.PUT);
        op2.put("binName", "bin2");
        op2.put("value", "new val");

        expectedOpsListMap.add(op1);
        expectedOpsListMap.add(op2);
        expectedBatchMap.put("key", keyMap);
        expectedBatchMap.put("opsList", expectedOpsListMap);

        BatchWrite actualBody = (BatchWrite) mapper.bytesToObject(mapper.objectToBytes(expectedBatchMap));
        RestClientKey actualKey = actualBody.key;

        Assertions.assertEquals(2, actualBody.opsList.size());
        Assertions.assertEquals(RecordKeyType.STRING, actualKey.keyType);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchWriteConstructionWithPolicy(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("key", RecordKeyType.STRING);
        Map<String, Object> batchWritePolicy = new HashMap<>();
        Map<String, Object> expectedBatchMap = getBatchWriteBase();

        batchWritePolicy.put("recordExistsAction", "UPDATE");

        expectedBatchMap.put("key", keyMap);
        expectedBatchMap.put("policy", batchWritePolicy);

        BatchWrite actualBody = (BatchWrite) mapper.bytesToObject(mapper.objectToBytes(expectedBatchMap));
        RestClientKey actualKey = actualBody.key;

        Assertions.assertEquals(actualBody.policy.recordExistsAction, RecordExistsAction.UPDATE);
        Assertions.assertEquals(0, actualBody.opsList.size());
        Assertions.assertEquals(RecordKeyType.STRING, actualKey.keyType);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchWriteConstructionIntegerKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap(5, RecordKeyType.INTEGER);
        Map<String, Object> batchMap = getBatchWriteBase();
        batchMap.put("key", keyMap);

        BatchWrite mappedBody = (BatchWrite) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(mappedBody.opsList, new ArrayList<>());
        Assertions.assertEquals(RecordKeyType.INTEGER, rcKey.keyType);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchWriteConstructionBytesKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("lookslikebytes", RecordKeyType.BYTES);
        Map<String, Object> batchMap = getBatchWriteBase();
        batchMap.put("key", keyMap);

        BatchWrite mappedBody = (BatchWrite) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.BYTES, rcKey.keyType);
        Assertions.assertEquals(mappedBody.opsList, new ArrayList<>());
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchWriteConstructionDigestKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("digest", RecordKeyType.DIGEST);
        Map<String, Object> batchMap = getBatchWriteBase();
        batchMap.put("key", keyMap);

        BatchWrite mappedBody = (BatchWrite) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.DIGEST, rcKey.keyType);
        Assertions.assertEquals(mappedBody.opsList, new ArrayList<>());
    }

    @Test
    public void testConversionToBatchWriteWithOps() {
        Key realKey = new Key(ns, set, "test");
        RestClientKey rcKey = new RestClientKey(realKey);
        List<com.aerospike.restclient.domain.operationmodels.Operation> opsList = new ArrayList<>();

        opsList.add(new AddOperation("bin1", 1));
        opsList.add(new PutOperation("bin2", "new val"));

        BatchWrite rCBRB = new BatchWrite();

        rCBRB.key = rcKey;
        rCBRB.opsList = opsList;

        com.aerospike.client.BatchWrite convertedBatchWrite = rCBRB.toBatchRecord();

        Assertions.assertTrue(ASTestUtils.compareKeys(realKey, convertedBatchWrite.key));
        Assertions.assertNull(convertedBatchWrite.policy);

        Assertions.assertEquals(convertedBatchWrite.ops[0].type, Operation.Type.ADD);
        Assertions.assertEquals(convertedBatchWrite.ops[0].binName, "bin1");
        Assertions.assertEquals(convertedBatchWrite.ops[0].value, Value.get(1));
        Assertions.assertEquals(convertedBatchWrite.ops[1].type, Operation.Type.WRITE);
        Assertions.assertEquals(convertedBatchWrite.ops[1].binName, "bin2");
        Assertions.assertEquals(convertedBatchWrite.ops[1].value, Value.get("new val"));
    }

    /*
     * BatchUDF specific tests
     */

    private final static String udfPkg = "test-package-name";
    private final static String udfFunc = "test-function-name";

    @Test
    public void testUDFNoArgConstructor() {
        new BatchUDFPolicy();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchUDFConstructionStringKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("key", RecordKeyType.STRING);
        Map<String, Object> batchMap = getBatchUDFBase();
        batchMap.put("key", keyMap);

        BatchUDF mappedBody = (BatchUDF) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.STRING, rcKey.keyType);
        Assertions.assertEquals(udfPkg, mappedBody.packageName);
        Assertions.assertEquals(udfFunc, mappedBody.functionName);
        Assertions.assertNull(mappedBody.functionArgs);
        Assertions.assertNull(mappedBody.policy);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchUDFConstructionWithFunctionArgs(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("key", RecordKeyType.STRING);
        Map<String, Object> batchMap = getBatchUDFBase();
        List<Object> expectedFunctionArgs = new ArrayList<>();
        expectedFunctionArgs.add("str");
        expectedFunctionArgs.add(1);
        expectedFunctionArgs.add(1.2);
        expectedFunctionArgs.add(true);

        batchMap.put("key", keyMap);
        batchMap.put("functionArgs", expectedFunctionArgs);

        BatchUDF mappedBody = (BatchUDF) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.STRING, rcKey.keyType);
        Assertions.assertEquals(udfPkg, mappedBody.packageName);
        Assertions.assertEquals(udfFunc, mappedBody.functionName);
        Assertions.assertTrue(ASTestUtils.compareCollection(mappedBody.functionArgs, expectedFunctionArgs));
        Assertions.assertNull(mappedBody.policy);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchUDFConstructionWithPolicy(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("key", RecordKeyType.STRING);
        Map<String, Object> batchUDFPolicy = new HashMap<>();
        Map<String, Object> expectedBatchMap = getBatchUDFBase();

        batchUDFPolicy.put("sendKey", true);
        expectedBatchMap.put("key", keyMap);
        expectedBatchMap.put("policy", batchUDFPolicy);

        BatchUDF actualBody = (BatchUDF) mapper.bytesToObject(mapper.objectToBytes(expectedBatchMap));
        RestClientKey actualKey = actualBody.key;

        Assertions.assertTrue(actualBody.policy.sendKey);
        Assertions.assertEquals(RecordKeyType.STRING, actualKey.keyType);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchUDFConstructionIntegerKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap(5, RecordKeyType.INTEGER);
        Map<String, Object> batchMap = getBatchUDFBase();
        batchMap.put("key", keyMap);

        BatchUDF mappedBody = (BatchUDF) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.INTEGER, rcKey.keyType);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchUDFConstructionBytesKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("lookslikebytes", RecordKeyType.BYTES);
        Map<String, Object> batchMap = getBatchUDFBase();
        batchMap.put("key", keyMap);

        BatchUDF mappedBody = (BatchUDF) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.BYTES, rcKey.keyType);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchUDFConstructionDigestKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("digest", RecordKeyType.DIGEST);
        Map<String, Object> batchMap = getBatchUDFBase();
        batchMap.put("key", keyMap);

        BatchUDF mappedBody = (BatchUDF) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.DIGEST, rcKey.keyType);
    }

    @Test
    public void testDeleteNoArgConstructor() {
        new BatchDeletePolicy();
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchDeleteConstructionStringKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("key", RecordKeyType.STRING);
        Map<String, Object> batchMap = getBatchDeleteBase();
        batchMap.put("key", keyMap);

        BatchDelete mappedBody = (BatchDelete) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.STRING, rcKey.keyType);
        Assertions.assertNull(mappedBody.policy);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchDeleteConstructionWithFunctionArgs(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("key", RecordKeyType.STRING);
        Map<String, Object> batchMap = getBatchDeleteBase();

        batchMap.put("key", keyMap);

        BatchDelete mappedBody = (BatchDelete) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.STRING, rcKey.keyType);
        Assertions.assertNull(mappedBody.policy);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchDeleteConstructionWithPolicy(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("key", RecordKeyType.STRING);
        Map<String, Object> batchDeletePolicy = new HashMap<>();
        Map<String, Object> expectedBatchMap = getBatchDeleteBase();

        batchDeletePolicy.put("commitLevel", "COMMIT_MASTER");
        expectedBatchMap.put("key", keyMap);
        expectedBatchMap.put("policy", batchDeletePolicy);

        BatchDelete actualBody = (BatchDelete) mapper.bytesToObject(mapper.objectToBytes(expectedBatchMap));
        RestClientKey actualKey = actualBody.key;

        Assertions.assertEquals(actualBody.policy.commitLevel, CommitLevel.COMMIT_MASTER);
        Assertions.assertEquals(RecordKeyType.STRING, actualKey.keyType);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchDeleteConstructionIntegerKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap(5, RecordKeyType.INTEGER);
        Map<String, Object> batchMap = getBatchDeleteBase();
        batchMap.put("key", keyMap);

        BatchDelete mappedBody = (BatchDelete) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.INTEGER, rcKey.keyType);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchDeleteConstructionBytesKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("lookslikebytes", RecordKeyType.BYTES);
        Map<String, Object> batchMap = getBatchDeleteBase();
        batchMap.put("key", keyMap);

        BatchDelete mappedBody = (BatchDelete) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.BYTES, rcKey.keyType);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testObjectMappedBatchDeleteConstructionDigestKey(IASTestMapper mapper) throws Exception {
        Map<String, Object> keyMap = getKeyMap("digest", RecordKeyType.DIGEST);
        Map<String, Object> batchMap = getBatchDeleteBase();
        batchMap.put("key", keyMap);

        BatchDelete mappedBody = (BatchDelete) mapper.bytesToObject(mapper.objectToBytes(batchMap));

        RestClientKey rcKey = mappedBody.key;

        Assertions.assertEquals(RecordKeyType.DIGEST, rcKey.keyType);
    }

    /* HELPERS */
    private Map<String, Object> getBatchReadBase() {
        Map<String, Object> batchMap = new HashMap<>();
        batchMap.put("type", "READ");
        batchMap.put("readAllBins", true);
        batchMap.put("binNames", new String[]{});
        return batchMap;
    }

    private Map<String, Object> getBatchWriteBase() {
        Map<String, Object> batchMap = new HashMap<>();
        batchMap.put("type", "WRITE");
        batchMap.put("opsList", new ArrayList<>());
        return batchMap;
    }

    private Map<String, Object> getBatchUDFBase() {
        Map<String, Object> batchMap = new HashMap<>();
        batchMap.put("type", "UDF");
        batchMap.put("packageName", udfPkg);
        batchMap.put("functionName", udfFunc);
        return batchMap;
    }

    private Map<String, Object> getBatchDeleteBase() {
        Map<String, Object> batchMap = new HashMap<>();
        batchMap.put("type", AerospikeAPIConstants.BATCH_TYPE_DELETE);
        return batchMap;
    }

    private Map<String, Object> getKeyMap(Object userKey, RecordKeyType keytype) {
        Map<String, Object> keyMap = new HashMap<>();
        keyMap.put(AerospikeAPIConstants.NAMESPACE, ns);
        keyMap.put(AerospikeAPIConstants.SETNAME, set);
        keyMap.put(AerospikeAPIConstants.USER_KEY, userKey);

        if (keytype != null) {
            keyMap.put(AerospikeAPIConstants.KEY_TYPE, keytype.toString());
        }

        return keyMap;
    }
}

class JsonRestClientBatchRecordBodyMapper extends ASTestMapper {

    public JsonRestClientBatchRecordBodyMapper() {
        super(JSONMessageConverter.getJSONObjectMapper(), BatchRecord.class);
    }
}

class MsgPackRestClientBatchRecordBodyMapper extends ASTestMapper {

    public MsgPackRestClientBatchRecordBodyMapper() {
        super(MsgPackConverter.getASMsgPackObjectMapper(), BatchRecord.class);
    }
}
