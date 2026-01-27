/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.rocksdb;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalValueState;

import org.apache.flink.runtime.state.internal.KeyAccessibleState;
import org.apache.flink.util.FlinkRuntimeException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;

/**
 * {@link ValueState} implementation that stores state in RocksDB.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 */
class RocksDBValueState<K, N, V> extends AbstractRocksDBState<K, N, V>
        implements InternalValueState<K, N, V>, KeyAccessibleState<K, V> {

    /** Serializer used from async threads**/
    private final ThreadLocal<DataOutputSerializer> asyncAccessDataOutputSerializer;
    private final ThreadLocal<DataInputDeserializer> asyncAccessDataInputSerializer;
    private final ThreadLocal<TypeSerializer<V>> asyncAccessValueSerializer;
    private final ThreadLocal<TypeSerializer<K>> asyncAccessKeySerializer;
    private final ThreadLocal<TypeSerializer<N>> asyncAccessNamespaceSerializer;

    /**
     * Creates a new {@code RocksDBValueState}.
     *
     * @param columnFamily The RocksDB column family that this state is associated to.
     * @param namespaceSerializer The serializer for the namespace.
     * @param valueSerializer The serializer for the state.
     * @param defaultValue The default value for the state.
     * @param backend The backend for which this state is bind to.
     */
    private RocksDBValueState(
            ColumnFamilyHandle columnFamily,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue,
            RocksDBKeyedStateBackend<K> backend) {

        super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);

        this.asyncAccessDataOutputSerializer = ThreadLocal.withInitial(() -> new DataOutputSerializer(128));
        this.asyncAccessDataInputSerializer = ThreadLocal.withInitial(() -> new DataInputDeserializer());
        this.asyncAccessValueSerializer = ThreadLocal.withInitial(valueSerializer::duplicate);
        this.asyncAccessKeySerializer = ThreadLocal.withInitial(backend.getKeySerializer()::duplicate);
        this.asyncAccessNamespaceSerializer = ThreadLocal.withInitial(namespaceSerializer::duplicate);
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return backend.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public V value() throws IOException {
        try {
            byte[] valueBytes =
                    backend.db.get(columnFamily, serializeCurrentKeyWithGroupAndNamespace());

            if (valueBytes == null) {
                return getDefaultValue();
            }
            dataInputView.setBuffer(valueBytes);
            return valueSerializer.deserialize(dataInputView);
        } catch (RocksDBException e) {
            throw new IOException("Error while retrieving data from RocksDB.", e);
        }
    }

    // main thread
    public V valueOfKey(K key) throws IOException {
        try {
            byte[] valueBytes =
                    backend.db.get(columnFamily, mySerializeCurrentKeyWithGroupAndNamespace(key));

            if (valueBytes == null) {
                return getDefaultValue();
            }
            DataInputDeserializer dataInputViewLocal = asyncAccessDataInputSerializer.get();
            dataInputViewLocal.setBuffer(valueBytes);
            return asyncAccessValueSerializer.get().deserialize(dataInputViewLocal);
        } catch (RocksDBException e) {
            throw new IOException("Error while retrieving data from RocksDB.", e);
        }
    }

    @Override
    // main thread
    public V get(K key) throws IOException, RocksDBException {
        //K oldKey = backend.getCurrentKey();
        // N oldNamespace = currentNamespace;
        //backend.setCurrentKey(key);
        //setCurrentNamespace(namespace); // no need to change the namespace since I do not have windows
        V value = valueOfKey(key);
        //backend.setCurrentKey(oldKey);
        //setCurrentNamespace(oldNamespace);
        return value;
    }

    @Override
    // prefetching thread
    public byte[] getRawBytes(K key) throws IOException, RocksDBException {
        byte[] serializedKey = mySerializeCurrentKeyWithGroupAndNamespace(key);
        return backend.db.get(columnFamily, serializedKey);
    }

    @Override
    // main thread
    public V deserializeRawBytes(byte[] valueBytes) throws IOException {
        if (valueBytes == null) {
            return getDefaultValue();
        }
        // safe to use the shared dataInputView since this method is called by the main thread only
        DataInputDeserializer dataInputViewLocal = asyncAccessDataInputSerializer.get();
        dataInputViewLocal.setBuffer(valueBytes);
        return asyncAccessValueSerializer.get().deserialize(dataInputViewLocal);
    }

    // prefetching thread
    private byte[] mySerializeCurrentKeyWithGroupAndNamespace(K key) throws IOException {
        // build composite RocksDB key without touching backend.currentKey/namespace

        // 1) write key-group prefix
        final int keyGroupId = KeyGroupRangeAssignment.assignToKeyGroup(
                key, backend.getNumberOfKeyGroups());

        DataOutputSerializer out = asyncAccessDataOutputSerializer.get();

        out.clear();

        CompositeKeySerializationUtils.writeKeyGroup(keyGroupId, backend.getKeyGroupPrefixBytes(), out);

        // 2) write user key
        asyncAccessKeySerializer.get().serialize(key, out);

        // 3) write namespace
        asyncAccessNamespaceSerializer.get().serialize(currentNamespace, out);

        byte[] res = out.getCopyOfBuffer();

        return res;
    }

    @Override
    public void update(V value) throws IOException {
        if (value == null) {
            clear();
            return;
        }

        try {
            backend.db.put(
                    columnFamily,
                    writeOptions,
                    serializeCurrentKeyWithGroupAndNamespace(),
                    serializeValue(value));
        } catch (RocksDBException e) {
            throw new IOException("Error while adding data to RocksDB", e);
        }
    }

    @Override
    // main thread
    public void update(K key, V value) throws IOException, RocksDBException {
        K oldKey = backend.getCurrentKey();
        //N oldNamespace = namespace;
        backend.setCurrentKey(key);
        //setCurrentNamespace(namespace);
        updateAsync(key, value);
        if (oldKey != null) {
            backend.setCurrentKey(oldKey);
        }
        //setCurrentNamespace(oldNamespace);
    }

    @Override
    // async threads and main
    public void updateAsync(K key, V value) throws IOException {
        if (value == null) {
            //clear
            try {
                backend.db.delete(
                        columnFamily, writeOptions, mySerializeCurrentKeyWithGroupAndNamespace(key));
            } catch (RocksDBException e) {
                throw new FlinkRuntimeException("Error while removing entry from RocksDB", e);
            }
            return;
        }

        try{
            DataOutputSerializer myDataOutput = asyncAccessDataOutputSerializer.get();
            myDataOutput.clear();
            asyncAccessValueSerializer.get().serialize(value, myDataOutput);
            byte[] serializedVal = myDataOutput.getCopyOfBuffer();
            backend.db.put(
                    columnFamily,
                    writeOptions,
                    mySerializeCurrentKeyWithGroupAndNamespace(key),
                    serializedVal);
        } catch (RocksDBException e) {
            throw new IOException("Error while adding data to RocksDB", e);
        }
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            RocksDBKeyedStateBackend<K> backend) {
        return (IS)
                new RocksDBValueState<>(
                        registerResult.f0,
                        registerResult.f1.getNamespaceSerializer(),
                        registerResult.f1.getStateSerializer(),
                        stateDesc.getDefaultValue(),
                        backend);
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            IS existingState) {
        return (IS)
                ((RocksDBValueState<K, N, SV>) existingState)
                        .setNamespaceSerializer(registerResult.f1.getNamespaceSerializer())
                        .setValueSerializer(registerResult.f1.getStateSerializer())
                        .setDefaultValue(stateDesc.getDefaultValue())
                        .setColumnFamily(registerResult.f0);
    }
}
