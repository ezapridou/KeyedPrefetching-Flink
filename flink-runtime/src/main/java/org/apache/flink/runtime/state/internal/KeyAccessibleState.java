package org.apache.flink.runtime.state.internal;

public interface KeyAccessibleState<K, T>{
    /**
     * Returns the value associated with the given key.
     *
     * @param key The key for which the value is to be retrieved.
     * @return The value associated with the given key or {@code null} if no value exists.
     * @throws Exception The method may forward exceptions thrown internally.
     */
    T get(K key) throws Exception;

    /**
     * Updates the state with the given key.
     *
     * @param key The key for which the state is to be updated.
     * @param value The new values to be added to the state.
     * @throws Exception The method may forward exceptions thrown internally.
     */
    void update(K key, T value) throws Exception;

    /**
     * Returns the value associates with the given key in byte[]
     * This method does not update the current key of the state, so it is thread-safe.
     *
     * @param key
     * @return
     * @throws Exception
     */
    byte[] getRawBytes(K key) throws Exception;

    /**
     * Deserializes a value from byte[] to List<T>
     * This method does not update the current key of the state, so it is thread-safe.
     *
     * @param bytes
     * @return
     * @throws Exception
     */
    T deserializeRawBytes(byte[] bytes) throws Exception;

    /**
     * Updates the state with the given key.
     * We use this method when we want to do the update using a thread different than the main one.
     * This method does not update the current key of the state, so it is thread-safe.
     *
     * @param key The key for which the state is to be updated.
     * @param values The new values to be added to the state.
     * @throws Exception The method may forward exceptions thrown internally.
     */
    void updateAsync(K key, T values) throws Exception;
}
