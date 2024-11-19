package com.michelin.kafka;

@FunctionalInterface
public interface RecordProcessor<T, E extends Exception> {
    /**
     * Performs this operation on the given argument.
     *
     * @param t the input argument
     */
    void processRecord(T t) throws E;
}


