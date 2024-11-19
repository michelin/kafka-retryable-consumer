package com.michelin.kafka.configuration;

public class KafkaConfigurationException extends Exception{
    public KafkaConfigurationException() {
        super();
    }

    public KafkaConfigurationException(String message) {
        super(message);
    }

    public KafkaConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaConfigurationException(Throwable cause) {
        super(cause);
    }

    protected KafkaConfigurationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
