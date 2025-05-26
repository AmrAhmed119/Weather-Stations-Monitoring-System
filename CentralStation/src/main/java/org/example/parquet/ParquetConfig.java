package org.example.parquet;

public enum ParquetConfig {
    BATCH_SIZE(10),
    OUTPUT_DIRECTORY("/app/parquet/");

    private final Object value;

    ParquetConfig(Object value) {
        this.value = value;
    }

    public int getInt() {
        if (value instanceof Integer i) {
            return i;
        }
        throw new IllegalStateException("Value is not an int");
    }

    public String getString() {
        if (value instanceof String s) {
            return s;
        }
        throw new IllegalStateException("Value is not a String");
    }
}