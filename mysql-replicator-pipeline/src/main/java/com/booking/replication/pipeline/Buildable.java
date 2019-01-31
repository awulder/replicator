package com.booking.replication.pipeline;

public interface Buildable<Input, Output> {
    Pipeline<Input, Output> build();
}
