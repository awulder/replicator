package com.booking.replication.pipeline;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@SuppressWarnings("unused")
public interface Pipeline<Input, Output> {

    Pipeline<Input, Output> start() throws InterruptedException;

    Pipeline<Input, Output> wait(long timeout, TimeUnit unit) throws InterruptedException;

    void join() throws InterruptedException;

    void stop() throws InterruptedException;

    void onException(Consumer<Exception> handler);

    boolean push(Input input);

    int size();

    static <Input> PipelineBuilderSourceTap<Input, Input> builder() {
        return new PipelineBuilder<>();
    }
}
