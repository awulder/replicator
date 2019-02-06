package com.booking.replication.pipeline;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public interface SinkPostProcess<Input, Output> {
    SinkPostProcess<Input, Output> to(Function<Output, Boolean> to);

    Buildable<Input, Output> post(Consumer<Input> post);

    Buildable<Input, Output> post(BiConsumer<Input, Integer> post);

    Pipeline<Input, Output> build();
}
