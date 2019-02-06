package com.booking.replication.pipeline;

import java.util.Deque;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface SourceTap<Input, Output> {

    SourceTap<Input, Output> threads(int threads);

    SourceTap<Input, Output> tasks(int tasks);

    SourceTap<Input, Output> partitioner(BiFunction<Input, Integer, Integer> partitioner);

    SourceTap<Input, Output> queue();

    SourceTap<Input, Output> queue(Class<? extends Deque> queueType);

    PipelineConfigurator<Input, Output> fromPull(Function<Integer, Input> supplier);

    PipelineConfigurator<Input, Output> setInputAsCallback();
}
