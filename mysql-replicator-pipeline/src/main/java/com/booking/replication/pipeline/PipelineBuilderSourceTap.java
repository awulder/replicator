package com.booking.replication.pipeline;

import java.util.Deque;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface PipelineBuilderSourceTap<Input, Output> {

    PipelineBuilderSourceTap<Input, Output> threads(int threads);

    PipelineBuilderSourceTap<Input, Output> tasks(int tasks);

    PipelineBuilderSourceTap<Input, Output> partitioner(BiFunction<Input, Integer, Integer> partitioner);

    PipelineBuilderSourceTap<Input, Output> queue();

    PipelineBuilderSourceTap<Input, Output> queue(Class<? extends Deque> queueType);

    StreamsBuilderFilter<Input, Output> fromPull(Function<Integer, Input> supplier);

    StreamsBuilderFilter<Input, Output> fromPush();
}
