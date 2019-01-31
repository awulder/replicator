package com.booking.replication.pipeline;

import java.util.function.Function;
import java.util.function.Predicate;

public interface StreamsBuilderFilter<Input, Output> {

    StreamsBuilderFilter<Input, Output> filter(Predicate<Input> filter);

    <To> PipelineBuilderSinkTap<Input, To> process(Function<Output, To> process);

    StreamsBuilderPost<Input, Output> to(Function<Output, Boolean> to);
}
