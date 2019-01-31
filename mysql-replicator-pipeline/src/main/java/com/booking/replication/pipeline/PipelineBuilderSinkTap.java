package com.booking.replication.pipeline;

import java.util.function.Function;

public interface PipelineBuilderSinkTap<Input, Output> {

    <To> PipelineBuilderSinkTap<Input, To> process(Function<Output, To> process);

    StreamsBuilderPost<Input, Output> to(Function<Output, Boolean> to);
}
