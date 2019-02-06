package com.booking.replication.pipeline;

import java.util.function.Function;

public interface DoFn<Input, Output> {
    <To> PipelineConfigurator<Input, To> process(Function<Output, To> process);
}
