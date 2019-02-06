package com.booking.replication.pipeline;

import java.util.function.Function;

public interface SinkTap<Input, Output>  {
    SinkPostProcess<Input, Output> to(Function<Output, Boolean> to);
}
