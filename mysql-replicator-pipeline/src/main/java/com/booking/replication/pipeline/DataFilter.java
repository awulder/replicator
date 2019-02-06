package com.booking.replication.pipeline;

import java.util.function.Function;
import java.util.function.Predicate;

public interface DataFilter<Input, Output>  {
    
    DataFilter<Input, Output> filter(Predicate<Input> filter);

    SinkPostProcess<Input, Output> to(Function<Output, Boolean> to);

}
