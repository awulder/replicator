package com.booking.replication.pipeline;

import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Logger;

public final class PipelineConfigurator<Input, Output> implements

        // every pipe has source & sink
        SourceTap<Input, Output>,
        SinkTap<Input, Output>,

        // optionally data transforms and filters
        DoFn<Input, Output>,
        DataFilter<Input, Output>,

        // optionally sink postprocessing
        SinkPostProcess<Input, Output>,

        // build()
        Buildable<Input, Output> {

    private static final Logger LOG = Logger.getLogger(PipelineConfigurator.class.getName());

    private int threads;
    private int tasks;
    private BiFunction<Input, Integer, Integer> partitioner;
    private Class<? extends Deque> queueType;
    private Function<Integer, Input> from;
    private Predicate<Input> filter;
    private Function<Input, Output> process;
    private Function<Output, Boolean> to;
    private BiConsumer<Input, Integer> post;

    private PipelineConfigurator(
            int threads,
            int tasks,
            BiFunction<Input, Integer, Integer> partitioner,
            Class<? extends Deque> queueType,
            Function<Integer, Input> from,
            Predicate<Input> filter,
            Function<Input, Output> process,
            Function<Output, Boolean> to,
            BiConsumer<Input, Integer> post) {
        this.threads = threads;
        this.tasks = tasks;
        this.partitioner = partitioner;
        this.queueType = queueType;
        this.from = from;
        this.filter = filter;
        this.process = process;
        this.to = to;
        this.post = post;
    }

    PipelineConfigurator() {
        this(0, 1, null, null, null, null, null, null, null);
    }

    @Override
    public final SourceTap<Input, Output> threads(int threads) {
        if (threads > 0) {
            this.threads = threads;
            return this;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public final SourceTap<Input, Output> tasks(int tasks) {
        if (tasks > 0) {
            this.tasks = tasks;
            return this;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public SourceTap<Input, Output> partitioner(BiFunction<Input, Integer, Integer> partitioner) {
        Objects.requireNonNull(partitioner);
        this.partitioner = partitioner;
        return this;
    }

    @Override
    public SourceTap<Input, Output> queue() {
        return this.queue(ConcurrentLinkedDeque.class);
    }

    @Override
    public SourceTap<Input, Output> queue(Class<? extends Deque> queueType) {
        Objects.requireNonNull(queueType);
        this.queueType = queueType;
        return this;
    }

    @Override
    public final PipelineConfigurator<Input, Output> fromPull(Function<Integer, Input> supplier) {
        Objects.requireNonNull(supplier);
        this.from = supplier;
        return this;
    }

    @Override
    public final PipelineConfigurator<Input, Output> setInputAsCallback() {
        this.from = null;
        return this;
    }

    @Override
    public final PipelineConfigurator<Input, Output> filter(Predicate<Input> filter) {
        Objects.requireNonNull(filter);
        return new PipelineConfigurator<>(
                this.threads,
                this.tasks,
                this.partitioner,
                this.queueType,
                this.from,
                input -> (this.filter == null || this.filter.test(input)) && filter.test(input),
                null,
                null,
                null
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public final <To> PipelineConfigurator<Input, To> process(Function<Output, To> process) {
        Objects.requireNonNull(process);
        return new PipelineConfigurator<>(
                this.threads,
                this.tasks,
                this.partitioner,
                this.queueType,
                this.from,
                this.filter,
                input -> {
                    Output output = (this.process != null)?(this.process.apply(input)):((Output) input);
                    if (output != null) {
                        return process.apply(output);
                    } else {
                        return null;
                    }
                },
                null,
                null
        );
    }

    @Override
    public final SinkPostProcess<Input, Output> to(Function<Output, Boolean> to) {
        Objects.requireNonNull(to);
        return new PipelineConfigurator<>(
                this.threads,
                this.tasks,
                this.partitioner,
                this.queueType,
                this.from,
                this.filter,
                this.process,
                output -> {
                    boolean result = true;

                    if (this.to != null) {
                        result = this.to.apply(output);
                    }

                    if (output != null && result) {
                        result = to.apply(output);
                    }

                    return result;
                },
                null
        );
    }

    @Override
    public final Buildable<Input, Output> post(Consumer<Input> post) {
        Objects.requireNonNull(post);
        return this.post((input, task) -> post.accept(input));
    }

    @Override
    public final Buildable<Input, Output> post(BiConsumer<Input, Integer> post) {
        Objects.requireNonNull(post);
        return new PipelineConfigurator<>(
                this.threads,
                this.tasks,
                this.partitioner,
                this.queueType,
                this.from,
                this.filter,
                this.process,
                this.to,
                (input, task) -> {
                    if (this.post != null) {
                        this.post.accept(input, task);
                    }

                    if (input != null) {
                        post.accept(input, task);
                    }
                }
        );
    }

    @Override
    public final Pipeline<Input, Output> build() {
        return new PipelineImplementation<>(
                this.threads,
                this.tasks,
                this.partitioner,
                this.queueType,
                this.from,
                this.filter,
                this.process,
                this.to,
                this.post
        );
    }
}
