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

public final class PipelineBuilder<Input, Output> implements

        PipelineBuilderSourceTap<Input, Output>,

        PipelineBuilderSinkTap<Input, Output>,

        StreamsBuilderFilter<Input, Output>,

        StreamsBuilderPost<Input, Output>,

        Buildable<Input, Output> {

    private static final Logger LOG = Logger.getLogger(PipelineBuilder.class.getName());

    private int threads;
    private int tasks;
    private BiFunction<Input, Integer, Integer> partitioner;
    private Class<? extends Deque> queueType;
    private Function<Integer, Input> from;
    private Predicate<Input> filter;
    private Function<Input, Output> process;
    private Function<Output, Boolean> to;
    private BiConsumer<Input, Integer> post;

    private PipelineBuilder(
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

    PipelineBuilder() {
        this(0, 1, null, null, null, null, null, null, null);
    }

    @Override
    public final PipelineBuilderSourceTap<Input, Output> threads(int threads) {
        if (threads > 0) {
            this.threads = threads;
            return this;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public final PipelineBuilderSourceTap<Input, Output> tasks(int tasks) {
        if (tasks > 0) {
            this.tasks = tasks;
            return this;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public PipelineBuilderSourceTap<Input, Output> partitioner(BiFunction<Input, Integer, Integer> partitioner) {
        Objects.requireNonNull(partitioner);
        this.partitioner = partitioner;
        return this;
    }

    @Override
    public PipelineBuilderSourceTap<Input, Output> queue() {
        return this.queue(ConcurrentLinkedDeque.class);
    }

    @Override
    public PipelineBuilderSourceTap<Input, Output> queue(Class<? extends Deque> queueType) {
        Objects.requireNonNull(queueType);
        this.queueType = queueType;
        return this;
    }

    @Override
    public final StreamsBuilderFilter<Input, Output> fromPull(Function<Integer, Input> supplier) {
        Objects.requireNonNull(supplier);
        this.from = supplier;
        return this;
    }

    @Override
    public final StreamsBuilderFilter<Input, Output> fromPush() {
        this.from = null;
        return this;
    }

    @Override
    public final StreamsBuilderFilter<Input, Output> filter(Predicate<Input> filter) {
        Objects.requireNonNull(filter);
        return new PipelineBuilder<>(
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
    public final <To> PipelineBuilderSinkTap<Input, To> process(Function<Output, To> process) {
        Objects.requireNonNull(process);
        return new PipelineBuilder<>(
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
    public final StreamsBuilderPost<Input, Output> to(Function<Output, Boolean> to) {
        Objects.requireNonNull(to);
        return new PipelineBuilder<>(
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
        return new PipelineBuilder<>(
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
