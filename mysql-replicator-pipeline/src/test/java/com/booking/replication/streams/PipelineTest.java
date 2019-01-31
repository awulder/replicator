package com.booking.replication.streams;

import com.booking.replication.pipeline.Pipeline;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PipelineTest {
    @Test
    public void testFromPull() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();

        Pipeline.<Integer>builder()
                .queue()
                .fromPull((task) -> {
                    count1.incrementAndGet();
                    return ThreadLocalRandom.current().nextInt();
                })
                .to((value) -> {
                    count2.incrementAndGet();
                    return true;
                })
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();

        assertEquals(count1.get(), count2.get());
    }

    @Test
    public void testFromPush() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();

        int number = ThreadLocalRandom.current().nextInt();

        Pipeline<Integer, Integer> pipeline = Pipeline.<Integer>builder()
                .queue()
                .fromPush()
                .to((value) -> {
                    count1.incrementAndGet();
                    assertEquals(number, value.intValue());
                    return true;
                })
                .build()
                .start();

        pipeline.push(number);
        pipeline.wait(1L, TimeUnit.SECONDS).stop();

        assertEquals(1, count1.get());
    }

    @Test
    public void testFilter() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();

        Pipeline.<Integer>builder()
                .queue()
                .fromPull((task) -> {
                    int value = ThreadLocalRandom.current().nextInt();

                    if (value > 0) {
                        count1.incrementAndGet();
                    }

                    return value;
                })
                .filter((value) -> value > 0)
                .to((value) -> {
                    count2.incrementAndGet();
                    assertTrue(value > 0);
                    return true;
                })
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();

        assertEquals(count1.get(), count2.get());
    }

    @Test
    public void testProcess() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();

        Pipeline.<Integer>builder()
                .queue()
                .fromPull((task) -> {
                    count1.incrementAndGet();
                    return ThreadLocalRandom.current().nextInt();
                })
                .process(Object::toString)
                .to((value) -> {
                    count2.incrementAndGet();
                    assertTrue(String.class.isInstance(value));
                    return true;
                })
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();

        assertEquals(count1.get(), count2.get());
    }

    @Test
    public void testMultipleProcess() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();

        Pipeline.<Integer>builder()
                .queue()
                .fromPull((task) -> {
                    count1.incrementAndGet();
                    return ThreadLocalRandom.current().nextInt();
                })
                .process(Object::toString)
                .process((value) -> String.format("value=%s", value))
                .to((value) -> {
                    count2.incrementAndGet();
                    assertTrue(value.startsWith("value="));
                    return true;
                })
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();

        assertEquals(count1.get(), count2.get());
    }

    @Test
    public void testThreads() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();

        Pipeline<Integer, String> pipeline = Pipeline.<Integer>builder()
                .tasks(10)
                .threads(10)
                .queue()
                .fromPull((task) -> {
                    count1.incrementAndGet();
                    return ThreadLocalRandom.current().nextInt();
                })
                .process(Object::toString)
                .process((value) -> String.format("value=%s", value))
                .to((value) -> {
                    count2.incrementAndGet();
                    return true;
                })
                .build()
                .start();

        while (count2.get() < count1.get()) {
            pipeline.wait(1L, TimeUnit.SECONDS);
        }

        pipeline.stop();

        assertEquals(count1.get(), count2.get());
    }

    @Test
    public void testMultipleTo() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();
        AtomicInteger count3 = new AtomicInteger();

        Pipeline.<Integer>builder()
                .queue()
                .fromPull((task) -> {
                    int value = ThreadLocalRandom.current().nextInt();

                    if (value > 0) {
                        count1.incrementAndGet();
                    }

                    return value;
                })
                .filter(value -> value > 0)
                .process(Object::toString)
                .to((value) -> {
                    count2.incrementAndGet();
                    assertTrue(String.class.isInstance(value));
                    return true;
                })
                .to((value) -> {
                    count3.incrementAndGet();
                    assertTrue(Integer.parseInt(value) > 0);
                    return true;
                })
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();

        assertEquals(count1.get(), count2.get());
        assertEquals(count1.get(), count3.get());
    }

    @Test
    public void testPost() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();
        AtomicInteger count3 = new AtomicInteger();

        Pipeline.<Integer>builder()
                .queue()
                .fromPull((task) -> {
                    int value = ThreadLocalRandom.current().nextInt();

                    if (value > 0) {
                        count1.incrementAndGet();
                    }

                    return value;
                })
                .filter(value -> value > 0)
                .process(Object::toString)
                .to((value) -> {
                    count2.incrementAndGet();
                    assertTrue(String.class.isInstance(value));
                    return true;
                })
                .post((value) -> {
                    count3.incrementAndGet();
                    assertTrue(value > 0);
                })
                .build()
                .start()
                .wait(1L, TimeUnit.SECONDS)
                .stop();

        assertEquals(count1.get(), count2.get());
        assertEquals(count1.get(), count3.get());
    }

    @Test
    public void testOnException() throws InterruptedException {
        int number = ThreadLocalRandom.current().nextInt();

        Pipeline<Integer, Integer> pipeline = Pipeline.<Integer>builder()
                .queue()
                .fromPush()
                .to((value) -> {
                    throw new NullPointerException();
                })
                .build()
                .start();

        pipeline.onException((exception) -> assertTrue(NullPointerException.class.isInstance(exception)));
        pipeline.push(number);
        pipeline.wait(1L, TimeUnit.SECONDS).stop();
    }

    @Test
    public void testChain() throws InterruptedException {
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();
        AtomicInteger count3 = new AtomicInteger();
        AtomicInteger count4 = new AtomicInteger();

        Pipeline<String, String> pipelineDestination = Pipeline.<String>builder()
                .tasks(10)
                .threads(10)
                .queue()
                .fromPush()
                .to(output -> {
                    count3.incrementAndGet();
                    return true;
                })
                .post(input -> count4.incrementAndGet())
                .build();

        Pipeline<Integer, String> pipelineSource = Pipeline.<Integer>builder()
                .fromPush()
                .process(Object::toString)
                .process((value) -> {
                    count2.incrementAndGet();
                    return String.format("value=%s", value);
                })
                .to(pipelineDestination::push)
                .build();

        pipelineDestination.start();
        pipelineSource.start();

        for (int index = 0; index < 20; index++) {
            count1.incrementAndGet();
            pipelineSource.push(ThreadLocalRandom.current().nextInt());
        }

        while (count4.get() < count1.get()) {
            pipelineSource.wait(1L, TimeUnit.SECONDS);
        }

        pipelineDestination.stop();
        pipelineSource.stop();

        assertEquals(count1.get(), count2.get());
        assertEquals(count1.get(), count3.get());
        assertEquals(count1.get(), count4.get());
    }
}
