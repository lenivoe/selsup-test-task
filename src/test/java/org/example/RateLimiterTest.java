package org.example;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

class RateLimiterTest {
    @Test
    @SuppressWarnings("BusyWait")
    void test() throws InterruptedException {
        int requestsLimit = 37;
        var duration = Duration.of(50, ChronoUnit.MILLIS);

        var rateLimiter = new CrptApi.RateLimiter(duration, requestsLimit);

        int tasksCount = 10000;
        var completedTasksCounter = new AtomicInteger(0);

        var futures = Stream
                .generate(() -> CompletableFuture.runAsync(() -> {
                    try {
                        rateLimiter.acquire();
                        completedTasksCounter.incrementAndGet();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }))
                .limit(tasksCount)
                .toArray(CompletableFuture[]::new);

        Thread.sleep(duration.toMillis() / 2);
        while (completedTasksCounter.get() < tasksCount) {
            Assertions.assertEquals(0, completedTasksCounter.get() % requestsLimit);
            Thread.sleep(duration.toMillis());
        }

        CompletableFuture.allOf(futures).join();

        Assertions.assertEquals(tasksCount, completedTasksCounter.get());
    }
}