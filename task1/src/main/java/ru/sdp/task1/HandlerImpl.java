package ru.sdp.task1;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class HandlerImpl implements Handler {

    private final AtomicInteger counter = new AtomicInteger();
    private final Client client;
    private final int timeout = 15000;
    private final ExecutorService pool = Executors.newFixedThreadPool(2);

    public HandlerImpl(Client client) {
        this.client = client;
    }

    @Override
    public ApplicationStatusResponse performOperation(String id) {

        CompletableFuture<Response> completableFuture1 = CompletableFuture.supplyAsync(() -> client.getApplicationStatus1(id), pool);
        CompletableFuture<Response> completableFuture2 = CompletableFuture.supplyAsync(() -> client.getApplicationStatus2(id), pool);

        CompletableFuture<Object> result = CompletableFuture.anyOf(completableFuture1, completableFuture2);
        try {
            Response clientResponse = (Response) result.get(timeout, TimeUnit.MILLISECONDS);
            return switch (clientResponse) {
                case Response.Success success ->
                        new ApplicationStatusResponse.Success(success.applicationId(), success.applicationStatus());
                case Response.Failure failure -> new ApplicationStatusResponse.Failure(null, counter.incrementAndGet());
                case Response.RetryAfter retryAfter ->
                        new ApplicationStatusResponse.Failure(retryAfter.delay(), counter.incrementAndGet());
            };
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            // TODO log.error (e.getMessage(), e);
            return new ApplicationStatusResponse.Failure(null, counter.incrementAndGet());
        }
    }
}
