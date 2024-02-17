package ru.sdp.task2;

import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;

public class HandlerImpl implements Handler {

    private final int consumerCount = 10;
    private final ExecutorService consumerPool = Executors.newFixedThreadPool(consumerCount);
    private final ExecutorService producerPool = Executors.newFixedThreadPool(2);
    private final Duration timeout = Duration.ofMillis(5000);

    private final ConcurrentLinkedQueue<Message> queue = new ConcurrentLinkedQueue<>();

    private final Client client;

    private volatile boolean flag;

    public HandlerImpl(Client client) {
        this.client = client;
    }

    @Override
    public Duration timeout() {
        return timeout;
    }

    @Override
    public void performOperation() {
        CompletableFuture.supplyAsync(client::readData, consumerPool).thenAccept(event -> {
            for (Address address : event.recipients()) {
                queue.add(new Message(address, event.payload()));
            }
        });
    }

    public void startConsumer() {
        flag = true;
        for (int i = 0; i < consumerCount; i++) {
            consumerPool.submit(() -> {
                while (flag) {
                    final Message message = queue.poll();
                    if (message != null) {
                        Result result = client.sendData(message.address(), message.payload());
                        if (result == Result.REJECTED) {
                            Timer timer = new Timer();
                            timer.schedule(new TimerTask() {
                                @Override
                                public void run() {
                                    queue.add(message);
                                }
                            }, timeout.toMillis());
                        }
                    }
                }
            });
        }
    }

    public void StopConsumer() {
        flag = false;
    }
}
