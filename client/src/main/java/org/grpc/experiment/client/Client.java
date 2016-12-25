package org.grpc.experiment.client;

import org.grpc.experiment.stubs.GreeterGrpc;
import org.grpc.experiment.stubs.GreeterOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Client {

    private static final int PORT = 8129;
    private static final Logger LOG = Logger.getLogger(Client.class);

    private static AtomicInteger messagesPerSecond = new AtomicInteger();
    private final static int NUMBER_OF_TIMES = 10000000;
    private final static int NUMBER_OF_CLIENTS = 1;
    private final String MSG = "TEST DATA ... DATA TESTTEST DATA ... DATA TESTTEST DATA ... DATA TESTTEST DATA ... DATA TESTTEST DATA ... DATA TESTTEST DATA ... DATA TESTTEST DATA ... DATA TEST";

    private final ManagedChannel channel;
    private final GreeterGrpc.GreeterBlockingStub blockingStub;

    public Client(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext(true)
                .build();
        blockingStub = GreeterGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * Say hello to server.
     */
    public String greet(String name) {
        //LOG.info("Will try to greet " + name + " ...");
        GreeterOuterClass.HelloRequest request = GreeterOuterClass.HelloRequest.newBuilder().setName(name).build();
        GreeterOuterClass.HelloReply response;
        try {
            response = blockingStub.sayHello(request);
        } catch (StatusRuntimeException e) {
            LOG.warn("RPC failed: " + e.getStatus());
            return null;
        }
        //LOG.info("Greeting: " + response.getMessage());
        return response.getMessage();
    }

    public String greet() {
        return greet(MSG);
    }

    private static void runImpl() throws InterruptedException {
        Client client = new Client("localhost", PORT);
        try {

            for (int i = 0; i < NUMBER_OF_TIMES; ++i) {
                client.greet();
                messagesPerSecond.incrementAndGet();
            }

        } finally {
            client.shutdown();
        }
    }

    private static void run() throws Exception {
        messagesPerSecond.set(0);
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

        scheduledExecutorService.scheduleAtFixedRate((Runnable) () -> trackTime(), 1, 1, TimeUnit.SECONDS);

        ArrayList<Future<?>> concurrentClients = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_CLIENTS);

        for (int i = 0; i < NUMBER_OF_CLIENTS; ++i) {
            concurrentClients.add(executorService.submit((Runnable)() -> {
                try {
                    runImpl();
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }));
        }

        for (Future<?> task : concurrentClients) {
            task.get();
        }

        scheduledExecutorService.shutdown();
        executorService.shutdown();
        trackTime();
    }

    private static void trackTime() {
        int rate = messagesPerSecond.getAndSet(0);
        LOG.info("Messages per second: " + rate);
    }

    public static void main(String[] args) {
        try {
            run();
        } catch (Exception e) {
            LOG.fatal("Fatal error in client: " + e.getMessage(), e);
        }
    }
}
