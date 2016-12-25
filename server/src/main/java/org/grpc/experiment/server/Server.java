package org.grpc.experiment.server;

import org.grpc.experiment.stubs.GreeterGrpc;
import org.grpc.experiment.stubs.GreeterOuterClass;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.log4j.Logger;

public class Server {

    private static final int PORT = 8129;
    private static final Logger LOG = Logger.getLogger(Server.class);
    private io.grpc.Server server;

    private static class GreeterImpl implements GreeterGrpc.Greeter {

        @Override
        public void sayHello(GreeterOuterClass.HelloRequest request, StreamObserver<GreeterOuterClass.HelloReply> responseObserver) {
            GreeterOuterClass.HelloReply reply =
                    GreeterOuterClass.HelloReply.newBuilder().
                            setMessage("Hello " + request.getName()).build();

            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private void start() throws Exception {
        server = ServerBuilder.forPort(PORT)
                .addService(GreeterGrpc.bindService(new GreeterImpl()))
                .build()
                .start();

        LOG.info("Server started, listening on " + PORT);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may has been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                Server.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    public static void main(String[] args) {
        final Server srv = new Server();
        try {
            srv.start();
            srv.blockUntilShutdown();
        } catch (Exception e) {
            LOG.fatal("Fatal Error in Server: " + e.getMessage(), e);
        }
    }
}
