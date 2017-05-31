package io.vertx.streams.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class ConsumerStream<T> {

  private static final int DISCONNECTED = 0, CONNECTING = 1, CONNECTED = 2;

  private final ConsumerImpl<T> consumer;
  private final ReadStreamImpl readStream;
  private final Handler<AsyncResult<WriteStream<T>>> writeStreamHandler;
  private String localAddress;
  private int status = DISCONNECTED;

  class ReadStreamImpl implements ReadStream<T> {

    final Handler<AsyncResult<ReadStream<T>>> doneHandler;
    Handler<T> handler;
    Handler<Void> endHandler;

    public ReadStreamImpl(Handler<AsyncResult<ReadStream<T>>> doneHandler) {
      this.doneHandler = doneHandler;
    }

    @Override
    public ReadStream<T> exceptionHandler(Handler<Throwable> handler) {
      return this;
    }

    @Override
    public ReadStream<T> handler(Handler<T> h) {
      handler = h;
      return this;
    }

    @Override
    public ReadStream<T> pause() {
      return this;
    }

    @Override
    public ReadStream<T> resume() {
      return this;
    }

    @Override
    public ReadStream<T> endHandler(Handler<Void> handler) {
      endHandler = handler;
      return this;
    }
  }

  public ConsumerStream(ConsumerImpl<T> consumer,
                        Handler<AsyncResult<ReadStream<T>>> readStreamHandler,
                        Handler<AsyncResult<WriteStream<T>>> writeStreamHandler) {
    this.consumer = consumer;
    this.readStream = readStreamHandler != null ? new ReadStreamImpl(readStreamHandler) : null;
    this.writeStreamHandler = writeStreamHandler;
  }

  public void subscribe() {
    subscribe(null, new DeliveryOptions());
  }

  public void subscribe(Object body, DeliveryOptions options) {
    if (status != DISCONNECTED) {
      throw new IllegalArgumentException();
    }
    this.status = CONNECTING;
    if (readStream != null) {
      Future<ReadStream<T>> fut = Future.future();
      localAddress = consumer.transport.<T>bind(ar -> {
        if (ar.failed()) {
          status = DISCONNECTED;
          fut.fail(ar.cause());
        } else {
          ReadStream<T> stream = ar.result();
          stream.pause();
          fut.complete(stream);
        }
      });
      options.addHeader("stream", "open");
      options.addHeader("addr", localAddress);
      consumer.bus.send(consumer.address, body, options, ar1 -> {
        if (ar1.failed()) {
          throw new UnsupportedOperationException("Implement me");
        } else {
          fut.setHandler(ar2 -> {
            if (ar2.succeeded()) {
              ReadStream<T> stream = ar2.result();
              readStream.doneHandler.handle(Future.succeededFuture(readStream));
              stream.handler(readStream.handler);
              stream.endHandler(readStream.endHandler);
              stream.resume();
            } else {
              readStream.doneHandler.handle(Future.failedFuture(ar2.cause()));
            }
          });
        }
      });
    } else if (writeStreamHandler != null) {
      options.addHeader("stream", "open");
      consumer.bus.send(consumer.address, body, options, ar1 -> {
        if (ar1.failed()) {
          throw new UnsupportedOperationException("Implement me");
        } else {
          String addr = "" + ar1.result().body();
          consumer.transport.<T>connect(addr, writeStreamHandler);
        }
      });
    }
  }
}
