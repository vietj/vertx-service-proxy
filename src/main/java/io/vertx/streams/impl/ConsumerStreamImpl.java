package io.vertx.streams.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.streams.ReadStream;
import io.vertx.streams.CloseableReadStream;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class ConsumerStreamImpl<T> implements CloseableReadStream<T> {

  private static final int DISCONNECTED = 0, CONNECTING = 1, CONNECTED = 2;

  private final ConsumerImpl<T> consumer;
  private Handler<T> handler;
  private Handler<Void> endHandler;
  private String localAddress;
  private int status = DISCONNECTED;

  public ConsumerStreamImpl(ConsumerImpl<T> consumer) {
    this.consumer = consumer;
  }

  public void subscribe(Handler<AsyncResult<Void>> doneHandler) {
    subscribe(null, new DeliveryOptions(), doneHandler);
  }

  public void subscribe(Object body, DeliveryOptions options, Handler<AsyncResult<Void>> doneHandler) {
    if (status != DISCONNECTED) {
      throw new IllegalArgumentException();
    }
    this.status = CONNECTING;
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
//            consumer.unregister();
//            completionHandler.handle(Future.failedFuture(ar2.cause()));
        throw new UnsupportedOperationException("Implement me");
      } else {
        fut.setHandler(ar2 -> {
          if (ar2.succeeded()) {
            ReadStream<T> stream = ar2.result();
            doneHandler.handle(Future.succeededFuture());
            stream.handler(handler);
            stream.endHandler(endHandler);
            stream.resume();
          } else {
            doneHandler.handle(Future.failedFuture(ar2.cause()));
          }
        });
      }
    });
  }

  @Override
  public CloseableReadStream<T> exceptionHandler(Handler<Throwable> handler) {
    return this;
  }

  @Override
  public CloseableReadStream<T> handler(Handler<T> h) {
    handler = h;
    return this;
  }

  @Override
  public CloseableReadStream<T> pause() {
    return this;
  }

  @Override
  public CloseableReadStream<T> resume() {
    return this;
  }

  @Override
  public CloseableReadStream<T> endHandler(Handler<Void> handler) {
    endHandler = handler;
    return this;
  }

  @Override
  public void close() {
    consumer.bus.send(consumer.address, null, new DeliveryOptions().addHeader("addr", localAddress).addHeader("stream", "close"));
  }
}
