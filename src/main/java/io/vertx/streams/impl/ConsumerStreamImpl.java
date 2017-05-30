package io.vertx.streams.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.streams.WriteStream;
import io.vertx.streams.CloseableReadStream;

import java.util.LinkedList;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class ConsumerStreamImpl<T> implements CloseableReadStream<T> {

  private static final Handler NULL_HANDLER = o -> {};

  private final ConsumerImpl<T> consumer;
  private Handler<T> handler = NULL_HANDLER;
  private Handler<Void> endHandler = NULL_HANDLER;
  private String localAddress;

  public ConsumerStreamImpl(ConsumerImpl<T> consumer) {
    this.consumer = consumer;
  }

  private static final int DISCONNECTED = 0, CONNECTING = 1, CONNECTED = 2;

  private LinkedList<T> pending = new LinkedList<>();
  private int status = DISCONNECTED;
  private boolean ended;

  public void subscribe(Handler<AsyncResult<Void>> doneHandler) {
    subscribe(null, new DeliveryOptions(), doneHandler);
  }

  public void subscribe(Object body, DeliveryOptions options, Handler<AsyncResult<Void>> doneHandler) {
    if (status != DISCONNECTED) {
      throw new IllegalArgumentException();
    }
    this.status = CONNECTING;
    consumer.transport.bind(new WriteStream<T>() {
      @Override
      public WriteStream<T> exceptionHandler(Handler<Throwable> handler) {
        return this;
      }
      @Override
      public WriteStream<T> write(T t) {
        synchronized (ConsumerStreamImpl.this) {
          switch (status) {
            case CONNECTING:
              pending.add(t);
              break;
            case CONNECTED: {
              handler.handle(t);
              break;
            }
          }
        }
        return this;
      }
      @Override
      public void end() {
        synchronized (ConsumerStreamImpl.this) {
          switch (status) {
            case CONNECTING:
              ended = true;
              break;
            case CONNECTED: {
              endHandler.handle(null);
              break;
            }
          }
        }
      }
      @Override
      public WriteStream<T> setWriteQueueMaxSize(int i) {
        return this;
      }
      @Override
      public boolean writeQueueFull() {
        return false;
      }
      @Override
      public WriteStream<T> drainHandler(Handler<Void> handler) {
        return this;
      }
    }, ar -> {
      if (ar.failed()) {
        status = DISCONNECTED;
        doneHandler.handle(Future.failedFuture(ar.cause()));
      } else {
        localAddress = ar.result();
        options.addHeader("stream", "open");
        options.addHeader("addr", localAddress);
        consumer.bus.send(consumer.address, body, options, ar2 -> {
          if (ar2.failed()) {
//            consumer.unregister();
//            completionHandler.handle(Future.failedFuture(ar2.cause()));
            throw new UnsupportedOperationException("Implement me");
          } else {
            doneHandler.handle(Future.succeededFuture());
            synchronized (ConsumerStreamImpl.this) {
              T item;
              while ((item = pending.poll()) != null) {
                handler.handle(item);
              }
              status = CONNECTED;
              if (ended) {
                endHandler.handle(null);
              }
            }
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
