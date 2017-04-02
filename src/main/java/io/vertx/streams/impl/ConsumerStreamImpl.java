package io.vertx.streams.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.streams.WriteStream;
import io.vertx.streams.ConsumerStream;

import java.util.LinkedList;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class ConsumerStreamImpl<T> implements ConsumerStream<T> {

  private final EventBus bus;
  private final Context ctx = Vertx.currentContext();
  private Handler<T> handler;
  private Handler<Void> endHandler;
  private String localAddress;
  private String address;
  private EventBusTransport transport;

  public ConsumerStreamImpl(EventBus bus) {
    this.bus = bus;
    this.transport = new EventBusTransport(bus);
  }

  private static final int DISCONNECTED = 0, CONNECTING = 1, CONNECTED = 2;

  private LinkedList<T> pending = new LinkedList<>();
  private int status = DISCONNECTED;

  @Override
  public void subscribe(String address, Handler<AsyncResult<Void>> doneHandler) {
    if (status != DISCONNECTED) {
      throw new IllegalArgumentException();
    }
    this.status = CONNECTING;
    this.address = address;
    transport.bind(address, new WriteStream<T>() {
      @Override
      public WriteStream<T> exceptionHandler(Handler<Throwable> handler) {
        return this;
      }
      @Override
      public WriteStream<T> write(T t) {
        switch (status) {
          case CONNECTING:
            pending.add(t);
            break;
          case CONNECTED: {
            if (handler != null) {
              handler.handle(t);
            }
            break;
          }
        }
        return this;
      }
      @Override
      public void end() {
        switch (status) {
          case CONNECTING:
            // Todo
            break;
          case CONNECTED: {
            if (endHandler != null) {
              endHandler.handle(null);
            }
            break;
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
        status = CONNECTED;
        localAddress = ar.result();
        doneHandler.handle(Future.succeededFuture());
        synchronized (ConsumerStreamImpl.this) {
          T item;
          while ((item = pending.poll()) != null) {
            T o = item;
            ctx.runOnContext(v -> {
              handler.handle(o);
            });
          }
        }
      }
    });
  }

  @Override
  public ConsumerStream<T> exceptionHandler(Handler<Throwable> handler) {
    return this;
  }

  @Override
  public ConsumerStream<T> handler(Handler<T> h) {
    handler = h;
    return this;
  }

  @Override
  public ConsumerStream<T> pause() {
    return this;
  }

  @Override
  public ConsumerStream<T> resume() {
    return this;
  }

  @Override
  public ConsumerStream<T> endHandler(Handler<Void> handler) {
    endHandler = handler;
    return this;
  }

  @Override
  public void close() {
    bus.send(address, localAddress, new DeliveryOptions().addHeader("action", "close"));
  }
}
