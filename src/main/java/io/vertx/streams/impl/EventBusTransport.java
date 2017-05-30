package io.vertx.streams.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

import java.util.LinkedList;
import java.util.UUID;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class EventBusTransport implements Transport {

  private final EventBus bus;

  public EventBusTransport(EventBus bus) {
    this.bus = bus;
  }

  @Override
  public <T> String bind(Handler<AsyncResult<ReadStream<T>>> completionHandler) {
    String uuid = UUID.randomUUID().toString();
    TheStream<T> stream = new TheStream<T>(uuid);
    MessageConsumer<T> consumer = bus.consumer(uuid, msg -> {
      String action = msg.headers().get("action");
      if ("end".equals(action)) {
        stream.write((T) END_SENTINEL);
      } else if (action == null) {
        stream.write(msg.body());
      }
    });
    consumer.completionHandler(ar1 -> {
      if (ar1.succeeded()) {
        completionHandler.handle(Future.succeededFuture(stream));
      } else {
        completionHandler.handle(Future.failedFuture(ar1.cause()));
      }
    });
    return uuid;
  }

  private static final Object END_SENTINEL = new Object();

  static class TheStream<T> implements ReadStream<T> {

    private final String address;
    private boolean paused;
    private LinkedList<T> pending = new LinkedList<>();
    private Handler<T> handler;
    private Handler<Void> endHandler;

    public TheStream(String address) {
      this.address = address;
    }

    @Override
    public synchronized ReadStream<T> exceptionHandler(Handler<Throwable> handler) {
      return this;
    }

    void write(T item) {
      Handler<T> handler;
      Handler<Void> endHandler;
      synchronized (this) {
        if (paused || pending.size() > 0) {
          pending.add(item);
          return;
        }
        handler = this.handler;
        endHandler = this.endHandler;
      }
      if (item == END_SENTINEL) {
        if (endHandler != null) {
          endHandler.handle(null);
        }
      } else {
        if (handler != null) {
          handler.handle(item);
        }
      }
    }

    @Override
    public synchronized ReadStream<T> handler(Handler<T> handler) {
      this.handler = handler;
      return this;
    }

    @Override
    public synchronized ReadStream<T> pause() {
      paused = true;
      return this;
    }

    @Override
    public ReadStream<T> resume() {
      synchronized (this) {
        paused = false;
      }
      while (true) {
        T item;
        Handler<T> handler;
        Handler<Void> endHandler;
        synchronized (this) {
          if (pending.isEmpty()) {
            break;
          }
          handler = this.handler;
          endHandler = this.endHandler;
          item = pending.removeFirst();
        }
        if (item == END_SENTINEL) {
          if (endHandler != null) {
            endHandler.handle(null);
          }
        } else {
          if (handler != null) {
            handler.handle(item);
          }
        }
      }
      return this;
    }

    @Override
    public synchronized ReadStream<T> endHandler(Handler<Void> handler) {
      endHandler = handler;
      return this;
    }
  }

  @Override
  public <T> void connect(String address, Handler<AsyncResult<WriteStream<T>>> completionHandler) {
    completionHandler.handle(Future.succeededFuture(new EventBusStreamImpl<T>(address)));
  }

  private class EventBusStreamImpl<T> implements WriteStream<T> {

    final String address;

    public EventBusStreamImpl(String address) {
      this.address = address;
    }

    @Override
    public WriteStream<T> exceptionHandler(Handler<Throwable> handler) {
      return this;
    }

    @Override
    public WriteStream<T> write(T t) {
      bus.send(address, t);
      return this;
    }

    @Override
    public void end() {
      bus.send(address, null, new DeliveryOptions().addHeader("action", "end"));
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

  }
}
