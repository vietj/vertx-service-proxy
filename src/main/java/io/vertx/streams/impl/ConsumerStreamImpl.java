package io.vertx.streams.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.streams.ConsumerStream;

import java.util.LinkedList;
import java.util.UUID;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class ConsumerStreamImpl<T> implements ConsumerStream<T> {

  private final EventBus bus;
  private Handler<T> handler;
  private Handler<Void> endHandler;
  private String uuid;
  private String address;

  public ConsumerStreamImpl(EventBus bus) {
    this.bus = bus;
  }

  private LinkedList<Message<T>> pending = new LinkedList<>();

  private void deliver(Message<T> msg) {
    String action = msg.headers().get("action");
    if (action != null) {
      switch (action) {
        case "end":
          if (endHandler != null) {
            endHandler.handle(null);
          }
          break;
        default:
          break;
      }
    } else {
      handler.handle(msg.body());
    }
  }

  @Override
  public void subscribe(String address, Handler<AsyncResult<Void>> doneHandler) {
    this.uuid = UUID.randomUUID().toString();
    this.address = address;
    MessageConsumer<T> consumer = bus.consumer(uuid, msg -> {
      synchronized (ConsumerStreamImpl.this) {
        if (pending != null) {
          pending.add(msg);
          return;
        }
      }
      deliver(msg);
    });
    consumer.completionHandler(ar1 -> {
      if (ar1.succeeded()) {
        bus.send(address, uuid, new DeliveryOptions().addHeader("action", "open"), ar2 -> {
          if (ar2.failed()) {
            consumer.unregister();
          }
          doneHandler.handle(ar2.mapEmpty());
          synchronized (ConsumerStreamImpl.this) {
            Message<T> msg;
            while ((msg = pending.poll()) != null) {
              deliver(msg); // Not great but for now OK
            }
            pending = null;
          }
        });
      } else {
        doneHandler.handle(ar1);
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
    bus.send(address, uuid, new DeliveryOptions().addHeader("action", "close"));
  }
}
