package io.vertx.streams.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.streams.WriteStream;
import io.vertx.streams.ProducerStream;

import java.util.UUID;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
class EventBusTransport {

  private final EventBus bus;

  EventBusTransport(EventBus bus) {
    this.bus = bus;
  }

  <T> void bind(String address, WriteStream<T> to, Handler<AsyncResult<String>> completionHandler) {
    String uuid = UUID.randomUUID().toString();
    MessageConsumer<T> consumer = bus.consumer(uuid, msg -> {
      String action = msg.headers().get("action");
      if ("end".equals(action)) {
        to.end();
      } else if (action == null) {
        to.write(msg.body());
      }
    });
    consumer.completionHandler(ar1 -> {
      if (ar1.succeeded()) {
        bus.send(address, uuid, new DeliveryOptions().addHeader("action", "open"), ar2 -> {
          if (ar2.failed()) {
            consumer.unregister();
            completionHandler.handle(Future.failedFuture(ar2.cause()));
          } else {
            completionHandler.handle(Future.succeededFuture(uuid));
          }
        });
      } else {
        completionHandler.handle(Future.failedFuture(ar1.cause()));
      }
    });
  }

  <T> ProducerStream<T> createStream(Message<String> msg) {
    return new ProducerStreamImpl<>(msg);
  }


  private class ProducerStreamImpl<T> implements ProducerStream<T> {

    final Message<String> msg;
    final String dst;
    Handler<Void> closeHandler;

    public ProducerStreamImpl(Message<String> msg) {
      this.msg = msg;
      this.dst = msg.body();
    }

    @Override
    public void complete() {
      msg.reply(null);
    }

    @Override
    public void fail(Throwable err) {
      msg.fail(0, err.getMessage());
    }

    @Override
    public WriteStream<T> exceptionHandler(Handler<Throwable> handler) {
      return this;
    }

    @Override
    public WriteStream<T> write(T t) {
      bus.send(dst, t);
      return this;
    }

    @Override
    public void end() {
      bus.send(dst, null, new DeliveryOptions().addHeader("action", "end"));
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

    @Override
    public ProducerStream<T> closeHandler(Handler<Void> handler) {
      closeHandler = handler;
      return this;
    }

    @Override
    public Handler<Void> closeHandler() {
      return closeHandler;
    }
  }
}
