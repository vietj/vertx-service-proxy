package io.vertx.streams.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.streams.Consumer;
import io.vertx.streams.CloseableReadStream;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class ConsumerImpl<T> implements Consumer<T> {

  final EventBus bus;
  final String address;
  final Transport transport;

  public ConsumerImpl(EventBus bus, String address) {
    this.bus = bus;
    this.transport = new EventBusTransport(bus);
    this.address = address;
  }

  public ConsumerImpl(EventBus bus, String address, Transport transport) {
    this.bus = bus;
    this.transport = transport;
    this.address = address;
  }

  @Override
  public void openReadStream(Object body, DeliveryOptions options, Handler<AsyncResult<CloseableReadStream<T>>> doneHandler) {
    ConsumerStreamImpl<T> stream = new ConsumerStreamImpl<>(this);
    stream.subscribe(body, options, ar -> doneHandler.handle(ar.map(stream)));
  }

  @Override
  public void openReadStream(Handler<AsyncResult<CloseableReadStream<T>>> doneHandler) {
    ConsumerStreamImpl<T> stream = new ConsumerStreamImpl<>(this);
    stream.subscribe(ar -> doneHandler.handle(ar.map(stream)));
  }
}
