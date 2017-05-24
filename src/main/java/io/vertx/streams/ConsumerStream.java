package io.vertx.streams;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.streams.ReadStream;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
@VertxGen
public interface ConsumerStream<T> extends ReadStream<T> {

  static <T> void open(EventBus bus, String address, Object body, DeliveryOptions options, Handler<AsyncResult<ConsumerStream<T>>> handler) {
    Consumer<T> consumer = Consumer.consumer(bus, address);
    consumer.open(body, options, handler);
  }

  @Override
  ConsumerStream<T> exceptionHandler(Handler<Throwable> handler);

  @Override
  ConsumerStream<T> handler(Handler<T> handler);

  @Override
  ConsumerStream<T> pause();

  @Override
  ConsumerStream<T> resume();

  @Override
  ConsumerStream<T> endHandler(Handler<Void> handler);

  void close();
}
