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
public interface CloseableReadStream<T> extends ReadStream<T> {

  static <T> void open(EventBus bus, String address, Object body, DeliveryOptions options, Handler<AsyncResult<CloseableReadStream<T>>> handler) {
    Consumer<T> consumer = Consumer.consumer(bus, address);
    consumer.openReadStream(body, options, handler);
  }

  @Override
  CloseableReadStream<T> exceptionHandler(Handler<Throwable> handler);

  @Override
  CloseableReadStream<T> handler(Handler<T> handler);

  @Override
  CloseableReadStream<T> pause();

  @Override
  CloseableReadStream<T> resume();

  @Override
  CloseableReadStream<T> endHandler(Handler<Void> handler);

  void close();
}
