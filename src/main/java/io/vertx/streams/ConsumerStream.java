package io.vertx.streams;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.streams.ReadStream;
import io.vertx.streams.impl.ConsumerStreamImpl;
import io.vertx.streams.impl.Transport;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
@VertxGen
public interface ConsumerStream<T> extends ReadStream<T> {

  static <T> ConsumerStream<T> consumer(EventBus bus) {
    return new ConsumerStreamImpl<>(bus);
  }

  static <T> ConsumerStream<T> consumer(EventBus bus, Transport transport) {
    return new ConsumerStreamImpl<>(bus, transport);
  }

  void subscribe(String address, Handler<AsyncResult<Void>> doneHandler);

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
