package io.vertx.streams;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
@VertxGen
public interface ProducerStream<T> extends WriteStream<T> {

  @Fluent
  ProducerStream<T> closeHandler(Handler<Void> handler);

}
