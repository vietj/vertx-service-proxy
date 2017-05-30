package io.vertx.streams;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
@VertxGen
public interface CloseableWriteStream<T> extends WriteStream<T> {

  @Fluent
  CloseableWriteStream<T> closeHandler(Handler<Void> handler);

}
