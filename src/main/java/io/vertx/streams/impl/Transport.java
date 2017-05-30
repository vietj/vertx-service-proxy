package io.vertx.streams.impl;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
@VertxGen
public interface Transport {

  <T> void bind(WriteStream<T> stream, Handler<AsyncResult<String>> completionHandler);

  <T> String bind(Handler<AsyncResult<ReadStream<T>>> completionHandler);

  <T> void connect(String address, Handler<AsyncResult<WriteStream<T>>> completionHandler);

}
