package io.vertx.streams.impl;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
@VertxGen
public interface Transport {

  <T> void openStream(WriteStream<T> to, Handler<AsyncResult<String>> completionHandler);

  <T> void bindStream(String address, Handler<AsyncResult<WriteStream<T>>> completionHandler);

}
