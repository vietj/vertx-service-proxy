package io.vertx.streams.impl;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.streams.WriteStream;
import io.vertx.streams.ProducerStream;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
@VertxGen
public interface Transport {

  <T> void bind(WriteStream<T> to, Handler<AsyncResult<String>> completionHandler);

  <T> void resolveStream(Message<String> msg, Handler<AsyncResult<ProducerStream<T>>> completionHandler);

}
