/*
 * Copyright 2014 Red Hat, Inc.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  and Apache License v2.0 which accompanies this distribution.
 *
 *  The Eclipse Public License is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 *
 *  The Apache License v2.0 is available at
 *  http://www.opensource.org/licenses/apache2.0.php
 *
 *  You may elect to redistribute this code under either of these licenses.
 */
package io.vertx.streams.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import io.vertx.streams.CloseableReadStream;
import io.vertx.streams.CloseableWriteStream;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class StreamProducerManager<T> {

  private final Transport transport;
  private Map<String, ProducerStream> active = new HashMap<>();

  public StreamProducerManager(EventBus bus) {
    this.transport = new EventBusTransport(bus);
  }

  public StreamProducerManager(Transport transport) {
    this.transport = transport;
  }

  public void openReadStream(String addr, Handler<AsyncResult<CloseableWriteStream<T>>> handler) {
    transport.<T>connect(addr, ar -> {
      if (ar.succeeded()) {
        WriteStream<T> sub = ar.result();
        ProducerWriteStream stream = new ProducerWriteStream(sub);
        active.put(addr, stream);
        handler.handle(Future.succeededFuture(stream));
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  public String openWriteStream(Handler<AsyncResult<CloseableReadStream<T>>> handler) {
    String addr = transport.<T>bind(ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture(new ProducerReadStream(ar.result())));
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
    active.put(addr, new ProducerStream() {
      @Override
      void onClose() {
      }
    });
    return addr;
  }

  public void close(String addr) {
    ProducerStream stream = active.remove(addr);
    if (stream != null) {
      stream.onClose();
    }
  }

  abstract class ProducerStream {

    abstract void onClose();

  }

  private class ProducerReadStream extends ProducerStream implements CloseableReadStream<T> {

    private final ReadStream<T> stream;

    public ProducerReadStream(ReadStream<T> stream) {
      this.stream = stream;
    }

    @Override
    public CloseableReadStream<T> exceptionHandler(Handler<Throwable> handler) {
      stream.exceptionHandler(handler);
      return this;
    }
    
    @Override
    public CloseableReadStream<T> handler(Handler<T> handler) {
      stream.handler(handler);
      return this;
    }

    @Override
    public CloseableReadStream<T> pause() {
      stream.pause();
      return this;
    }

    @Override
    public CloseableReadStream<T> resume() {
      stream.resume();
      return this;
    }

    @Override
    public CloseableReadStream<T> endHandler(Handler<Void> handler) {
      stream.endHandler(handler);
      return this;
    }

    @Override
    public void close() {
      // Todo
    }

    @Override
    void onClose() {
    }
  }

  private class ProducerWriteStream extends ProducerStream implements CloseableWriteStream<T> {

    private final WriteStream<T> stream;
    private Handler<Void> closeHandler;

    ProducerWriteStream(WriteStream<T> stream) {
      this.stream = stream;
    }

    @Override
    void onClose() {
      if (closeHandler != null) {
        closeHandler.handle(null);
      }
    }

    @Override
    public CloseableWriteStream<T> closeHandler(Handler<Void> handler) {
      closeHandler = handler;
      return this;
    }

    @Override
    public WriteStream<T> exceptionHandler(Handler<Throwable> handler) {
      return this;
    }

    @Override
    public WriteStream<T> write(T data) {
      stream.write(data);
      return this;
    }

    @Override
    public void end() {
      stream.end();
    }

    @Override
    public WriteStream<T> setWriteQueueMaxSize(int maxSize) {
      stream.setWriteQueueMaxSize(maxSize);
      return this;
    }

    @Override
    public boolean writeQueueFull() {
      return stream.writeQueueFull();
    }

    @Override
    public WriteStream<T> drainHandler(Handler<Void> handler) {
      stream.drainHandler(handler);
      return this;
    }
  }
}
