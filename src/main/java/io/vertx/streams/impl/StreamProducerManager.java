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
import io.vertx.core.streams.WriteStream;
import io.vertx.streams.ProducerStream;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class StreamProducerManager<T> {

  private final Transport transport;
  private Map<String, ProducerStreamImpl<T>> active = new HashMap<>();

  public StreamProducerManager(EventBus bus) {
    this.transport = new EventBusTransport(bus);
  }

  public StreamProducerManager(Transport transport) {
    this.transport = transport;
  }

  public void open(String addr, Handler<AsyncResult<ProducerStream<T>>> fut) {
    transport.<T>bindStream(addr, ar -> {
      if (ar.succeeded()) {
        WriteStream<T> sub = ar.result();
        ProducerStreamImpl<T> stream = new ProducerStreamImpl<>(sub);
        active.put(addr, stream);
        fut.handle(Future.succeededFuture(stream));
      } else {
        fut.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  public void close(String addr) {
    ProducerStreamImpl<T> stream = active.remove(addr);
    if (stream != null) {
      Handler<Void> closeHandler = stream.closeHandler;
      if (closeHandler != null) {
        closeHandler.handle(null);
      }
    }
  }

  class ProducerStreamImpl<T> implements ProducerStream<T> {

    private final WriteStream<T> stream;
    private Handler<Void> closeHandler;

    public ProducerStreamImpl(WriteStream<T> stream) {
      this.stream = stream;
    }

    @Override
    public ProducerStream<T> closeHandler(Handler<Void> handler) {
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
