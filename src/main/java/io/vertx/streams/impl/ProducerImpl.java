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

import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.streams.WriteStream;
import io.vertx.streams.Producer;
import io.vertx.streams.ProducerStream;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class ProducerImpl<T> implements Producer<T> {

  private final EventBus bus;
  private Handler<ProducerStream<T>> handler;
  private Map<String, ProducerStreamImpl> active = new HashMap<>();

  public ProducerImpl(EventBus bus) {
    this.bus = bus;
  }

  @Override
  public Producer<T> handler(Handler<ProducerStream<T>> handler) {
    this.handler = handler;
    return this;
  }

  class ProducerStreamImpl implements ProducerStream<T> {

    final Message<String> msg;
    final String dst;
    Handler<Void> closeHandler;

    public ProducerStreamImpl(Message<String> msg) {
      this.msg = msg;
      this.dst = msg.body();
    }

    @Override
    public void complete() {
      msg.reply(null);
    }

    @Override
    public void fail(Throwable err) {
      msg.fail(0, err.getMessage());
    }

    @Override
    public WriteStream<T> exceptionHandler(Handler<Throwable> handler) {
      return this;
    }

    @Override
    public WriteStream<T> write(T t) {
      bus.send(dst, t);
      return this;
    }

    @Override
    public void end() {
      bus.send(dst, null, new DeliveryOptions().addHeader("action", "end"));
    }

    @Override
    public WriteStream<T> setWriteQueueMaxSize(int i) {
      return this;
    }

    @Override
    public boolean writeQueueFull() {
      return false;
    }

    @Override
    public WriteStream<T> drainHandler(Handler<Void> handler) {
      return this;
    }

    @Override
    public ProducerStream<T> closeHandler(Handler<Void> handler) {
      closeHandler = handler;
      return this;
    }
  }

  @Override
  public void listen(String address) {
    bus.<String>consumer(address, msg -> {
      String action = msg.headers().get("action");
      if (action != null) {
        switch (action) {
          case "open":
            ProducerStreamImpl sub = new ProducerStreamImpl(msg);
            active.put(sub.dst, sub);
            handler.handle(sub);
            break;
          case "close":
            String dst = msg.body();
            ProducerStreamImpl stream = active.remove(dst);
            if (stream != null) {
              Handler<Void> closeHandler = stream.closeHandler;
              if (closeHandler != null) {
                closeHandler.handle(null);
              }
            }
            break;
        }
      }
    });
  }
}
