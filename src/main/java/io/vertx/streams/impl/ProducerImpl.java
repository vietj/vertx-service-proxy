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

  private final EventBusTransport transport;
  private final EventBus bus;
  private Handler<ProducerStream<T>> handler;
  private Map<String, ProducerStream> active = new HashMap<>();

  public ProducerImpl(EventBus bus) {
    this.bus = bus;
    this.transport = new EventBusTransport(bus);
  }

  @Override
  public Producer<T> handler(Handler<ProducerStream<T>> handler) {
    this.handler = handler;
    return this;
  }

  @Override
  public void listen(String address) {
    bus.<String>consumer(address, msg -> {
      String dst = msg.body();
      String action = msg.headers().get("action");
      if (action != null) {
        switch (action) {
          case "open":
            ProducerStream sub = transport.createStream(msg);
            active.put(dst, sub);
            handler.handle(sub);
            break;
          case "close":
            ProducerStream stream = active.remove(dst);
            if (stream != null) {
              Handler<Void> closeHandler = stream.closeHandler();
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
