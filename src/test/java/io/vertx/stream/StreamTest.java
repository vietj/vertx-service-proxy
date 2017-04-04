package io.vertx.stream;

import io.vertx.core.Vertx;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.streams.ConsumerStream;
import io.vertx.streams.Producer;
import io.vertx.streams.impl.EventBusTransport;
import io.vertx.streams.impl.NetTransport;
import io.vertx.streams.impl.Transport;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class StreamTest extends VertxTestBase {

  @Test
  public void testEventBusStream() throws Exception {
    Vertx vertx = Vertx.vertx();
    testStream(vertx, new EventBusTransport(vertx.eventBus()));
    await();
  }

  @Test
  public void testNetStream() throws Exception {
    Vertx vertx = Vertx.vertx();
    NetTransport transport = new NetTransport(vertx, 1234, "localhost");
    vertx.createNetServer().connectHandler(transport).listen(1234, onSuccess(v -> {
      try {
        testStream(vertx, transport);
      } catch (Exception e) {
        fail(e);
      }
    }));
    await();
  }

  private void testStream(Vertx vertx, Transport transport) throws Exception {

    Producer<String> producer = Producer.publisher(vertx.eventBus(), transport);
    producer.handler(sub -> {
      sub.write("foo");
      sub.write("bar");
      sub.write("juu");
      sub.end();
    });
    producer.listen("the-address");

    AtomicInteger count = new AtomicInteger();
    ConsumerStream<String> consumer = ConsumerStream.consumer(vertx.eventBus(), transport);
    consumer.handler(event -> {
      int val = count.getAndIncrement();
      switch (val) {
        case 1:
          assertEquals("foo", event);
          break;
        case 2:
          assertEquals("bar", event);
          break;
        case 3:
          assertEquals("juu", event);
          break;
        default:
          fail("Unexpected " + val);
      }
    });
    consumer.endHandler(v -> {
      assertEquals(4, count.getAndIncrement());
      testComplete();
    });
    consumer.subscribe("the-address", ar -> {
      assertEquals(0, count.getAndIncrement());
    });
  }

  @Test
  public void testClose() throws Exception {
    Vertx vertx = Vertx.vertx();

    Producer<String> producer = Producer.publisher(vertx.eventBus());
    producer.handler(sub -> {
      sub.closeHandler(v -> {
        testComplete();
      });
    });
    producer.listen("the-address");

    ConsumerStream<String> consumer = ConsumerStream.consumer(vertx.eventBus());
    consumer.handler(event -> {
      fail();
    });
    consumer.subscribe("the-address", onSuccess(v -> {
      consumer.close();
    }));
    await();


  }

  @Test
  public void testWebsocketTransport() throws Exception {
  }
}
