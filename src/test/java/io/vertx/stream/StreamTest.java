package io.vertx.stream;

import io.vertx.core.Vertx;
import io.vertx.streams.ConsumerStream;
import io.vertx.streams.Producer;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class StreamTest extends VertxTestBase {

  @Test
  public void testSimple() throws Exception {

    Vertx vertx = Vertx.vertx();

    Producer<String> producer = Producer.publisher(vertx.eventBus());
    producer.handler(sub -> {
      sub.complete();
      sub.write("foo");
      sub.write("bar");
      sub.write("juu");
      sub.end();
    });
    producer.listen("the-address");

    AtomicInteger count = new AtomicInteger();
    ConsumerStream<String> consumer = ConsumerStream.consumer(vertx.eventBus());
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

    await();
  }

  @Test
  public void testClose() throws Exception {
    Vertx vertx = Vertx.vertx();

    Producer<String> producer = Producer.publisher(vertx.eventBus());
    producer.handler(sub -> {
      sub.closeHandler(v -> {
        testComplete();
      });
      sub.complete();
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
}
