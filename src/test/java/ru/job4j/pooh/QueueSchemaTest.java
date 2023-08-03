package ru.job4j.pooh;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.*;

class QueueSchemaTest {

    @Test
    public void whenSingleReceiver() throws InterruptedException {
        var queue = new QueueSchema();
        var result = new CopyOnWriteArrayList<String>();
        var count = new CountDownLatch(1);
        queue.addReceiver(new TextReceiver(count, "weather", result));
        queue.publish(new Message("weather", "18"));
        var thread = new Thread(queue);
        thread.start();
        count.await();
        thread.interrupt();
        assertThat(result).contains("18");
    }

    @Test
    public void whenWithoutMessage() throws InterruptedException {
        var queue = new QueueSchema();
        var result = new CopyOnWriteArrayList<String>();
        queue.addReceiver(new TextReceiver(new CountDownLatch(1), "weather", result));
        var thread = new Thread(queue);
        thread.start();
        thread.interrupt();
        thread.join();
        assertThat(result).isEmpty();
    }

    @Test
    public void whenReceiverOnlyByWeather() throws InterruptedException {
        var queue = new QueueSchema();
        var result = new CopyOnWriteArrayList<String>();
        var count = new CountDownLatch(1);
        queue.addReceiver(new TextReceiver(count, "weather", result));
        queue.publish(new Message("weather", "18"));
        queue.publish(new Message("city", "Moskow"));
        var thread = new Thread(queue);
        thread.start();
        count.await();
        thread.interrupt();
        assertThat(result).contains("18");
    }

    @Test
    public void whenReceiverMultiMessageByWeather() throws InterruptedException {
        var queue = new QueueSchema();
        var result = new CopyOnWriteArrayList<String>();
        var count = new CountDownLatch(2);
        queue.addReceiver(new TextReceiver(count, "weather", result));
        queue.publish(new Message("weather", "18"));
        queue.publish(new Message("weather", "20"));
        var thread = new Thread(queue);
        thread.start();
        count.await();
        thread.interrupt();
        assertThat(result).contains("18", "20");
    }

    @Test
    public void whenMultiReceivers() throws InterruptedException {
        var queue = new QueueSchema();
        var resultFirst = new CopyOnWriteArrayList<String>();
        var resultSecond = new CopyOnWriteArrayList<String>();
        var count = new CountDownLatch(2);
        queue.addReceiver(new TextReceiver(count, "weather", resultFirst));
        queue.addReceiver(new TextReceiver(count, "weather", resultSecond));
        queue.publish(new Message("weather", "23"));
        queue.publish(new Message("weather", "20"));
        queue.publish(new Message("city", "11"));
        var thread = new Thread(queue);
        thread.start();
        count.await();
        thread.interrupt();
        assertThat(resultFirst).containsOnly("23");
        assertThat(resultSecond).containsOnly("20");
    }

    @Test
    public void whenMultiReceivers2() throws InterruptedException {
        var queue = new QueueSchema();
        var result = new CopyOnWriteArrayList<String>();
        var result2 = new CopyOnWriteArrayList<String>();
        var count = new CountDownLatch(2);
        queue.addReceiver(new TextReceiver(count, "weather", result));
        queue.addReceiver(new TextReceiver(count, "weather", result2));
        queue.publish(new Message("weather", "20"));
        queue.publish(new Message("weather", "21"));
        queue.publish(new Message("weather", "22"));
        queue.publish(new Message("weather", "23"));
        queue.publish(new Message("city", "11"));
        var thread = new Thread(queue);
        thread.start();
        count.await();
        thread.interrupt();
        assertThat(result).containsOnly("20", "22");
        assertThat(result2).containsOnly("21", "23");
    }

}