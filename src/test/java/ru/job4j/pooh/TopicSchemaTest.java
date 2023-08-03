package ru.job4j.pooh;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

@Disabled
class TopicSchemaTest {

    @Test
    @Timeout(1)
    public void whenSingleReceiver() throws InterruptedException {
        var topic = new TopicSchema();
        var result = new CopyOnWriteArrayList<String>();
        var count = new CountDownLatch(1);
        topic.addReceiver(new TextReceiver(count, "weather", result));
        topic.publish(new Message("weather", "18"));
        var thread = new Thread(topic);
        thread.start();
        count.await();
        thread.interrupt();
        assertThat(result).contains("18");
    }

    @Test
    @Timeout(1)
    public void whenWithoutMessage() throws InterruptedException {
        var topic = new TopicSchema();
        var result = new CopyOnWriteArrayList<String>();
        topic.addReceiver(new TextReceiver(new CountDownLatch(1), "weather", result));
        var thread = new Thread(topic);
        thread.start();
        thread.interrupt();
        thread.join();
        assertThat(result).isEmpty();
    }

    @Test
    @Timeout(1)
    public void whenReceiverOnlyByWeather() throws InterruptedException {
        var topic = new TopicSchema();
        var result = new CopyOnWriteArrayList<String>();
        var count = new CountDownLatch(1);
        topic.addReceiver(new TextReceiver(count, "weather", result));
        topic.publish(new Message("weather", "18"));
        topic.publish(new Message("city", "Moskow"));
        var thread = new Thread(topic);
        thread.start();
        count.await();
        thread.interrupt();
        assertThat(result).contains("18");
    }

    @Test
    @Timeout(1)
    public void whenMultiReceivers() throws InterruptedException {
        var topic = new TopicSchema();
        var outFirst = new CopyOnWriteArrayList<String>();
        var countFirst = new CountDownLatch(2);
        topic.addReceiver(new TextReceiver(countFirst, "weather", outFirst));
        var outSecond = new CopyOnWriteArrayList<String>();
        var countSecond = new CountDownLatch(2);
        topic.addReceiver(new TextReceiver(countSecond, "weather", outSecond));
        topic.publish(new Message("weather", "23"));
        topic.publish(new Message("weather", "20"));
        topic.publish(new Message("city", "11"));
        var thread = new Thread(topic);
        thread.start();
        countFirst.await();
        countSecond.await();
        thread.interrupt();
        assertThat(outFirst).containsOnly("23", "20");
        assertThat(outSecond).containsOnly("23", "20");
    }

    @Test
    @Timeout(1)
    public void whenMultiDifferentReceivers() throws InterruptedException {
        var topic = new TopicSchema();
        var countFirst = new CountDownLatch(2);
        var countSecond = new CountDownLatch(2);
        var countThird = new CountDownLatch(2);
        var countFourth = new CountDownLatch(2);
        var outWeatherFirst = new CopyOnWriteArrayList<String>();
        var outWeatherSecond = new CopyOnWriteArrayList<String>();
        var outCityFirst = new CopyOnWriteArrayList<String>();
        var outCitySecond = new CopyOnWriteArrayList<String>();
        topic.addReceiver(new TextReceiver(countFirst, "weather", outWeatherFirst));
        topic.addReceiver(new TextReceiver(countThird, "city", outCitySecond));
        topic.addReceiver(new TextReceiver(countSecond, "weather", outWeatherSecond));
        topic.addReceiver(new TextReceiver(countFourth, "city", outCityFirst));
        topic.publish(new Message("weather", "+23"));
        topic.publish(new Message("city", "Moscow"));
        topic.publish(new Message("weather", "+15"));
        topic.publish(new Message("city", "Tula"));
        topic.publish(new Message("weather", "+20"));
        topic.publish(new Message("city", "Tambov"));
        var thread = new Thread(topic);
        thread.start();
        countFirst.await();
        countSecond.await();
        countThird.await();
        countFourth.await();
        thread.interrupt();
        assertThat(outWeatherFirst).containsOnly("+23", "+15", "+20");
        assertThat(outWeatherSecond).containsOnly("+23", "+15", "+20");
        assertThat(outCityFirst).containsOnly("Moscow", "Tula", "Tambov");
        assertThat(outCitySecond).containsOnly("Moscow", "Tula", "Tambov");
    }
}