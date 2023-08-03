package ru.job4j.pooh;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Режим темы.
 * Поставщик создает события, потребитель их получает.
 * Представим, что поставщик отправил два сообщения в очередь: ["message 1", "message 2"].
 * Если у нас два потребителя, то каждый из них должен получить все сообщения от поставщика,
 * То есть потребитель 1 получит сообщения: "message 1", "message 2",
 * и потребитель 2 тоже получит сообщения: "message 1", "message 2".
 * То есть в режиме темы все сообщения от поставщика должны быть доставлены каждому потребителю.
 */

public class TopicSchema implements Schema {

    private final HashMap<String, ArrayList<Receiver>> receivers = new HashMap<>();

    private final ConcurrentHashMap<String, BlockingQueue<String>> data = new ConcurrentHashMap<>();

    private final Condition condition = new Condition();

    @Override
    public void addReceiver(Receiver receiver) {
        receivers.putIfAbsent(receiver.name(), new ArrayList<>());
        receivers.get(receiver.name()).add(receiver);
        condition.on();
    }

    @Override
    public void publish(Message message) {
        data.putIfAbsent(message.name(), new LinkedBlockingQueue<>());
        data.get(message.name()).add(message.text());
        condition.on();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            do {
                Set<String> topicSet = data.keySet();
                for (String topic : topicSet) {
                    var receiverList = receivers.get(topic);
                    if (receiverList == null) {
                        continue;
                    }
                    var queue = data.get(topic);
                    var message = queue.poll();
                    while (message != null) {
                        for (var receiver : receiverList) {
                            receiver.receive(message);
                        }
                        message = queue.poll();
                    }
                }
                condition.off();
            } while (condition.check());
            try {
                condition.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
