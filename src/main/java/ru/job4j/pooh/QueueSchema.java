package ru.job4j.pooh;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Режим очередь.
 * Поставщик создает события, потребитель их получает. Представим, что поставщик
 * отправил два сообщения в очередь: ["message 1", "message 2"]. Если у нас два
 * потребителя, то первый получит сообщение "message 1", а второй "message 2".
 * То есть в режиме очередь все сообщения равномерно распределяются между потребителями.
 */
public class QueueSchema implements Schema {
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Receiver>> receivers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BlockingQueue<String>> data = new ConcurrentHashMap<>();
    private final Condition condition = new Condition();

    @Override
    public void addReceiver(Receiver receiver) {
        receivers.putIfAbsent(receiver.name(), new CopyOnWriteArrayList<>());
        receivers.get(receiver.name()).add(receiver);
        condition.on();
    }

    /**
     * Основная структура для хранения сообщений - это карта. Ключ в карте
     * - это имя контейнера, а значение - это многопоточная очередь.
     */
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
                    for (var queueKey : receivers.keySet()) {
                        var queue = data.getOrDefault(queueKey, new LinkedBlockingQueue<>());
                        var receiversByQueue = receivers.get(queueKey);
                        var it = receiversByQueue.iterator();
                        while (it.hasNext()) {
                            var data = queue.poll();
                            if (data != null) {
                                it.next().receive(data);
                            }
                            if (data == null) {
                                break;
                            }
                            if (!it.hasNext()) {
                                it = receiversByQueue.iterator();
                            }
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

    /**
     * Метод run используются пулом нитей в классе PoohService.
     * Если сервис остановили, то нитям придет уведомление, что нить прервана и нужно выйти из цикла.
     * Цикл for перебирает всех зарегистрированный потребителей и отправляет им сообщение.
     * Обратите внимание, что тут используется метод poll(), который не блокирует нить выполнения,
     * а отправляет null, если очередь пустая.
     * Если все очереди пустые, то флаг переходить в режим false и нить выполнения блокируется до
     * момента появления новых сообщений.
     * Обратите внимание, что после выставления флага в режим false идет дополнительная проверка
     * в цикле while. Этот момент будет гарантировать ситуацию, что если после выставление
     * флага в режим false в очередь добавили еще сообщения, то нить не будет блокироваться,
     * а снова будет отправлять сообщения.
     */
}
