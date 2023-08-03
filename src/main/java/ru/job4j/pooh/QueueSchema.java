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
    private final CopyOnWriteArrayList<Receiver> receivers = new CopyOnWriteArrayList<>();
    //контейнер для регистрации потребителей.
    private final ConcurrentHashMap<String, BlockingQueue<String>> data = new ConcurrentHashMap<>();
    //используется для аккумуляции сообщений от поставщика.
    private final Condition condition = new Condition();
    //многопоточный блокирующий флаг. С помощью этого флага мы регулируем работу очередей в контейнере data.

    @Override
    public void addReceiver(Receiver receiver) {
        receivers.add(receiver);
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
                var mailing = true;
                while (mailing) {
                    for (var receiver : receivers) {
                        var queue = data.get(receiver.name());
                        var message = queue.poll();
                        if (message == null) {
                            mailing = false;
                            break;
                        }
                        receiver.receive(message);
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
