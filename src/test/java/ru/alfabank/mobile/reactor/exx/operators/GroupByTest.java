package ru.alfabank.mobile.reactor.exx.operators;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.SignalType;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.logging.Level;




/**
 * Реактивная дока приводит рекомендации по использованию оператора groupBy
 * https://projectreactor.io/docs/core/release/reference/#advanced-three-sorts-batching
 * <p>
 * StepVerifier.create(
 * Flux.just(1, 3, 5, 2, 4, 6, 11, 12, 13)
 * .groupBy(i -> i % 2 == 0 ? "even" : "odd")
 * .concatMap(g -> g.defaultIfEmpty(-1) //if empty groups, show them
 * .map(String::valueOf) //map to string
 * .startWith(g.key())) //start with the group's key
 * )
 * .expectNext("odd", "1", "3", "5", "11", "13")
 * .expectNext("even", "2", "4", "6", "12")
 * .verifyComplete();
 * <p>
 * Grouping is best suited for when you have a medium to low number of groups.
 * The groups must also imperatively be consumed (such as by a flatMap) so that
 * groupBy continues fetching data from upstream and feeding more groups.
 * Sometimes, these two constraints multiply and lead to hangs,
 * such as when you have a high cardinality and
 * the concurrency of the flatMap consuming the groups is too low.
 */
public class GroupByTest {

    /**
     * Пример, аналогичный доке, тут все ОК, так как у нас мало элементов в потоке.
     * Но почему-то concatMap, а не flatMap, как рекомендует реактивная дока.
     * Порядок групп в результирующей последовательности соответствует порядку их создания,
     * элементы групп не перемешиваются.
     * Что вроде бы тоже логично, так как concatMap последовательно подписывается
     * на GroupedFlux'ы каждой отдельной группы
     */
    @Test
    void groupByWithConcatMapPasses() {
        int groupsCount = 5;
        StepVerifier.create(
                        Flux.just(1, 3, 5, 2, 4, 6, 11, 12, 13)
                                .log("range", Level.INFO, SignalType.REQUEST, SignalType.ON_NEXT)
                                .groupBy(i -> "modulo is %s:".formatted(i % groupsCount))
                                .concatMap((GroupedFlux<String, Integer> g) ->
                                        g.defaultIfEmpty(-1)
                                                .map(String::valueOf)
                                                .startWith(g.key()))
                                .log("concatMap", Level.INFO, SignalType.ON_NEXT, SignalType.CANCEL)
                )
                .expectNext("modulo is 1:", "1", "6", "11")
                .expectNext("modulo is 3:", "3", "13")
                .expectNext("modulo is 0:", "5")
                .expectNext("modulo is 2:", "2", "12")
                .expectNext("modulo is 4:", "4")
                .verifyComplete();
    }

    /**
     * Аналогично {@link #groupByWithConcatMapPasses()}, только собираем элементы теперь flatMap'ом.
     * На той же изначальной последовательности результирующая последовательность другая, что логично.
     * FlatMap жадно подписался на все GroupedFlux'ы и прокидывает элементы дальше сразу по мере получения
     */
    @Test
    void groupByWithFlatMapPasses() {
        int groupsCount = 5;
        StepVerifier.create(
                        Flux.just(1, 3, 5, 2, 4, 6, 11, 12, 13)
                                .groupBy(i -> "modulo is %s:".formatted(i % groupsCount))
                                .flatMap((GroupedFlux<String, Integer> g) ->
                                        g.defaultIfEmpty(-1)
                                                .map(String::valueOf)
                                                .startWith(g.key()))
                )
                .expectNext("modulo is 1:", "1")
                .expectNext("modulo is 3:", "3")
                .expectNext("modulo is 0:", "5")
                .expectNext("modulo is 2:", "2")
                .expectNext("modulo is 4:", "4")
                .expectNext("6", "11", "12", "13")
                .verifyComplete();
    }

    /**
     * Аналогично {@link #groupByWithFlatMapPasses()},
     * но конкурентность у flatMap стала меньше, чем количество групп.
     * Пока все работает, только поменялась результирующая последовательность.
     * Логично, flatMap сначала вычитал первые flatMapConcurrency групп,
     * потом подписался на следующую группу и вычитал её
     */
    @Test
    void groupByWithFlatMapPassesInStrangeOrder() {
        int groupsAmount = 5;
        int flatMapConcurrency = groupsAmount - 1;
        StepVerifier.create(
                        Flux.just(1, 3, 5, 2, 4, 6, 11, 12, 13)
                                .groupBy(i -> "modulo is %s:".formatted(i % groupsAmount))
                                .flatMap((GroupedFlux<String, Integer> g) ->
                                                g.defaultIfEmpty(-1)
                                                        .map(String::valueOf)
                                                        .startWith(g.key()),
                                        flatMapConcurrency)
                )
                .expectNext("modulo is 1:", "1")
                .expectNext("modulo is 3:", "3")
                .expectNext("modulo is 0:", "5")
                .expectNext("modulo is 2:", "2")
                .expectNext("6", "11", "12", "13")
                .expectNext("modulo is 4:", "4")
                .verifyComplete();
    }

    /**
     * Аналогично {@link #groupByWithFlatMapPassesInStrangeOrder()},
     * но при той же маленькой конкурентности flatMap сделаем маленький prefetch у groupBy.
     * Поменяем количество групп с 5 до 3.
     * Элементы теперь создаем через range.
     * Пока тоже работает. Может дока врёт? Не врёт, но об этом в следующем тесте
     * {@link #groupByWithFlatMapTimeoutBecauseOfSmallPrefetch10ElementsOfRange()}
     */
    @Test
    void groupByWithFlatMapFineWithSmallPrefetchOn9Elements() {
        int elementsCount = 9;
        int groupsAmount = 3;
        int flatMapConcurrency = 2;
        int groupByPrefetch = 3;
        StepVerifier.create(
                        Flux.range(0, elementsCount)
                                .log("range", Level.INFO, SignalType.REQUEST, SignalType.ON_NEXT)
                                .groupBy(
                                        i -> "modulo is %s:".formatted(i % groupsAmount),
                                        groupByPrefetch
                                )
                                .flatMap((GroupedFlux<String, Integer> g) ->
                                                g.log("groupedFlux " + g.key(), Level.INFO, SignalType.REQUEST, SignalType.ON_NEXT)
                                                        .map(String::valueOf)
                                                        .startWith(g.key()),
                                        flatMapConcurrency)
                                .log("flatMapped", Level.INFO, SignalType.ON_NEXT, SignalType.CANCEL)
                )
                .expectNext("modulo is 0:", "0")
                .expectNext("modulo is 1:", "1")
                .expectNext("3", "4", "6", "7")
                .expectNext("modulo is 2:", "2", "5", "8")
                .verifyComplete();
    }

    /**
     * Всё то же самое, что в предыдущем тесте, но количество элементов вместо 9 стало 10.
     * Обратите внимание на verifyTimeout(stepVerifierTimeout) в конце теста.
     * В предыдущем примере {@link #groupByWithFlatMapFineWithSmallPrefetchOn9Elements()}
     * последовательность завершалась успешно сигналом complete.
     * Ага! Сломалось! Сломалось?
     * groupBy честно насоздавал 3 (groupsAmount) группы и положил в них элементы.
     * - modulo is 0: 0, 3, 6
     * - modulo is 1: 1, 4, 7
     * - modulo is 2: 2, 5, 8
     * На первые две (flatMapConcurrency) подписался flatMap и честно вычитал их в порядке получения:
     * - "modulo is 0:", "0"
     * - "modulo is 1:", "1"
     * - "3", "4", "6", "7"
     * А дальше всё застряло. Потому что range готов отдать нам ещё 1 элемент "9".
     * Но у groupBy уже есть 3 (groupByPrefetch) элемента,
     * которые он вычитал, но никуда не отправил. А не отправил он потому, что
     * flatMap подписан только на две первые группы, а на третью группу пока никто не подписан.
     * В предыдущем примере с 9 элементами последовательность в этот момент заканчивалась,
     * первые два GroupedFlux'а отправляли complet в flatMap и он мог перейти к последней группе.
     * В этом примере groupBy не знает, когда закончится последовательность и всё повисает
     */
    @Test
    void groupByWithFlatMapTimeoutBecauseOfSmallPrefetch10ElementsOfRange() {
        int elementsCount = 10;
        int groupsAmount = 3;
        int flatMapConcurrency = 2;
        int groupByPrefetch = 3;
        Duration stepVerifierTimeout = Duration.ofSeconds(3);
        StepVerifier.create(
                        Flux.range(0, elementsCount)
                                .log("range", Level.INFO, SignalType.REQUEST, SignalType.ON_NEXT)
                                .groupBy(
                                        i -> "modulo is %s:".formatted(i % groupsAmount),
                                        groupByPrefetch
                                )
                                .flatMap((GroupedFlux<String, Integer> g) ->
                                                g.log("groupedFlux " + g.key(), Level.INFO, SignalType.REQUEST, SignalType.ON_NEXT)
                                                        .map(String::valueOf)
                                                        .startWith(g.key()),
                                        flatMapConcurrency)
                                .log("flatMapped", Level.INFO, SignalType.ON_NEXT, SignalType.CANCEL)
                )
                .expectNext("modulo is 0:", "0")
                .expectNext("modulo is 1:", "1")
                .expectNext("3", "4", "6", "7")
                .verifyTimeout(stepVerifierTimeout);
    }

    /**
     * Тот же пример, что и в {@link #groupByWithFlatMapTimeoutBecauseOfSmallPrefetch10ElementsOfRange()},
     * но с другим количеством групп и элементов. В подписчика отправились все элементы из первых двух групп,
     * которые мы успели получить
     */
    @Test
    void groupByWithFlatMapTimeoutBecauseOfSmallPrefetchOnSpecificElements4Groups() {
        int groupsAmount = 4;
        int flatMapConcurrency = 2;
        int groupByPrefetch = 3;
        Duration stepVerifierTimeout = Duration.ofSeconds(3);
        StepVerifier.create(
                        Flux.just(
                                        0, 4, 8, // 0 group
                                        1, // 1 group
                                        12, 16, 20, 24, 28, // 0 group
                                        2, 6, // 2 group, never requested by flatMap
                                        7, // 3 group, never requested by flatMap
                                        32 // will never be requested by groupBy
                                )
                                .log("range", Level.INFO, SignalType.REQUEST, SignalType.ON_NEXT)
                                .groupBy(
                                        i -> "modulo is %s:".formatted(i % groupsAmount),
                                        groupByPrefetch
                                )
                                .flatMap((GroupedFlux<String, Integer> g) ->
                                                g.log("groupedFlux " + g.key(), Level.INFO, SignalType.REQUEST, SignalType.ON_NEXT)
                                                        .map(String::valueOf)
                                                        .startWith(g.key()),
                                        flatMapConcurrency)
                                .log("flatMapped", Level.INFO, SignalType.ON_NEXT, SignalType.CANCEL)
                )
                .expectNext("modulo is 0:", "0", "4", "8")
                .expectNext("modulo is 1:", "1")
                .expectNext("12", "16", "20", "24", "28")
                .verifyTimeout(stepVerifierTimeout);
    }

    @Test
    void groupByWithConcatMapFineWithBigPrefetch() {
        int elementsCount = 10;
        int groupsAmount = 3;
        int groupByPrefetch = 10;
        StepVerifier.create(
                        Flux.range(0, elementsCount)
                                .log("range", Level.INFO, SignalType.REQUEST, SignalType.ON_NEXT)
                                .groupBy(i -> "modulo is %s:".formatted(i % groupsAmount), groupByPrefetch)
                                .concatMap((GroupedFlux<String, Integer> g) ->
                                        g.defaultIfEmpty(-1)
                                                .map(String::valueOf)
                                                .startWith(g.key()))
                                .log("concatMap", Level.INFO, SignalType.ON_NEXT, SignalType.CANCEL)
                )
                .expectNext("modulo is 0:", "0", "3", "6", "9")
                .expectNext("modulo is 1:", "1", "4", "7")
                .expectNext("modulo is 2:", "2", "5", "8")
                .verifyComplete();
    }

    @Test
    void groupByWithConcatMapTimeoutWithSmallPrefetch() {
        int elementsCount = 10;
        int groupsAmount = 3;
        int groupByPrefetch = 3;
        Duration stepVerifierTimeout = Duration.ofSeconds(3);
        StepVerifier.create(
                        Flux.range(0, elementsCount)
                                .log("range", Level.INFO, SignalType.REQUEST, SignalType.ON_NEXT)
                                .groupBy(i -> "modulo is %s:".formatted(i % groupsAmount), groupByPrefetch)
                                .concatMap((GroupedFlux<String, Integer> g) ->
                                        g.defaultIfEmpty(-1)
                                                .map(String::valueOf)
                                                .startWith(g.key()))
                                .log("concatMap", Level.INFO, SignalType.ON_NEXT, SignalType.CANCEL)
                )
                .expectNext("modulo is 0:", "0")
                .verifyTimeout(stepVerifierTimeout);
    }
}
