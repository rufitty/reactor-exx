package ru.alfabank.mobile.reactor.exx;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.SignalType;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.logging.Level;

import static java.util.function.Function.identity;

public class GroupByTest {

    @Test
    void groupByWithConcatMapPasses() {
        StepVerifier.create(
                        Flux.just(1, 3, 5, 2, 4, 6, 11, 12, 13)
                                .groupBy(i -> "modulo is %s:".formatted(i % 5), 4)
                                .concatMap((GroupedFlux<String, Integer> g) ->
                                        g.defaultIfEmpty(-1)
                                                .map(String::valueOf)
                                                .startWith(g.key()))
                )
                .expectNext("modulo is 1:", "1", "6", "11")
                .expectNext("modulo is 3:", "3", "13")
                .expectNext("modulo is 0:", "5")
                .expectNext("modulo is 2:", "2", "12")
                .expectNext("modulo is 4:", "4")
                .verifyComplete();
    }

    @Test
    void groupByWithFlatMapPasses() {
        StepVerifier.create(
                        Flux.just(1, 3, 5, 2, 4, 6, 11, 12, 13)
                                .groupBy(i -> "modulo is %s:".formatted(i % 5))
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

    @Test
    void groupByWithFlatMapHangsBecauseOfSmallPrefetch() {
        int elementsCount = 10;
        int groupsAmount = 3;
        int flatMapConcurrency = 2;
        Duration stepVerifierTimeout = Duration.ofSeconds(10);
        int groupByPrefetch = 3;
        StepVerifier.create(
                        Flux.range(0, elementsCount)
                                .log("range", Level.INFO, SignalType.REQUEST)
                                .groupBy(
                                        i -> "modulo is %s:".formatted(i % groupsAmount),
                                        identity(),
                                        groupByPrefetch
                                )
                                .flatMap((GroupedFlux<String, Integer> g) ->
                                                g.map(String::valueOf)
                                                        .startWith(g.key()),
                                        flatMapConcurrency)
                                .log("flatMapped", Level.INFO, SignalType.ON_NEXT, SignalType.CANCEL)
                )
                .expectNextCount(8)
                .verifyTimeout(stepVerifierTimeout);
    }
}
