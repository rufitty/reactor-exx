package ru.alfabank.mobile.reactor.exx.operators;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.SignalType;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.logging.Level;

public class GroupByTest {

    @Test
    void groupByWithConcatMapPasses() {
        StepVerifier.create(
                        Flux.just(1, 3, 5, 2, 4, 6, 11, 12, 13)
                                .log("range", Level.INFO, SignalType.REQUEST, SignalType.ON_NEXT)
                                .groupBy(i -> "modulo is %s:".formatted(i % 5))
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

    @Test
    void groupByWithFlatMapTimeoutBecauseOfSmallPrefetchOnSpecificElements3Groups() {
        int groupsAmount = 3;
        int flatMapConcurrency = 2;
        int groupByPrefetch = 3;
        Duration stepVerifierTimeout = Duration.ofSeconds(3);
        StepVerifier.create(
                        Flux.just(
                                        0, // 0 group
                                        1, // 1 group
                                        2, 5, 8, // 2 group
                                        9 // will never be requested
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
                .expectNext("modulo is 0:", "0")
                .expectNext("modulo is 1:", "1")
                .verifyTimeout(stepVerifierTimeout);
    }

    @Test
    void groupByWithFlatMapTimeoutBecauseOfSmallPrefetchOnSpecificElements4Groups() {
        int groupsAmount = 4;
        int flatMapConcurrency = 2;
        int groupByPrefetch = 3;
        Duration stepVerifierTimeout = Duration.ofSeconds(3);
        StepVerifier.create(
                        Flux.just(
                                        0, // 0 group
                                        1, // 1 group
                                        2, 6, // 2 group
                                        3, 7, //3 group
                                        4 // will never be requested
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
                .expectNext("modulo is 0:", "0")
                .expectNext("modulo is 1:", "1")
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
