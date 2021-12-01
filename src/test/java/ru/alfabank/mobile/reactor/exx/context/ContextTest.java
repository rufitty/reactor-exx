package ru.alfabank.mobile.reactor.exx.context;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

public class ContextTest {
    private static final Logger log = LoggerFactory.getLogger(ContextTest.class);

    /**
     * Контекст иммутабельный привязывается к конкретному подписчику (в момент подписки).
     * Это значит, что измененный контекст видят только операторы, находящиеся выше по цепочке.
     * <p>
     * В примере выше contextWrite(ContextView) задает первоначальный контекст с ключом wish,
     * а contextWrite(Function<Context, Context>) добавляет в этот контекст новое значение с ключом who
     */
    @Test
    void simpleContext() {
        String who = "World!";
        String wish = "Have a nice day!";
        Flux<String> r = Flux.just("Hello", "Bye")
                .flatMap(s -> Mono.deferContextual(ctx -> {
                    log.info("First  step has 'wish' key {} for {}", ctx.hasKey("wish"), s); // true
                    return Mono.just(s + " " + ctx.get("who"));
                }))
                .contextWrite(ctx ->
                        ctx.put("who", who)
                )
                .flatMap(s -> Mono.deferContextual(ctx -> {
                    log.info("Second step has 'who'  key {} for {}", ctx.hasKey("who"), s); //false
                    return Mono.just(s + " " + ctx.get("wish"));
                }))
                .contextWrite(
                        Context.of("wish", wish)
                )
                .flatMap(s -> Mono.deferContextual(ctx -> {
                    log.info("Third  step has 'who'  key {} for {}", ctx.hasKey("who"), s); //false
                    log.info("Third  step has 'wish' key {} for {}", ctx.hasKey("wish"), s); //false
                    return Mono.just(s);
                }));

        StepVerifier.create(r)
                .expectNext("Hello World! Have a nice day!")
                .expectNext("Bye World! Have a nice day!")
                .verifyComplete();
    }
}
