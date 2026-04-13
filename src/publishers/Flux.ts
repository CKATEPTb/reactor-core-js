import {PipePublisher} from "@/publishers/PipePublisher";
import {Publisher} from "@/publishers/Publisher";
import {Sink} from "@/sinks/Sink";
import {Mono} from "@/publishers/Mono";

export class Flux<T> implements PipePublisher<T> {
    public static generate<T>(generator: ((sink: Sink<T>) => void)): Flux<T> {
        // todo
    }

    public static from<T>(publisher: Publisher<T>): Flux<T> {
        // todo
    }

    public static fromIterable<T>(iterable: Iterable<T>): Flux<T> {
        // todo
    }

    public static range(start: number, count: number): Flux<number> {
        // todo
    }

    public static empty<T = never>(): Flux<T> {
        // todo
    }

    public static defer<T>(factory: () => Flux<T>): Flux<T> {
        // todo
    }

    public first(): Mono<T> {
        // todo
    }

    public last(): Mono<T> {
        // todo
    }

    public count(): Mono<number> {
        // todo
    }

    public hasElements(): Mono<boolean> {
        // todo
    }

    public collect(force: boolean = false): Mono<T[]> {
        // todo
    }

    public indexed(): Flux<[number, T]> {
        // todo
    }

    public skip(n: number): Flux<T> {
        // todo
    }

    public skipWhile(predicate: (value: T) => boolean): Flux<T> {
        // todo
    }

    public skipUntil(other: Publisher<any>): Flux<T> {
        // todo
    }

    public distinct(): Flux<T> {
        // todo
    }

    public distinctUntilChanged(comparator: (previous: T, current: T) => boolean = (previous, current) => previous != current): Flux<T> {
        // todo
    }

    public delayElements(ms: number): Flux<T> {
        // todo
    }

    public concatWith(other: Publisher<T>): Flux<T> {
        // todo
    }

    public mergeWith(other: Publisher<T>): Flux<T> {
        // todo
    }

    public reduce(reducer: (acc: T, next: T) => T): Mono<T> {
        // todo
    }

    public reduceWith<A>(seedFactory: () => A, reducer: (acc: A, next: T) => A): Mono<A> {
        // todo
    }

    public then(): Mono<void> {
        // todo
    }

    public thenEmpty(other: Publisher<any>): Mono<void> {
        // todo
    }
}