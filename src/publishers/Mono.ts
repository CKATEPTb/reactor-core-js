import {Publisher} from "@/publishers/Publisher";
import {Sink} from "@/sinks/Sink";
import {Flux} from "@/publishers/Flux";
import {PipePublisher} from "@/publishers/PipePublisher";

export class Mono<T> implements PipePublisher<T> {

    protected constructor(publisher: Publisher<T>) {
        // todo
    }

    public static generate<T>(generator: ((sink: Sink<T>) => void)): Mono<T> {
        // todo
    }

    public static from<T>(publisher: Publisher<T>): Mono<T> {
        // todo
    }
    public static just<T>(value: T): Mono<T> {
        // todo
    }

    public static justOrEmpty<T>(value: T | null | undefined): Mono<T> {
        // todo
    }

    public static empty<T = never>(): Mono<T> {
        // todo
    }

    public static error<T = never>(error: any): Mono<T> {
        // todo
    }

    public static fromPromise<T>(promise: Promise<T>): Mono<T> {
        return Mono.generate(sink => promise
            .then((value) => sink.next(value))
            .catch((err) => sink.error(err)))
    }

    public static defer<T>(factory: () => Mono<T>): Mono<T> {
        // todo
    }

    public flatMapMany<R>(mapper: (value: T) => Publisher<R>): Flux<R> {
        // todo
    }

    public zipWith<R>(other: Mono<R>): Mono<[T, R]> {
        // todo
    }

    public zipWhen<R>(fn: (value: T) => Mono<R>): Mono<[T, R]> {
        // todo
    }

    public hasElement(): Mono<boolean> {
        // todo
    }

    public toPromise(): Promise<T | null> {
        // todo
    }
}
