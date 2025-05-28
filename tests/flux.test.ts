import {Flux, Mono, Schedulers, Sinks, Subscriber} from '@/.'

describe('Flux Behavior', () => {
    let results: string[]

    beforeEach(() => {
        results = []
    })

    const createSubscriber = (label: string): Subscriber<string> => ({
        onNext: (value) => results.push(`${label}: ${value}`),
        onError: (error) => results.push(`${label}: error: ${error.message}`),
        onComplete: () => results.push(`${label}: complete`)
    })

    test('Flux#fromIterable', () => {
        Flux.fromIterable(['Hello', 'World', '!'])
            .subscribe(createSubscriber('fromIterable'))
            .request(3)
        expect(results).toEqual(['fromIterable: Hello', 'fromIterable: World', 'fromIterable: !', 'fromIterable: complete'])
    })

    test('Flux#range', () => {
        Flux.range(1, 3)
            .map(String)
            .subscribe(createSubscriber('range'))
            .request(3)
        expect(results).toEqual(['range: 1', 'range: 2', 'range: 3', 'range: complete'])
    })

    test('Flux#empty', () => {
        Flux.empty<string>()
            .subscribe(createSubscriber('empty'))
            .request(1)
        expect(results).toEqual(['empty: complete'])
    })

    test('Flux#defer', () => {
        let called = false
        const flux = Flux.defer(() => {
            called = true
            return Flux.from(Mono.just('deferred'))
        })
        expect(called).toEqual(false)
        flux.subscribe(createSubscriber('deferred'))
            .request(1)
        expect(called).toEqual(true)
        expect(results).toEqual(['deferred: deferred', 'deferred: complete'])
    })

    test('Flux#from', () => {
        Flux.from(Mono.just('done'))
            .subscribe(createSubscriber('from'))
            .request(1)
        expect(results).toEqual(['from: done', 'from: complete'])
    })

    test('Flux#map', () => {
        Flux.from(Mono.just('hello'))
            .map(value => value.toUpperCase())
            .subscribe(createSubscriber('map'))
            .request(1)
        expect(results).toEqual(['map: HELLO', 'map: complete'])
    })

    test('Flux#mapNotNull', () => {
        Flux.from(Mono.just('hello'))
            .mapNotNull(value => value.toUpperCase())
            .subscribe(createSubscriber('map'))
            .request(1)
        Flux.from(Mono.just('hello'))
            .mapNotNull(_ => null)
            .cast<string>()
            .subscribe(createSubscriber('map'))
            .request(1)
        expect(results).toEqual(['map: HELLO', 'map: complete', 'map: complete'])
    })

    test('Flux#flatMap', (done) => {
        Flux.from(Mono.just('hello'))
            .flatMap(value => Mono.just(value.toUpperCase()))
            .subscribe(createSubscriber('flatMap'))
            .request(1)
        // TODO тест должен быть синхронным, но к сожалению текущая реализация flatMap работает иначе
        Schedulers.macro().schedule(() => {
            expect(results).toEqual(['flatMap: HELLO', 'flatMap: complete'])
            done()
        })
    })

    test('Flux#filter', () => {
        Flux.range(1, 6)
            .filter(value => value % 2 == 0)
            .map(String)
            .subscribe(createSubscriber('filter'))
            .request(3)
        Flux.range(1, 6)
            .filter(_ => false)
            .map(String)
            .subscribe(createSubscriber('filter'))
            .request(3)
        expect(results).toEqual(['filter: 2', 'filter: 4', 'filter: 6', 'filter: complete', 'filter: complete'])
    })

    test('Flux#filterWhen', () => {
        Flux.range(1, 6)
            .filterWhen(value => Mono.just(value % 2 == 0))
            .map(String)
            .subscribe(createSubscriber('filter'))
            .request(3)
        Flux.range(1, 6)
            .filterWhen(_ => Mono.just(false))
            .map(String)
            .subscribe(createSubscriber('filter'))
            .request(3)
        expect(results).toEqual(['filter: 2', 'filter: 4', 'filter: 6', 'filter: complete', 'filter: complete'])
    })

    test('Flux#cast (should return some publisher)', () => {
        const flux = Flux.from(Mono.just(5));
        results.push(String(flux === flux.cast<number>()))
        expect(results).toEqual(['true'])
    })

    test('Flux#switchIfEmpty', () => {
        Flux.empty<string>()
            .switchIfEmpty(Mono.just('default'))
            .subscribe(createSubscriber('switchIfEmpty'))
            .request(5)
        expect(results).toEqual(['switchIfEmpty: default', 'switchIfEmpty: complete'])
    })

    test('Flux#onErrorReturn', () => {
        Flux.from(Mono.error<string>(new Error()))
            .onErrorReturn(Mono.just('default'))
            .subscribe(createSubscriber('onErrorReturn'))
            .request(1)
        expect(results).toEqual(['onErrorReturn: default', 'onErrorReturn: complete'])
    })

    test('Flux#onErrorContinue', () => {
        Flux.from(Mono.error<string>(new Error('message')))
            .onErrorContinue(error => error.message == 'message')
            .subscribe(createSubscriber('onErrorContinue'))
            .request(1)
        Flux.from(Mono.error<string>(new Error('message')))
            .onErrorContinue(error => error.message != 'message')
            .subscribe(createSubscriber('onErrorContinue'))
            .request(1)
        expect(results).toEqual(['onErrorContinue: complete', 'onErrorContinue: error: message', 'onErrorContinue: complete'])
    })

    test('Flux#doOnNext', () => {
        Flux.from(Mono.just<string>('done'))
            .doOnNext(value => results.push(value))
            .subscribe(createSubscriber('doOnNext'))
            .request(1)
        expect(results).toEqual(['done', 'doOnNext: done', 'doOnNext: complete'])
    })

    test('Flux#doOnError', () => {
        Flux.from(Mono.error<string>(new Error('message')))
            .doOnError(value => results.push(value.message))
            .subscribe(createSubscriber('doOnError'))
            .request(1)
        expect(results).toEqual(['message', 'doOnError: error: message', 'doOnError: complete'])
    })

    test('Flux#doFirst', () => {
        Flux.range(1, 5)
            .doFirst(() => results.push('first_1'))
            .map(String)
            .doFirst(() => results.push('first_2'))
            .doOnNext(value => results.push(value))
            .doFirst(() => results.push('first_3'))
            .subscribe(createSubscriber('doFirst'))
            .request(5)
        expect(results).toEqual([
            ...[3, 2, 1].map(value => `first_${value}`),
            ...[1, 2, 3, 4, 5].flatMap(val => [`${val}`, `doFirst: ${val}`]),
            'doFirst: complete'
        ])
        results = []
        Flux.range(1, 5)
            .map(value => {
                throw new Error(String(value))
            })
            .doFirst(() => results.push('first_1'))
            .map(String)
            .doFirst(() => results.push('first_2'))
            .doOnNext(value => results.push(value))
            .doFirst(() => results.push('first_3'))
            .subscribe(createSubscriber('doFirst'))
            .request(5)
        expect(results).toEqual([
            ...[3, 2, 1].map(value => `first_${value}`),
            ...[1, 2, 3, 4, 5].map(val => `doFirst: error: ${val}`),
            'doFirst: complete'
        ])
        results = []
        Flux.empty()
            .doFirst(() => results.push('first_1'))
            .map(String)
            .doFirst(() => results.push('first_2'))
            .doOnNext(value => results.push(value))
            .doFirst(() => results.push('first_3'))
            .subscribe(createSubscriber('doFirst'))
            .request(1)
        expect(results).toEqual([
            ...[3, 2, 1].map(value => `first_${value}`),
            'doFirst: complete'
        ])
    })

    test('Flux#doFinally', () => {
        Flux.range(1, 5)
            .doFinally(() => results.push('finally_1'))
            .map(String)
            .doFinally(() => results.push('finally_2'))
            .doOnNext(value => results.push(value))
            .doFinally(() => results.push('finally_3'))
            .subscribe(createSubscriber('doFinally'))
            .request(5)
        expect(results).toEqual([
            ...[1, 2, 3, 4, 5].flatMap(val => [`${val}`, `doFinally: ${val}`]),
            'doFinally: complete',
            ...[1, 2, 3].map(value => `finally_${value}`)
        ])
        results = []
        Flux.range(1, 5)
            .map(value => {
                throw new Error(String(value))
            })
            .doFinally(() => results.push('finally_1'))
            .map(String)
            .doFinally(() => results.push('finally_2'))
            .doOnNext(value => results.push(value))
            .doFinally(() => results.push('finally_3'))
            .subscribe(createSubscriber('doFinally'))
            .request(5)
        expect(results).toEqual([
            ...[1, 2, 3, 4, 5].map(val => `doFinally: error: ${val}`),
            'doFinally: complete',
            ...[1, 2, 3].map(value => `finally_${value}`)
        ])
        results = []
        Flux.empty()
            .doFinally(() => results.push('finally_1'))
            .map(String)
            .doFinally(() => results.push('finally_2'))
            .doOnNext(value => results.push(value))
            .doFinally(() => results.push('finally_3'))
            .subscribe(createSubscriber('doFinally'))
            .request(5)
        expect(results).toEqual([
            'doFinally: complete',
            ...[1, 2, 3].map(value => `finally_${value}`)
        ])
    })

    test('Flux#doOnSubscribe', () => {
        Flux.range(1, 2)
            .map(String)
            .doOnNext(value => results.push(value))
            .doOnNext(value => results.push(value))
            .doOnSubscribe(subscription => {
                subscription.request(2)
                results.push('onSubscribe')
            })
            .subscribe(createSubscriber('doOnSubscribe'))
        expect(results).toEqual([
            '1', '1', 'doOnSubscribe: 1',
            '2', '2', 'doOnSubscribe: 2',
            'doOnSubscribe: complete', 'onSubscribe'
        ])
    })

    test('Flux#publishOn', (done) => {
        Flux.from(Mono.just('async'))
            .publishOn(Schedulers.macro())
            .subscribe(createSubscriber('publishOn'))
            .request(1)
        expect(results).toEqual([])
        Schedulers.macro().schedule(() => {
            expect(results).toEqual(['publishOn: async', 'publishOn: complete'])
            done()
        })
    })

    test('Flux#subscribeOn', (done) => {
        Flux.from(Mono.just('async-sub'))
            .subscribeOn(Schedulers.micro())
            .subscribe(createSubscriber('subscribeOn'))
            .request(1)
        expect(results).toEqual([])
        Schedulers.macro().schedule(() => {
            expect(results).toEqual(['subscribeOn: async-sub', 'subscribeOn: complete'])
            done()
        })
    })

    test('Flux#first', () => {
        Flux.range(1, 5)
            .first()
            .map(String)
            .subscribe(createSubscriber('first'))
            .request(1)
        expect(results).toEqual(['first: 1', 'first: complete'])
    })

    test('Flux#last', () => {
        Flux.range(1, 5)
            .last()
            .map(String)
            .subscribe(createSubscriber('last'))
            .request(1)
        expect(results).toEqual(['last: 5', 'last: complete'])
    })

    test('Flux#count', () => {
        Flux.range(1, 5)
            .count()
            .map(String)
            .subscribe(createSubscriber('count'))
            .request(1)
        expect(results).toEqual(['count: 5', 'count: complete'])
    })

    test('Flux#hasElements', () => {
        Flux.range(1, 5)
            .hasElements()
            .map(String)
            .subscribe(createSubscriber('hasElements'))
            .request(1)
        Flux.empty()
            .hasElements()
            .map(String)
            .subscribe(createSubscriber('hasElements'))
            .request(1)
        expect(results).toEqual([
            'hasElements: true', 'hasElements: complete',
            'hasElements: false', 'hasElements: complete'
        ])
    })

    test('Flux#collect', () => {
        Flux.range(1, 5)
            .collect()
            .map(String)
            .subscribe(createSubscriber('collect'))
            .request(1)
        expect(results).toEqual([`collect: ${[1, 2, 3, 4, 5]}`, 'collect: complete'])
    })

    test('Flux#indexed', () => {
        Flux.range(1, 5)
            .indexed()
            .map(String)
            .subscribe(createSubscriber('indexed'))
            .request(5)
        expect(results).toEqual([
            ...[1, 2, 3, 4, 5]
                .map((value, index) => `indexed: ${[index, value]}`),
            'indexed: complete'
        ])
    })

    test('Flux#skip', () => {
        Flux.range(1, 5)
            .skip(3)
            .map(String)
            .subscribe(createSubscriber('skip'))
            .request(2)
        expect(results).toEqual([
            ...[4, 5]
                .map((value) => `skip: ${value}`),
            'skip: complete'
        ])
    })

    test('Flux#skipWhile', () => {
        Flux.range(1, 5)
            .skipWhile(value => value <= 3)
            .map(String)
            .subscribe(createSubscriber('skipWhile'))
            .request(2)
        expect(results).toEqual([
            ...[4, 5]
                .map((value) => `skipWhile: ${value}`),
            'skipWhile: complete'
        ])
    })

    test('Flux#skipUntil', (done) => {
        const sink = Sinks.one<boolean>();
        Flux.range(1, 5)
            .doFinally(() => {
                expect(results).toEqual([
                    ...[4, 5]
                        .map((value) => `skipUntil: ${value}`),
                    'skipUntil: complete'
                ])
                done()
            })
            .delayElements(1)
            .doOnNext(value => value == 4 && sink.next(true))
            .skipUntil(sink)
            .map(String)
            .subscribe(createSubscriber('skipUntil'))
            .request(2)
    })

    test('Flux#distinct', () => {
        Flux.fromIterable([1, 1, 2, 2, 1, 1])
            .distinct()
            .map(String)
            .subscribe(createSubscriber('distinct'))
            .request(2)
        expect(results).toEqual([
            ...[1, 2]
                .map((value) => `distinct: ${value}`),
            'distinct: complete'
        ])
    })

    test('Flux#distinctUntilChanged', () => {
        Flux.fromIterable([1, 1, 2, 2, 1, 1])
            .distinctUntilChanged()
            .map(String)
            .subscribe(createSubscriber('distinctUntilChanged'))
            .request(3)
        expect(results).toEqual([
            ...[1, 2, 1]
                .map((value) => `distinctUntilChanged: ${value}`),
            'distinctUntilChanged: complete'
        ])
    })

    test('Flux#delayElements', (done) => {
        const times: number[] = []
        Flux.fromIterable([1, 2, 3])
            .doFinally(() => {
                const diff = times
                    .map(value => Date.now() - value)
                    .reduce((acc, cur) => {
                        return acc + cur
                    })
                expect(diff).toBeGreaterThan(30)
                done()
            })
            .delayElements(10)
            .doOnNext(_ => times.push(Date.now()))
            .map(String)
            .subscribe(createSubscriber('delayElements'))
            .request(3)
        expect(results).toEqual([])
    })

    test('Flux#concatWith', () => {
        Flux.range(1, 5)
            .concatWith(Flux.range(6, 2))
            .map(String)
            .subscribe(createSubscriber('concatWith'))
            .request(7)
        expect(results).toEqual([
            ...[1, 2, 3, 4, 5, 6, 7]
                .map((value) => `concatWith: ${value}`),
            'concatWith: complete'
        ])
    })

    test('Flux#mergeWith', (done) => {
        Flux.range(1, 2)
            .doFinally(() => {
                expect(results).toEqual([
                    ...[3, 4, 1, 2]
                        .map((value) => `mergeWith: ${value}`),
                    'mergeWith: complete'
                ])
                done()
            })
            .delayElements(10)
            .mergeWith(Flux.range(3, 2))
            .map(String)
            .subscribe(createSubscriber('mergeWith'))
            .request(4)
    })

    test('Flux#reduce', () => {
        Flux.range(1, 5)
            .reduce((acc, next) => acc + next)
            .map(String)
            .subscribe(createSubscriber('reduce'))
            .request(1)
        expect(results).toEqual([
            'reduce: 15',
            'reduce: complete'
        ])
    })

    test('Flux#reduceWith', () => {
        Flux.range(1, 5)
            .reduceWith(() => 5, (acc, next) => acc + next)
            .map(String)
            .subscribe(createSubscriber('reduceWith'))
            .request(1)
        expect(results).toEqual([
            'reduceWith: 20',
            'reduceWith: complete'
        ])
    })

    test('Flux#then', () => {
        Flux.range(1, 5)
            .then()
            .cast<string>()
            .subscribe(createSubscriber('then'))
            .request(1)
        expect(results).toEqual([
            'then: complete'
        ])
    })

    test('Flux#thenEmpty', (done) => {
        Flux.range(1, 5)
            .doFinally(() => {
                expect(results).toEqual([
                    'thenEmpty: complete'
                ])
                done()
            })
            .thenEmpty(Mono.just(6))
            .cast<string>()
            .subscribe(createSubscriber('thenEmpty'))
            .request(1)
    })
})
