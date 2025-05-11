import {Flux, Mono, Schedulers, Subscriber} from '@/.'

describe('Mono Behavior', () => {
    let results: string[]

    beforeEach(() => {
        results = []
    })

    const createSubscriber = (label: string): Subscriber<string> => ({
        onNext: (value) => results.push(`${label}: ${value}`),
        onError: (error) => results.push(`${label}: error: ${error.message}`),
        onComplete: () => results.push(`${label}: complete`)
    })

    test('Mono#just', () => {
        Mono.just('just')
            .subscribe(createSubscriber('just'))
            .request(1)
        expect(results).toEqual(['just: just', 'just: complete'])
    })

    test('Mono#justOrEmpty', () => {
        Mono.justOrEmpty('just')
            .subscribe(createSubscriber('just'))
            .request(1)
        Mono.justOrEmpty(null)
            .cast<string>()
            .subscribe(createSubscriber('just'))
            .request(1)
        expect(results).toEqual(['just: just', 'just: complete', 'just: complete'])
    })

    test('Mono#empty', () => {
        Mono.empty<string>()
            .subscribe(createSubscriber('empty'))
            .request(1)
        expect(results).toEqual(['empty: complete'])
    })

    test('Mono#error', () => {
        Mono.error<string>(new Error('message'))
            .subscribe(createSubscriber('error'))
            .request(1)
        expect(results).toEqual(['error: error: message', 'error: complete'])
    })

    test('Mono#fromPromise', (done) => {
        Mono.fromPromise<string>(Promise.resolve('done'))
            .doFinally(() => {
                expect(results).toEqual(['fromPromise: done', 'fromPromise: complete'])
                done()
            })
            .subscribe(createSubscriber('fromPromise'))
            .request(1)
    })

    test('Mono#defer', () => {
        let called = false
        const mono = Mono.defer(() => {
            called = true
            return Mono.just('deferred')
        })
        expect(called).toEqual(false)
        mono.subscribe(createSubscriber('deferred'))
            .request(1)
        expect(called).toEqual(true)
        expect(results).toEqual(['deferred: deferred', 'deferred: complete'])
    })

    test('Mono#from', () => {
        Mono.from(Flux.range(1, 5))
            .map(String)
            .subscribe(createSubscriber('from'))
            .request(1)
        expect(results).toEqual(['from: 1', 'from: complete'])
    })

    test('Mono#map', () => {
        Mono.just('hello')
            .map(value => value.toUpperCase())
            .subscribe(createSubscriber('map'))
            .request(1)
        expect(results).toEqual(['map: HELLO', 'map: complete'])
    })

    test('Mono#mapNotNull', () => {
        Mono.just('hello')
            .mapNotNull(value => value.toUpperCase())
            .subscribe(createSubscriber('map'))
            .request(1)
        Mono.just('hello')
            .mapNotNull(_ => null)
            .cast<string>()
            .subscribe(createSubscriber('map'))
            .request(1)
        expect(results).toEqual(['map: HELLO', 'map: complete', 'map: complete'])
    })

    test('Mono#flatMap', () => {
        Mono.just('hello')
            .flatMap(value => Mono.just(value.toUpperCase()))
            .subscribe(createSubscriber('flatMap'))
            .request(1)
        expect(results).toEqual(['flatMap: HELLO', 'flatMap: complete'])
    })

    test('Mono#filter', () => {
        Mono.just('hello')
            .filter(value => value == 'hello')
            .subscribe(createSubscriber('filter'))
            .request(1)
        Mono.just('hello')
            .filter(value => value != 'hello')
            .subscribe(createSubscriber('filter'))
            .request(1)
        expect(results).toEqual(['filter: hello', 'filter: complete', 'filter: complete'])
    })

    test('Mono#filterWhen', () => {
        Mono.just('hello')
            .filterWhen(value => Mono.just(value == 'hello'))
            .subscribe(createSubscriber('filterWhen'))
            .request(1)
        Mono.just('hello')
            .filterWhen(value => Mono.just(value != 'hello'))
            .subscribe(createSubscriber('filterWhen'))
            .request(1)
        expect(results).toEqual(['filterWhen: hello', 'filterWhen: complete', 'filterWhen: complete'])
    })

    test('Mono#cast (should return some publisher)', () => {
        const mono = Mono.just(5);
        results.push(String(mono === mono.cast<number>()))
        expect(results).toEqual(['true'])
    })

    test('Mono#switchIfEmpty', () => {
        Mono.empty<string>()
            .switchIfEmpty(Mono.just('default'))
            .subscribe(createSubscriber('switchIfEmpty'))
            .request(1)
        expect(results).toEqual(['switchIfEmpty: default', 'switchIfEmpty: complete'])
    })

    test('Mono#onErrorReturn', () => {
        Mono.error<string>(new Error())
            .onErrorReturn(Mono.just('default'))
            .subscribe(createSubscriber('onErrorReturn'))
            .request(1)
        expect(results).toEqual(['onErrorReturn: default', 'onErrorReturn: complete'])
    })

    test('Mono#onErrorContinue', () => {
        Mono.error<string>(new Error('message'))
            .onErrorContinue(error => error.message == 'message')
            .subscribe(createSubscriber('onErrorContinue'))
            .request(1)
        Mono.error<string>(new Error('message'))
            .onErrorContinue(error => error.message != 'message')
            .subscribe(createSubscriber('onErrorContinue'))
            .request(1)
        expect(results).toEqual(['onErrorContinue: complete', 'onErrorContinue: error: message', 'onErrorContinue: complete'])
    })

    test('Mono#doOnNext', () => {
        Mono.just<string>('done')
            .doOnNext(value => results.push(value))
            .subscribe(createSubscriber('doOnNext'))
            .request(1)
        expect(results).toEqual(['done', 'doOnNext: done', 'doOnNext: complete'])
    })

    test('Mono#doOnError', () => {
        Mono.error<string>(new Error('message'))
            .doOnError(value => results.push(value.message))
            .subscribe(createSubscriber('doOnError'))
            .request(1)
        expect(results).toEqual(['message', 'doOnError: error: message', 'doOnError: complete'])
    })

    test('Mono#doFirst', () => {
        Mono.just<string>('done')
            .doFirst(() => results.push('first_1'))
            .doOnNext(value => results.push(value))
            .doFirst(() => results.push('first_2'))
            .subscribe(createSubscriber('doFirst'))
            .request(1)
        expect(results).toEqual(['first_2', 'first_1', 'done', 'doFirst: done', 'doFirst: complete'])
        results = []
        Mono.error<string>(new Error('message'))
            .doFirst(() => results.push('first_1'))
            .doFirst(() => results.push('first_2'))
            .subscribe(createSubscriber('doFirst'))
            .request(1)
        expect(results).toEqual(['first_2', 'first_1', 'doFirst: error: message', 'doFirst: complete'])
        results = []
        Mono.empty<string>()
            .doFirst(() => results.push('first_1'))
            .doFirst(() => results.push('first_2'))
            .subscribe(createSubscriber('doFirst'))
            .request(1)
        expect(results).toEqual(['first_2', 'first_1', 'doFirst: complete'])
    })

    test('Mono#doFinally', () => {
        Mono.just<string>('done')
            .doOnNext(value => results.push(value))
            .doFinally(() => results.push('finally'))
            .doOnNext(value => results.push(value))
            .subscribe(createSubscriber('doFinally'))
            .request(1)
        expect(results).toEqual(['done', 'done', 'doFinally: done', 'doFinally: complete', 'finally'])
        results = []
        Mono.error<string>(new Error('message'))
            .doFinally(() => results.push('finally'))
            .subscribe(createSubscriber('doFinally'))
            .request(1)
        expect(results).toEqual(['doFinally: error: message', 'doFinally: complete', 'finally'])
        results = []
        Mono.empty<string>()
            .doFinally(() => results.push('finally'))
            .subscribe(createSubscriber('doFinally'))
            .request(1)
        expect(results).toEqual(['doFinally: complete', 'finally'])
    })

    test('Mono#doOnSubscribe', () => {
        Mono.just<string>('done')
            .doOnNext(value => results.push(value))
            .doOnNext(value => results.push(value))
            .doOnSubscribe(subscription => {
                subscription.request(1)
                results.push('onSubscribe')
            })
            .subscribe(createSubscriber('doOnSubscribe'))
        expect(results).toEqual(['done', 'done', 'doOnSubscribe: done', 'doOnSubscribe: complete', 'onSubscribe'])
    })

    test('Mono#publishOn', (done) => {
        Mono.just('async')
            .publishOn(Schedulers.macro())
            .subscribe(createSubscriber('publishOn'))
            .request(1)
        expect(results).toEqual([])
        Schedulers.macro().schedule(() => {
            expect(results).toEqual(['publishOn: async', 'publishOn: complete'])
            done()
        })
    })

    test('Mono#subscribeOn', (done) => {
        Mono.just('async-sub')
            .subscribeOn(Schedulers.micro())
            .subscribe(createSubscriber('subscribeOn'))
            .request(1)
        expect(results).toEqual([])
        Schedulers.macro().schedule(() => {
            expect(results).toEqual(['subscribeOn: async-sub', 'subscribeOn: complete'])
            done()
        })
    })

    test('Mono#zipWith', () => {
        Mono.just('A')
            .zipWith(Mono.just('B'))
            .map(String)
            .subscribe(createSubscriber('zipWith'))
            .request(1)
        expect(results).toEqual(['zipWith: A,B', 'zipWith: complete'])
    })

    test('Mono#zipWhen', () => {
        Mono.just('A')
            .zipWhen(value => Mono.just(value))
            .map(String)
            .subscribe(createSubscriber('zipWhen'))
            .request(1)
        expect(results).toEqual(['zipWhen: A,A', 'zipWhen: complete'])
    })

    test('Mono#hasElement', () => {
        Mono.just('exists')
            .hasElement()
            .map(String)
            .subscribe(createSubscriber('hasElement'))
            .request(1)
        Mono.empty()
            .hasElement()
            .map(String)
            .subscribe(createSubscriber('hasElement'))
            .request(1)
        expect(results).toEqual([
            'hasElement: true', 'hasElement: complete',
            'hasElement: false', 'hasElement: complete'
        ])
    })

    test('Mono#toPromise', async () => {
        expect(await Mono.just('async-promise').toPromise()).toBe('async-promise')
        await Mono.error(new Error('promise-error')).toPromise()
            .catch((reason: Error) => results.push(reason.message))
        expect(results).toEqual(['promise-error'])
    })

    test('Mono#flatMapMany', () => {
        Mono.just('item')
            .flatMapMany(value => Flux.range(1, value.length)
                .map(num => `${value}-${num}`))
            .subscribe(createSubscriber('flatMapMany'))
            .request(4)
        expect(results).toEqual([
            'flatMapMany: item-1',
            'flatMapMany: item-2',
            'flatMapMany: item-3',
            'flatMapMany: item-4',
            'flatMapMany: complete'
        ])
    })

    test('Mono should not allow multiple subscriptions', () => {
        const mono = Mono.just('unique')
        mono.subscribe(createSubscriber('first'))
        expect(() => {
            mono.subscribe(createSubscriber('second'))
        }).toThrow("The completed sink is not accepting new emits.")
    })
})
