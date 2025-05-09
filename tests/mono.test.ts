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

    test('Mono should emit a single value and complete', () => {
        Mono.just('Hello')
            .subscribe(createSubscriber('mono'))
        expect(results).toEqual(['mono: Hello', 'mono: complete'])
    })

    test('Mono should complete without emitting any value', () => {
        Mono.empty<string>()
            .subscribe(createSubscriber('empty'))
        expect(results).toEqual(['empty: complete'])
    })

    test('Mono should emit an error and complete', () => {
        Mono.error<string>(new Error('Something went wrong'))
            .subscribe(createSubscriber('error'))
        expect(results).toEqual(['error: error: Something went wrong', 'error: complete'])
    })

    test('Mono map should transform emitted value', () => {
        Mono.just('hello').map(value => value.toUpperCase())
            .subscribe(createSubscriber('mapped'))
        expect(results).toEqual(['mapped: HELLO', 'mapped: complete'])
    })

    test('Mono flatMap should map and flatten another Mono', () => {
        Mono.just('hello').flatMap(value => Mono.just(value.toUpperCase()))
            .subscribe(createSubscriber('flatMapped'))
        expect(results).toEqual(['flatMapped: HELLO', 'flatMapped: complete'])
    })

    test('Mono switchIfEmpty should emit alternative value', () => {
        Mono.empty<string>().switchIfEmpty(Mono.just('default'))
            .subscribe(createSubscriber('switchIfEmpty'))
        expect(results).toEqual(['switchIfEmpty: default', 'switchIfEmpty: complete'])
    })

    test('Mono should not allow multiple subscriptions', () => {
        const mono = Mono.just('unique')
        mono.subscribe(createSubscriber('first'))

        expect(() => {
            mono.subscribe(createSubscriber('second'))
        }).toThrow("The completed sink is not accepting new emits.")

        expect(results).toEqual(['first: unique', 'first: complete'])
    })

    test('Mono should allow defer for lazy evaluation', () => {
        let called = false
        const mono = Mono.defer(() => {
            called = true
            return Mono.just('deferred')
        })
        expect(called).toEqual(false)
        mono.subscribe(createSubscriber('deferred'))
        expect(called).toEqual(true)
        expect(results).toEqual(['deferred: deferred', 'deferred: complete'])
    })

    test('Mono zipWith should combine two Mono values', () => {
        Mono.just('A').zipWith(Mono.just('B')).map(String)
            .subscribe(createSubscriber('zipped'))
        expect(results).toEqual(['zipped: A,B', 'zipped: complete'])
    })

    test('Mono from should emit the value from another publisher', () => {
        Mono.from(Flux.range(1, 5)).map(String)
            .subscribe(createSubscriber('from'))
        expect(results).toEqual(['from: 1', 'from: complete'])
    })

    test('Mono justOrEmpty should emit value or complete if null', () => {
        Mono.justOrEmpty('value').subscribe(createSubscriber('present'))
        Mono.justOrEmpty(null).subscribe(createSubscriber('absent'))
        expect(results).toEqual(['present: value', 'present: complete', 'absent: complete'])
    })

    test('Mono fromPromise should emit resolved value', () => {
        Mono.fromPromise(Promise.resolve('async value')).subscribe(createSubscriber('promise'))
        Schedulers.micro().schedule(() => expect(results).toEqual(['promise: async value', 'promise: complete']))
    })

    test('Mono flatMapMany should convert Mono to Flux', () => {
        Mono.just('item')
            .flatMapMany(value => Flux.range(1, 3).map(num => `${value}-${num}`))
            .subscribe(createSubscriber('flatMapMany')).request(3)
        expect(results).toEqual([
            'flatMapMany: item-1',
            'flatMapMany: item-2',
            'flatMapMany: item-3',
            'flatMapMany: complete'
        ])
    })

    test('Mono zipWhen should combine with another Mono', () => {
        Mono.just('A').zipWhen(() => Mono.just('B')).map(String)
            .subscribe(createSubscriber('zipWhen'))
        expect(results).toEqual(['zipWhen: A,B', 'zipWhen: complete'])
    })

    test('Mono hasElement should check presence of value', () => {
        Mono.just('exists').hasElement().map(String).subscribe(createSubscriber('hasElement'))
        Mono.empty().hasElement().map(String).subscribe(createSubscriber('noElement'))
        expect(results).toEqual([
            'hasElement: true', 'hasElement: complete',
            'noElement: false', 'noElement: complete'
        ])
    })

    test('Mono toPromise should convert to Promise', async () => {
        expect(await Mono.just('async-promise').toPromise()).toBe('async-promise')
    })

    test('Mono mapNotNull should filter out null values', () => {
        Mono.just('hello').mapNotNull(() => null)
            .subscribe(createSubscriber('mapNotNull'))
        expect(results).toEqual(['mapNotNull: complete'])
    })

    test('Mono filter should pass values matching predicate', () => {
        Mono.just(5).filter(value => value > 3).map(String)
            .subscribe(createSubscriber('filter'))
        expect(results).toEqual(['filter: 5', 'filter: complete'])
        Mono.just(5).filter(value => value < 3).map(String)
            .subscribe(createSubscriber('empty'))
        expect(results).toEqual(['filter: 5', 'filter: complete', 'empty: complete'])
    })

    test('Mono filterWhen should pass only when predicate returns true', () => {
        Mono.just(5).filterWhen(() => Mono.just(true)).map(String)
            .subscribe(createSubscriber('filterWhen'))
        expect(results).toEqual(['filterWhen: 5', 'filterWhen: complete'])
        Mono.just(5).filterWhen(() => Mono.just(false)).map(String)
            .subscribe(createSubscriber('empty'))
        expect(results).toEqual(['filterWhen: 5', 'filterWhen: complete', 'empty: complete'])
    })

    test('Mono onErrorReturn should replace error with fallback', () => {
        Mono.error<string>(new Error('fail')).onErrorReturn(Mono.just('fallback'))
            .subscribe(createSubscriber('onErrorReturn'))
        expect(results).toEqual(['onErrorReturn: fallback', 'onErrorReturn: complete'])
    })

    test('Mono onErrorContinue should ignore error based on predicate', () => {
        Mono.error<string>(new Error('ignore')).onErrorContinue(() => true)
            .subscribe(createSubscriber('onErrorContinue'))
        expect(results).toEqual(['onErrorContinue: complete'])
    })

    test('Mono doFirst should execute action before emission', () => {
        Mono.just('first')
            .doOnNext(() => results.push(`next`)) // некст срабатывает раньше чем doFirst, проблема в реализации doFirst, можешь поправить?
            .doFirst(() => results.push('before'))
            .subscribe(createSubscriber('doFirst'))
        expect(results).toEqual(['before', 'next', 'doFirst: first', 'doFirst: complete'])
    })

    test('Mono doOnNext should execute action on emission', () => {
        Mono.just('next')
            .doOnNext(value => results.push(`side: ${value}`))
            .subscribe(createSubscriber('doOnNext'))
        expect(results).toEqual(['side: next', 'doOnNext: next', 'doOnNext: complete'])
    })

    // TODO
    // test('Mono doFinally should execute after completion', () => {
    //     Mono.just('done').doFinally(() => results.push('finally')).subscribe(createSubscriber('doFinally'))
    //     expect(results).toEqual(['doFinally: done', 'doFinally: complete', 'finally'])
    // })

    // TODO
    // test('Mono doOnSubscribe should execute on subscription', () => {
    //     Mono.just('subscribed')
    //         .doOnSubscribe(() => results.push('subscribed'))
    //         .doOnNext(value => results.push('next'))
    //         .subscribe(createSubscriber('doOnSubscribe'))
    //     expect(results).toEqual(['next', 'subscribed', 'doOnSubscribe: subscribed', 'doOnSubscribe: complete'])
    // })

    test('Mono publishOn should change emission context', (done) => {
        Mono.just('async')
            .publishOn(Schedulers.macro())
            .subscribe(createSubscriber('publishOn'))
        expect(results).toEqual([])
        Schedulers.macro().schedule(() => {
            expect(results).toEqual(['publishOn: async', 'publishOn: complete'])
            done()
        })
    })
    test('Mono subscribeOn should change subscription context', (done) => {
        Mono.just('async-sub')
            .subscribeOn(Schedulers.micro())
            .subscribe(createSubscriber('subscribeOn'))
        expect(results).toEqual([])
        Schedulers.macro().schedule(() => {
            expect(results).toEqual(['subscribeOn: async-sub', 'subscribeOn: complete'])
            done()
        })
    })
})
