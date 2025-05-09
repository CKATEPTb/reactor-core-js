import {OneSink, Publisher, Schedulers, Sink, Sinks, Subscriber, Subscription} from '@/.';

describe('Sinks Behavior', () => {
    let results: string[];

    beforeEach(() => {
        results = [];
    });

    const createSubscriber = (label: string): Subscriber<string> => ({
        onNext: (value) => results.push(`${label}: ${value}`),
        onError: (error) => results.push(`${label}: error: ${error.message}`),
        onComplete: () => results.push(`${label}: complete`)
    });

    function createSinks(): Array<[Sink<string> & Publisher<string>, string]> {
        return [
            [Sinks.one(), 'one'],
            [Sinks.many().multicast(), 'many'],
            [Sinks.many().replay().all(), 'replay-all'],
            [Sinks.many().replay().latest(2), 'replay-latest'],
            [Sinks.many().replay().limit(2), 'replay-limit']
        ]
    }

    test("Sinks should emit only after request", (done) => {
        const sinks = createSinks()
        sinks.forEach(([sink, label]) => {
            const subscription = sink.subscribe(createSubscriber(label))
            Schedulers.micro().schedule(() => {
                subscription.request(1)
            })
        })
        sinks.forEach(([sink]) => {
            sink.next('emit')
        })
        Schedulers.macro().schedule(() => {
            expect(results).toEqual([
                'one: emit',
                'one: complete',
                'many: emit',
                'replay-all: emit',
                'replay-latest: emit',
                'replay-limit: emit'
            ])
            done()
        })
        expect(results).toEqual([])
    })

    test("Sinks should emit complete only if previous emit requested", () => {
        const sinks = createSinks()
        const subscriptions = sinks.map(([sink, label]) => sink.subscribe(createSubscriber(label)))

        function emit(complete = false) {
            sinks.forEach(([sink]) => {
                if (complete) sink.complete()
                else try {
                    sink.next('emit')
                } catch (ignored) {
                }
            })
        }

        for (let i = 0; i < 3; i++) {
            emit()
        }
        emit(true)
        subscriptions.forEach(value => value.request(2))
        expect(results).toEqual([
                'one: emit',
                'one: complete',
                'many: emit',
                'many: emit',
                'replay-all: emit',
                'replay-all: emit',
                'replay-latest: emit',
                'replay-latest: emit',
                'replay-limit: emit',
                'replay-limit: emit'
            ]
        )

        results = []
        subscriptions.forEach(value => value.request(1))
        expect(results).toEqual([
                'many: emit',
                'many: complete',
                'replay-all: emit',
                'replay-all: complete',
                'replay-latest: emit',
                'replay-latest: complete',
                'replay-limit: emit',
                'replay-limit: complete'
            ]
        )
    })

    test("Sinks shouldn't emit after complete", () => {
        const sinks = createSinks()
        sinks.forEach(([sink, label]) => {
            sink.complete()
            expect(() => sink.next(label))
                .toThrow("The completed sink is not accepting new emits.");
        })
    })

    test('OneSink should allow only one subscriber and complete after next', () => {
        const sink = Sinks.one<string>();
        sink.subscribe(createSubscriber('first')).request(1);
        expect(() => sink.subscribe(createSubscriber('second')))
            .toThrow("Only one subscriber is allowed for OneSink.");
        sink.next('emit');
        expect(results).toEqual(['first: emit', 'first: complete']);
    });

    test('ManySink should broadcast to multiple subscribers', () => {
        const sink = Sinks.many().multicast<string>();
        sink.subscribe(createSubscriber('first')).request(1);
        sink.subscribe(createSubscriber('second')).request(2);
        sink.next('emit');
        sink.next('emit');
        sink.complete()

        expect(results).toEqual(['first: emit', 'second: emit', 'second: emit', 'second: complete']);
    });

    test('Replay Sinks should replay emitted values to late subscribers only after request', () => {
        const sinks = createSinks().filter(([_, label]) => label.includes('replay'))
        const subscriptions: Array<[Sink<string> & Publisher<string>, Subscription]> =
            sinks.map(([sink, label]) => {
                sink.next('1');
                sink.next('2');
                sink.next('3');
                return [sink, sink.subscribe(createSubscriber(label))]
            })
        expect(results).toEqual([]);
        subscriptions.forEach(([sink, subscription]) => {
            subscription.request(4);
            sink.next('4');
            sink.next('5');
            sink.complete()
        })
        expect(results).toEqual([
            'replay-all: 1',
            'replay-all: 2',
            'replay-all: 3',
            'replay-all: 4',
            'replay-latest: 2',
            'replay-latest: 3',
            'replay-latest: 4',
            'replay-latest: 5',
            'replay-latest: complete',
            'replay-limit: 1',
            'replay-limit: 2',
            'replay-limit: 4',
            'replay-limit: 5',
            'replay-limit: complete'
        ]);
    });

    test('Sinks should control data flow with request count and should complete after request is exhausted', () => {
        const sinks: Array<[Sink<string> & Publisher<string>, Subscription]> = createSinks()
            .map(([sink, label]) => [sink, sink.subscribe(createSubscriber(label))])
        sinks.forEach(([sink, subscription]) => {
            subscription.request(1)
            sink.next('first')
            if (!(sink instanceof OneSink)) {
                sink.next('second')
            }
            sink.complete()
            subscription.request(2)
        })
        expect(results).toEqual([
                "one: first",
                "one: complete",
                "many: first",
                "many: second",
                "many: complete",
                "replay-all: first",
                "replay-all: second",
                "replay-all: complete",
                "replay-latest: first",
                "replay-latest: second",
                "replay-latest: complete",
                "replay-limit: first",
                "replay-limit: second",
                "replay-limit: complete",
            ]
        );
    });

    test('Sinks should handle errors correctly', () => {
        createSinks().forEach(([sink, label]) => {
            sink.subscribe(createSubscriber(label)).request(1)
            sink.error(new Error('Something went wrong'))
        });
        expect(results).toEqual([
                "one: error: Something went wrong",
                "one: complete",
                "many: error: Something went wrong",
                "replay-all: error: Something went wrong",
                "replay-latest: error: Something went wrong",
                "replay-limit: error: Something went wrong",
            ]
        );
    });

    test('Sinks should drop events if not requested', () => {
        createSinks().forEach(([sink, label]) => {
            const subscription = sink.subscribe(createSubscriber(label))
            sink.next('emit')
            subscription.request(1)
            if (!(sink instanceof OneSink)) {
                sink.next('emit')
            }
        })
        expect(results).toEqual([
            "one: emit",
            "one: complete",
            "many: emit",
            "replay-all: emit",
            "replay-latest: emit",
            "replay-limit: emit",
        ]);
    });

    test('Unsubscribed subscriber should not receive data', () => {
        const sinks = createSinks();
        sinks.forEach(([sink, label]) => {
            const subscription = sink.subscribe(createSubscriber(label));
            subscription.request(1);
            subscription.unsubscribe();
            sink.next('emit');
            sink.complete();
        });
        expect(results).toEqual([]);
    });

    test('One subscriber unsubscribes while others continue to receive data', () => {
        const sink = Sinks.many().multicast<string>();
        const sub1 = sink.subscribe(createSubscriber('sub1'));
        const sub2 = sink.subscribe(createSubscriber('sub2'));

        sub1.request(2);
        sub2.request(2);

        sink.next('first');
        sub1.unsubscribe();
        sink.next('second');
        sink.complete();

        expect(results).toEqual([
            'sub1: first',
            'sub2: first',
            'sub2: second',
            'sub2: complete'
        ]);
    });
});
