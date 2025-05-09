import {Scheduler, Schedulers} from "@/.";

describe('Schedulers Execution Order', () => {
    let results: string[];

    beforeEach(() => {
        results = [];
    });

    test('Immediate Scheduler should execute task immediately', () => {
        Schedulers.immediate().schedule(() => results.push('immediate'));
        results.push('after-immediate');

        expect(results).toEqual(['immediate', 'after-immediate']);
    });

    test('Micro Scheduler should execute task as a microtask', (done) => {
        Schedulers.micro().schedule(() => {
            results.push('micro')
            expect(results).toEqual(['after-micro', 'micro']);
            done()
        });
        results.push('after-micro');
    });

    test('Macro Scheduler should execute task as a macrotask', (done) => {
        Schedulers.macro().schedule(() => {
            results.push('macro');
            expect(results).toEqual(['after-macro', 'macro']);
            done();
        });
        results.push('after-macro');
    });

    test('Delay Scheduler should execute task after specified delay', (done) => {
        Schedulers.delay(5).schedule(() => {
            results.push('delayed');
            expect(results).toEqual(['after-delay', 'delayed']);
            done();
        });
        results.push('after-delay');
    });

    test('Mixed Schedulers should maintain correct order', (done) => {
        const schedulers: Array<[Scheduler, string]> = [
            [Schedulers.immediate(), 'immediate'],
            [Schedulers.micro(), 'micro'],
            [Schedulers.macro(), 'macro'],
            [Schedulers.delay(10), 'delay'],
            [Schedulers.immediate(), 'immediate']
        ]
        let countDown = schedulers.length
        schedulers.forEach(tuple => {
            tuple[0].schedule(() => {
                results.push(tuple[1])
                if(--countDown == 0) {
                    expect(results).toEqual(['immediate', 'immediate', 'after', 'micro', 'macro', 'delay']);
                    done()
                }
            })
        })
        results.push('after');
    });
});
