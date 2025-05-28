The **reactor-core-ts** library is a TypeScript-based implementation of
reactive streams, inspired by **reactor-core**. It provides powerful tools
for building reactive systems with backpressure support, designed for
high-performance data processing and efficient flow control.

This library supports a wide range of reactive patterns, including:

- **Mono:** A single-value or empty result.
- **Flux:** A multi-value, stream-like result.
- **Sinks:** Different types of data sinks with backpressure management.
- **Schedulers:** Various schedulers for asynchronous task execution.

## Installation

To install the package using **npm**:

```bash
npm install reactor-core-ts
```

To install the package using **pnpm**:

```bash
pnpm install reactor-core-ts
```

To install the package using **yarn**:

```bash
yarn install reactor-core-ts
```

## Features

* **Reactive Streams with Backpressure**: Seamless management of data flow and pressure.
* **Flexible Data Sinks**: Supports single, multi, and replay sinks.
* **Customizable Schedulers**: Immediate, Micro, Macro, and Delayed execution.
* **Reactive Extensions**: Map, filter, merge, concat, reduce, and more.
* **TypeScript Support**: Fully typed for safer and more maintainable code.

## Usage

### Basic Example: Using Mono and Flux

```typescript
import {Mono, Flux, Sinks, Schedulers} from "@ckateptb/reactor-core-js";

// Creating a Mono
const mono = Mono.just(42);

// Creating a Flux
const flux = Flux.range(1, 5);

// Using Schedulers for task execution
const immediateScheduler = Schedulers.immediate();
const microScheduler = Schedulers.micro();
const macroScheduler = Schedulers.macro();
const delayScheduler = Schedulers.delay(1000); // 1-second delay

// Schedule a simple task immediately
immediateScheduler.schedule(() => console.log("Immediate task executed"));

// Schedule a microtask
microScheduler.schedule(() => console.log("Microtask executed"));

// Schedule a macrotask
macroScheduler.schedule(() => console.log("Macrotask executed"));

// Schedule a delayed task and cancel it before execution
const delayedTask = delayScheduler.schedule(() => console.log("Delayed task executed"));
delayedTask.cancel(); // Cancels the task before it runs

// Pipe a Publisher
flux
    .doFinally(() => console.log('finally'))       // Executes after the pipeline completes
    .map(value => value * 2)                      // Transforms each value by multiplying by 2
    .filter(value => value % 2 === 0)              // Filters even numbers
    .concatWith(Flux.range(5, 5))                  // Concatenates with another range
    .mergeWith(Flux.range(10, 5))                  // Merges with another range
    .delayElements(5000)                           // Delays each emitted element by 5 seconds
    .doFirst(() => console.log('first'))            // Executes before the first emission
    // ... for more pipes, see the JSDoc
    // Subscribe to the publisher
    .subscribe({
        onNext: (value) => {
            console.log(value);
        },
        onError: (error) => {
            console.error(error);
        },
        onComplete: () => {
            console.log('complete');
        }
    })
    // Do not forget to request
    .request(Number.MAX_SAFE_INTEGER);

// Sinks
const sink = Sinks.many().multicast<number>();
let count = 0;

setInterval(() => {
    sink.next(count++); // Emit incrementing values every interval
}, 1000);

Flux.from(sink)
    .subscribe({
        onNext: (value) => console.log("Sink value:", value),
        onError: (error) => console.error("Sink error:", error),
        onComplete: () => console.log("Sink complete")
    })
    .request(5); // Request 5 values from the sink
```

## Contributing

### 1. Clone the repository:

```bash
git clone https://github.com/CKATEPTb/reactor-core-ts.git
```

### 2. Install dependencies::

```bash
pnpm install
```

### 3. Run the build::

```bash
pnpm run build
```

## License

### This project is licensed under the LGPL-3.0-only License.

See the LICENSE.md file for details.

## Author

### CKATEPTb

Feel free to open issues and submit pull requests to improve the library!