/**
 * @packageDocumentation
 * Browser source adapters used by Flux static factories.
 */
import {throwIfNullish} from "@/errors/helpers.js";
import {AsyncQueue} from "@/internal/async-queue.js";
import type {SourceFactory} from "@/publisher/types.js";

/** Options used when creating a Flux from a browser WebSocket. */
export interface WebSocketFluxOptions {
    /** Protocol or protocols passed to the browser WebSocket constructor when a URL is provided. */
    readonly protocols?: string | string[];
    /** Binary data format assigned to the socket before listening for messages. */
    readonly binaryType?: BinaryType;
    /** Closes the socket when the Flux subscription is cancelled. Defaults to true for URL-created sockets. */
    readonly closeOnCancel?: boolean;
}

/** Creates a lazy source factory from browser events. */
export function eventSource<TEvent extends Event>(
    target: EventTarget,
    type: string,
    options?: boolean | AddEventListenerOptions
): SourceFactory<TEvent> {
    throwIfNullish(target, "target");
    throwIfNullish(type, "type");
    return signal => {
        const queue = new AsyncQueue<TEvent>(signal);
        if (signal.aborted) {
            return queue;
        }
        const listener = (event: Event) => {
            queue.push(event as TEvent);
        };
        const cleanup = () => {
            target.removeEventListener(type, listener, options);
        };
        target.addEventListener(type, listener, options);
        signal.addEventListener("abort", cleanup, {once: true});
        return queue;
    };
}

/** Creates a lazy source factory from browser WebSocket message events. */
export function webSocketSource<T>(
    input: WebSocket | string | URL,
    options?: string | string[] | WebSocketFluxOptions
): SourceFactory<MessageEvent<T>> {
    throwIfNullish(input, "input");
    return signal => {
        const queue = new AsyncQueue<MessageEvent<T>>(signal);
        if (signal.aborted) {
            return queue;
        }
        const {closeOnCancel, socket} = webSocketFrom(input, options);
        let cleaned = false;
        const onMessage = (event: MessageEvent) => {
            queue.push(event as MessageEvent<T>);
        };
        const onError = (event: Event) => {
            cleanup(false);
            queue.error(event);
        };
        const onClose = () => {
            cleanup(false);
            queue.complete();
        };
        const onAbort = () => {
            cleanup(true);
        };
        function cleanup(cancelled: boolean): void {
            if (cleaned) {
                return;
            }
            cleaned = true;
            socket.removeEventListener("message", onMessage);
            socket.removeEventListener("error", onError);
            socket.removeEventListener("close", onClose);
            signal.removeEventListener("abort", onAbort);
            if (cancelled) {
                closeWebSocket(socket, closeOnCancel);
            }
        }
        socket.addEventListener("message", onMessage);
        socket.addEventListener("error", onError);
        socket.addEventListener("close", onClose);
        signal.addEventListener("abort", onAbort, {once: true});
        return queue;
    };
}

/** Normalizes browser WebSocket options while keeping the common protocols-only shorthand. */
function webSocketOptions(options?: string | string[] | WebSocketFluxOptions): WebSocketFluxOptions {
    return typeof options === "string" || Array.isArray(options) ? {protocols: options} : options ?? {};
}

/** Creates or reuses a browser WebSocket and returns its cancellation ownership policy. */
function webSocketFrom(
    input: WebSocket | string | URL,
    options?: string | string[] | WebSocketFluxOptions
): { readonly closeOnCancel: boolean; readonly socket: WebSocket } {
    const normalized = webSocketOptions(options);
    const ownsSocket = typeof input === "string" || input instanceof URL;
    const socket = ownsSocket ? openWebSocket(input, normalized.protocols) : input;
    if (normalized.binaryType !== undefined) {
        socket.binaryType = normalized.binaryType;
    }
    return {
        closeOnCancel: normalized.closeOnCancel ?? ownsSocket,
        socket
    };
}

/** Opens a browser WebSocket or fails clearly when the runtime does not provide one. */
function openWebSocket(url: string | URL, protocols?: string | string[]): WebSocket {
    const WebSocketConstructor = globalThis.WebSocket;
    if (typeof WebSocketConstructor !== "function") {
        throw new TypeError("WebSocket is not available in this runtime");
    }
    return protocols === undefined ? new WebSocketConstructor(url) : new WebSocketConstructor(url, protocols);
}

/** Closes a WebSocket when this Flux owns the socket or the caller requested close-on-cancel. */
function closeWebSocket(socket: WebSocket, closeOnCancel: boolean): void {
    if (closeOnCancel && socket.readyState < 2) {
        socket.close();
    }
}
