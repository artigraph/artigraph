#!/usr/bin/env node
// Checks that docs/assets/javascripts/playground.js releases each page's playgrounds when Zensical's
// instant navigation swaps the page content. Uses small DOM fakes, so it needs no browser or network.
import assert from "node:assert/strict";
import { test } from "node:test";

const workers = [];
const observers = [];
const idleCallbacks = [];
const pageSubscribers = [];

class FakeWorker {
  constructor() {
    this.messages = [];
    this.terminated = false;
    workers.push(this);
  }
  addEventListener() {}
  postMessage(message) {
    this.messages.push(message);
  }
  terminate() {
    this.terminated = true;
  }
}

class FakeIntersectionObserver {
  constructor(callback) {
    this.callback = callback;
    this.disconnected = false;
    observers.push(this);
  }
  observe(target) {
    this.target = target;
  }
  intersect() {
    this.callback([{ isIntersecting: true, target: this.target }]);
  }
  disconnect() {
    this.disconnected = true;
  }
}

function fakeElement() {
  return {
    disabled: false,
    textContent: "",
    value: "print(42)",
    defaultValue: "print(42)",
    addEventListener() {},
    getAttribute() {
      return null;
    },
    classList: { add() {}, toggle() {} },
  };
}

function fakePlayground() {
  const children = new Map();
  return {
    isConnected: true,
    dataset: { manifestUrl: "manifest.json" },
    classList: { toggle() {} },
    querySelector(selector) {
      if (!children.has(selector)) {
        children.set(selector, fakeElement());
      }
      return children.get(selector);
    },
  };
}

function navigateTo(...playgrounds) {
  const page = { querySelectorAll: () => playgrounds };
  for (const subscriber of pageSubscribers) {
    subscriber(page);
  }
}

function flushIdleCallbacks() {
  for (const callback of idleCallbacks.splice(0)) {
    callback();
  }
}

globalThis.Worker = FakeWorker;
globalThis.IntersectionObserver = FakeIntersectionObserver;
globalThis.window = globalThis;
globalThis.requestIdleCallback = (callback) => idleCallbacks.push(callback);
globalThis.document$ = { subscribe: (subscriber) => pageSubscribers.push(subscriber) };
// CodeMirror is imported from esm.sh, which Node refuses; the playground falls back to the textarea.
console.warn = () => {};
await import("../../docs/assets/javascripts/playground.js");

test("navigating away terminates the previous page's worker", () => {
  const first = fakePlayground();
  navigateTo(first);
  observers.at(-1).intersect();
  flushIdleCallbacks();
  assert.equal(workers.length, 1);

  first.isConnected = false;
  const second = fakePlayground();
  navigateTo(second);
  assert.ok(workers[0].terminated);
  observers.at(-1).intersect();
  flushIdleCallbacks();
  assert.equal(workers.length, 2);
  assert.ok(!workers[1].terminated);

  second.isConnected = false;
  navigateTo();
  assert.ok(workers[1].terminated);
});

test("navigating away disconnects a playground's pending prepare observer", () => {
  const playground = fakePlayground();
  navigateTo(playground);
  const observer = observers.at(-1);
  assert.ok(!observer.disconnected);

  playground.isConnected = false;
  navigateTo();
  assert.ok(observer.disconnected);
});

test("a playground removed before its idle prepare never starts a worker", () => {
  const before = workers.length;
  const playground = fakePlayground();
  navigateTo(playground);
  observers.at(-1).intersect();
  playground.isConnected = false;
  navigateTo();
  flushIdleCallbacks();

  assert.equal(workers.length, before);
});
