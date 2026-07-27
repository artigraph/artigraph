const playgrounds = new WeakMap();
const DEFAULT_TIMEOUT_MS = 90000;
const PREPARE_ROOT_MARGIN = "600px";
// esm.sh otherwise resolves transitive deps by semver range at request time; pinning them all keeps
// the two imports on one fixed copy of each shared package (e.g. @codemirror/state).
const CODEMIRROR_DEPS = [
  "@codemirror/autocomplete@6.20.4",
  "@codemirror/commands@6.11.1",
  "@codemirror/language@6.13.1",
  "@codemirror/lint@6.9.7",
  "@codemirror/search@6.7.2",
  "@codemirror/state@6.7.6",
  "@codemirror/streamparser@6.0.0",
  "@codemirror/view@6.43.15",
  "@lezer/common@1.5.3",
  "@lezer/highlight@1.2.5",
  "@lezer/lr@1.4.11",
  "@lezer/python@1.1.19",
  "@marijn/find-cluster-break@1.0.4",
  "crelt@1.0.7",
  "style-mod@4.1.4",
  "w3c-keyname@2.2.8",
].join(",");
const CODEMIRROR_URL = `https://esm.sh/codemirror@6.0.2?deps=${CODEMIRROR_DEPS}`;
const CODEMIRROR_PYTHON_URL = `https://esm.sh/@codemirror/lang-python@6.2.1?deps=${CODEMIRROR_DEPS}`;

let codeMirrorPromise = null;

function appendOutput(playground, text, kind = "stdout") {
  const output = playground.querySelector("[data-artigraph-output]");
  const prefix = kind === "stderr" ? "stderr: " : "";
  const suffix = text.endsWith("\n") ? "" : "\n";
  output.textContent += `${prefix}${text}${suffix}`;
  output.scrollTop = output.scrollHeight;
}

function setStatus(playground, text) {
  playground.querySelector("[data-artigraph-status]").textContent = text;
}

function stateFor(playground) {
  let state = playgrounds.get(playground);
  if (!state) {
    state = {
      initialized: false,
      worker: null,
      ready: false,
      preparing: false,
      running: false,
      timeout: null,
      prepareObserver: null,
      prepareScheduled: false,
      editorView: null,
      editorLoading: false,
    };
    playgrounds.set(playground, state);
  }
  return state;
}

function renderControls(playground) {
  const state = stateFor(playground);
  playground.querySelector("[data-artigraph-run]").disabled = state.running;
  playground.querySelector("[data-artigraph-stop]").disabled = !state.running;
  playground.classList.toggle("artigraph-playground--running", state.running);
  playground.classList.toggle("artigraph-playground--preparing", state.preparing && !state.running);
}

function startRunTimeout(playground) {
  const state = stateFor(playground);
  const timeoutMs = Number(playground.dataset.timeoutMs || DEFAULT_TIMEOUT_MS);
  state.timeout = setTimeout(() => {
    stopPlayground(playground, `Timed out after ${Math.round(timeoutMs / 1000)} seconds.`);
    appendOutput(playground, "Execution timed out and the worker was terminated.", "stderr");
  }, timeoutMs);
}

function clearRunTimeout(state) {
  if (state.timeout) {
    clearTimeout(state.timeout);
    state.timeout = null;
  }
}

function workerUrl() {
  return new URL("playground-worker.js", import.meta.url);
}

function manifestUrl(playground) {
  return playground.dataset.manifestUrl || new URL("../playground/manifest.json", import.meta.url).href;
}

function loadCodeMirror() {
  codeMirrorPromise ||= Promise.all([import(CODEMIRROR_URL), import(CODEMIRROR_PYTHON_URL)]).then(
    ([codeMirror, pythonLanguage]) => ({
      EditorView: codeMirror.EditorView,
      basicSetup: codeMirror.basicSetup,
      python: pythonLanguage.python,
    }),
  );
  return codeMirrorPromise;
}

function editorTheme(EditorView) {
  return EditorView.theme({
    "&": {
      height: "100%",
      color: "var(--md-code-fg-color)",
      backgroundColor: "var(--md-code-bg-color)",
    },
    "&.cm-focused": {
      outline: "2px solid var(--md-accent-fg-color)",
      outlineOffset: "-2px",
    },
    ".cm-scroller": {
      fontFamily: "var(--md-code-font-family)",
      fontSize: "0.72rem",
      lineHeight: "1.45",
    },
    ".cm-content": {
      minHeight: "100%",
      padding: "1rem 0",
      caretColor: "var(--md-code-fg-color)",
    },
    ".cm-line": {
      padding: "0 1rem",
    },
    ".cm-gutters": {
      color: "var(--md-default-fg-color--light)",
      backgroundColor: "var(--md-code-bg-color)",
      borderRight: "1px solid var(--md-default-fg-color--lightest)",
    },
    ".cm-activeLine, .cm-activeLineGutter": {
      backgroundColor: "color-mix(in srgb, var(--md-accent-fg-color) 9%, transparent)",
    },
    ".cm-selectionBackground, &.cm-focused .cm-selectionBackground": {
      backgroundColor: "color-mix(in srgb, var(--md-accent-fg-color) 28%, transparent)",
    },
  });
}

async function initializeEditor(playground) {
  const state = stateFor(playground);
  if (state.editorView || state.editorLoading) {
    return;
  }

  const textarea = playground.querySelector("[data-artigraph-code]");
  const editor = playground.querySelector("[data-artigraph-editor]");
  state.editorLoading = true;

  try {
    const { EditorView, basicSetup, python } = await loadCodeMirror();
    const view = new EditorView({
      doc: textarea.value,
      extensions: [
        basicSetup,
        python(),
        editorTheme(EditorView),
        EditorView.contentAttributes.of({ "aria-label": textarea.getAttribute("aria-label") }),
        EditorView.updateListener.of((update) => {
          if (update.docChanged) {
            textarea.value = update.state.doc.toString();
          }
        }),
      ],
      parent: editor,
    });
    state.editorView = view;
    editor.classList.add("artigraph-playground__editor--enhanced");
  } catch (error) {
    console.warn("CodeMirror failed to load; falling back to the plain textarea.", error);
  } finally {
    state.editorLoading = false;
  }
}

function codeFor(playground) {
  const state = stateFor(playground);
  if (state.editorView) {
    return state.editorView.state.doc.toString();
  }
  return playground.querySelector("[data-artigraph-code]").value;
}

function resetEditor(playground) {
  const state = stateFor(playground);
  const textarea = playground.querySelector("[data-artigraph-code]");
  const code = textarea.defaultValue;
  textarea.value = code;

  if (state.editorView) {
    state.editorView.dispatch({
      changes: {
        from: 0,
        to: state.editorView.state.doc.length,
        insert: code,
      },
    });
  }
}

function handleWorkerMessage(playground, worker, event) {
  const state = stateFor(playground);
  if (state.worker !== worker) {
    return;
  }

  const message = event.data || {};
  if (message.type === "status") {
    setStatus(playground, message.message);
  } else if (message.type === "ready") {
    state.ready = true;
    state.preparing = false;
    if (!state.running) {
      setStatus(playground, "Ready.");
    }
    renderControls(playground);
  } else if (message.type === "started") {
    // Time only user code, so a slow first Pyodide download doesn't count against the run.
    setStatus(playground, "Running your graph...");
    startRunTimeout(playground);
  } else if (message.type === "stdout" || message.type === "stderr") {
    appendOutput(playground, message.text, message.type);
  } else if (message.type === "done") {
    clearRunTimeout(state);
    state.running = false;
    state.ready = true;
    renderControls(playground);
    setStatus(playground, "Finished. Ready for another run.");
  } else if (message.type === "error") {
    if (message.phase === "prepare" && state.running) {
      return; // The pending run fails with the same error and reports it.
    }
    clearRunTimeout(state);
    state.running = false;
    state.preparing = false;
    renderControls(playground);
    setStatus(playground, message.phase === "prepare" && !state.ready ? "Preparation failed." : "Failed.");
    appendOutput(playground, message.message, "stderr");
    if (message.stack) {
      appendOutput(playground, message.stack, "stderr");
    }
  }
}

function handleWorkerError(playground, worker, event) {
  const state = stateFor(playground);
  if (state.worker !== worker) {
    return;
  }

  clearRunTimeout(state);
  worker.terminate();
  state.worker = null;
  state.ready = false;
  state.preparing = false;
  state.running = false;
  state.prepareScheduled = false;
  renderControls(playground);
  setStatus(playground, "Worker failed.");
  appendOutput(playground, event.message, "stderr");
}

function ensureWorker(playground) {
  const state = stateFor(playground);
  if (state.worker) {
    return state.worker;
  }

  const worker = new Worker(workerUrl(), { type: "module" });
  state.worker = worker;
  worker.addEventListener("message", (event) => handleWorkerMessage(playground, worker, event));
  worker.addEventListener("error", (event) => handleWorkerError(playground, worker, event));
  renderControls(playground);
  return worker;
}

function preparePlayground(playground) {
  const state = stateFor(playground);
  if (state.ready || state.preparing || state.running) {
    return;
  }

  const worker = ensureWorker(playground);
  state.preparing = true;
  renderControls(playground);
  setStatus(playground, "Preparing playground in background...");
  worker.postMessage({
    type: "prepare",
    manifestUrl: manifestUrl(playground),
  });
}

function runWhenIdle(callback) {
  if ("requestIdleCallback" in window) {
    window.requestIdleCallback(callback, { timeout: 2000 });
  } else {
    window.setTimeout(callback, 250);
  }
}

function schedulePrepare(playground) {
  const state = stateFor(playground);
  if (state.prepareScheduled) {
    return;
  }
  state.prepareScheduled = true;

  const start = () => runWhenIdle(() => preparePlayground(playground));
  if ("IntersectionObserver" in window) {
    state.prepareObserver = new IntersectionObserver(
      (entries) => {
        if (!entries.some((entry) => entry.isIntersecting)) {
          return;
        }
        state.prepareObserver?.disconnect();
        state.prepareObserver = null;
        start();
      },
      { rootMargin: PREPARE_ROOT_MARGIN },
    );
    state.prepareObserver.observe(playground);
  } else {
    start();
  }
}

function stopPlayground(playground, message = "Stopped.") {
  const state = stateFor(playground);
  if (state.worker) {
    state.worker.terminate();
  }
  state.prepareObserver?.disconnect();
  clearRunTimeout(state);
  state.worker = null;
  state.ready = false;
  state.preparing = false;
  state.running = false;
  state.prepareObserver = null;
  state.prepareScheduled = false;
  renderControls(playground);
  setStatus(playground, message);
}

function runPlayground(playground) {
  const state = stateFor(playground);
  if (state.running) {
    return;
  }

  const output = playground.querySelector("[data-artigraph-output]");
  const worker = ensureWorker(playground);
  output.textContent = "";
  state.running = true;
  state.preparing = false;
  renderControls(playground);
  setStatus(playground, state.ready ? "Running..." : "Finishing setup, then running...");
  worker.postMessage({
    type: "run",
    code: codeFor(playground),
    manifestUrl: manifestUrl(playground),
  });
}

function resetPlayground(playground) {
  const state = stateFor(playground);
  if (state.running) {
    stopPlayground(playground, "Stopped.");
  }

  resetEditor(playground);
  playground.querySelector("[data-artigraph-output]").textContent = "";

  if (state.ready) {
    setStatus(playground, "Ready.");
  } else if (state.preparing) {
    setStatus(playground, "Preparing playground in background...");
  } else {
    setStatus(playground, "Preparing when visible...");
    schedulePrepare(playground);
  }
  renderControls(playground);
}

function initializePlaygrounds(root = document) {
  for (const playground of root.querySelectorAll("[data-artigraph-playground]")) {
    const state = stateFor(playground);
    if (state.initialized) {
      continue;
    }
    state.initialized = true;

    initializeEditor(playground);
    playground.querySelector("[data-artigraph-run]").addEventListener("click", () => {
      runPlayground(playground);
    });
    playground.querySelector("[data-artigraph-stop]").addEventListener("click", () => {
      stopPlayground(playground);
    });
    playground.querySelector("[data-artigraph-reset]").addEventListener("click", () => {
      resetPlayground(playground);
    });
    setStatus(playground, "Preparing when visible...");
    renderControls(playground);
    schedulePrepare(playground);
  }
}

if (globalThis.document$) {
  globalThis.document$.subscribe(initializePlaygrounds);
} else {
  document.addEventListener("DOMContentLoaded", () => initializePlaygrounds());
}
