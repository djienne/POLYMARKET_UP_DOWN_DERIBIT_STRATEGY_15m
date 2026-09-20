import { useEffect, useRef, useState } from "react";
import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import { api } from "./lib/api";
import { useDash } from "./lib/store";
import { createWs, WsManager } from "./lib/ws";
import type { BootstrapPayload, ViewSource } from "./lib/types";

import TopStrip from "./components/TopStrip";
import ProbabilityPanel from "./components/ProbabilityPanel";
import EdgeRatioPanel from "./components/EdgeRatioPanel";
import MarketInfoCard from "./components/MarketInfoCard";
import WindowTimer from "./components/WindowTimer";
import PositionCard from "./components/PositionCard";
import InstanceStatsCard from "./components/InstanceStatsCard";
import EquitySparkline from "./components/EquitySparkline";
import PriceChart from "./components/PriceChart";
import TradeFeed from "./components/TradeFeed";
import Footer from "./components/Footer";
import AnimationCoordinator from "./components/anim/AnimationCoordinator";

type SourceTransition = {
  from: ViewSource;
  to: ViewSource;
  seq: number;
};

type PendingViewRequest = {
  key: string;
  source: ViewSource | null;
  instanceId: number | null;
};

const ALTERNATE_VIEW_INTERVAL_MS = 120_000;
const HTTP_BOOTSTRAP_COOLDOWN_MS = 2_500;
const SOURCE_TRANSITION_VISIBLE_MS = 2_200;
const SOURCE_TRANSITION_REDUCED_VISIBLE_MS = 450;
const SOURCE_TRANSITION_MAX_VISIBLE_MS = 3_500;
const SOURCE_TRANSITION_REDUCED_MAX_VISIBLE_MS = 1_000;

function viewRequestKey(source: ViewSource | null, instanceId?: number | null) {
  return `${source ?? "default"}:${instanceId ?? "auto"}`;
}

export default function App() {
  const wsRef = useRef<WsManager | null>(null);
  const bootstrapRequestRef = useRef(0);
  const desiredViewRequestRef = useRef<PendingViewRequest | null>(null);
  const pendingViewRequestRef = useRef<PendingViewRequest | null>(null);
  const viewRequestInFlightKeyRef = useRef<string | null>(null);
  const appliedViewKeyRef = useRef<string | null>(null);
  const httpBootstrapInFlightKeyRef = useRef<string | null>(null);
  const httpBootstrapLastStartedRef = useRef<{ key: string; at: number } | null>(null);
  const boundaryRefreshInFlightRef = useRef(false);
  const livePositionPinnedRef = useRef(false);
  const nextBoundaryRefreshAtRef = useRef(0);
  const lastBootstrapWindowStartRef = useRef<string | null>(null);
  const didHandleInitialSelection = useRef(false);
  const previousModeRef = useRef<ViewSource | null>(null);
  const sourceTransitionReadyRef = useRef(false);
  const transitionTimeoutRef = useRef<number | null>(null);
  const sourceTransitionFirstShownAtRef = useRef<number | null>(null);
  const [sourceTransition, setSourceTransition] = useState<SourceTransition | null>(null);
  const reduceMotion = useReducedMotion();
  const setAllInstances = useDash((s) => s.setAllInstances);
  const setSelected = useDash((s) => s.setSelected);
  const setViewSource = useDash((s) => s.setViewSource);
  const setAlternateLastSwitchAt = useDash((s) => s.setAlternateLastSwitchAt);
  const applyBootstrap = useDash((s) => s.applyBootstrap);
  const setWsStatus = useDash((s) => s.setWsStatus);
  const applyEnvelope = useDash((s) => s.applyEnvelope);
  const tickWindow = useDash((s) => s.tickWindow);
  const selected = useDash((s) => s.selectedInstanceId);
  const windowStartIso = useDash((s) => s.windowStartIso);
  const windowEndIso = useDash((s) => s.windowEndIso);
  const wsStatus = useDash((s) => s.wsStatus);
  const mode = useDash((s) => s.mode);
  const viewSource = useDash((s) => s.viewSource);
  const alternateModeEnabled = useDash((s) => s.alternateModeEnabled);
  const alternateLastSwitchAt = useDash((s) => s.alternateLastSwitchAt);
  const botLive = useDash((s) => s.liveness?.bot_live ?? false);
  const livePositionOpen = useDash((s) => s.liveness?.live_position_open ?? false);
  const hasDashboardPayload = useDash((s) => s.terminal != null || s.instance != null);

  const bootstrapMatchesRequest = (
    payload: BootstrapPayload,
    request: PendingViewRequest | null,
  ) => {
    if (!request) return true;
    if (request.source != null && payload.mode !== request.source) return false;
    const payloadInstanceId = payload.instance?.instance_id ?? null;
    if (request.instanceId != null && payloadInstanceId !== request.instanceId) {
      return false;
    }
    return true;
  };

  const bootstrapMovesWindowBackward = (payload: BootstrapPayload) => {
    const current = useDash.getState().windowStartIso;
    const next = payload.window_start_iso ?? null;
    if (!current || !next) return false;
    const currentTs = Date.parse(current);
    const nextTs = Date.parse(next);
    return Number.isFinite(currentTs) && Number.isFinite(nextTs) && nextTs < currentTs;
  };

  const applyBootstrapIfCurrent = (
    payload: BootstrapPayload,
    options: {
      syncSelectedFromPayload?: boolean;
      request?: PendingViewRequest | null;
      allowBackwardWindow?: boolean;
    } = {},
  ) => {
    const request = options.request ?? null;
    const pending = pendingViewRequestRef.current;
    if (!bootstrapMatchesRequest(payload, request)) return false;
    if (!request && pending && !bootstrapMatchesRequest(payload, pending)) return false;
    if (!options.allowBackwardWindow && bootstrapMovesWindowBackward(payload)) return false;

    const matchedPending =
      pending && bootstrapMatchesRequest(payload, pending) ? pending : null;
    const syncSelectedFromPayload =
      options.syncSelectedFromPayload ||
      request?.instanceId === null ||
      matchedPending?.instanceId === null;
    const currentSelected = useDash.getState().selectedInstanceId;
    const payloadInstanceId = payload.instance?.instance_id ?? null;
    if (
      syncSelectedFromPayload &&
      payloadInstanceId != null &&
      payloadInstanceId !== currentSelected
    ) {
      setSelected(payloadInstanceId);
    } else if (
      payload.mode === "live" &&
      payloadInstanceId != null &&
      payloadInstanceId !== currentSelected
    ) {
      setSelected(payloadInstanceId);
    } else if (
      payloadInstanceId != null &&
      payloadInstanceId !== currentSelected
    ) {
      return false;
    }
    lastBootstrapWindowStartRef.current = payload.window_start_iso ?? null;
    applyBootstrap(payload);
    const matchedDesired =
      desiredViewRequestRef.current &&
      bootstrapMatchesRequest(payload, desiredViewRequestRef.current)
        ? desiredViewRequestRef.current
        : null;
    if (matchedDesired) {
      appliedViewKeyRef.current = matchedDesired.key;
      desiredViewRequestRef.current = null;
    } else {
      appliedViewKeyRef.current = viewRequestKey(payload.mode, payloadInstanceId);
    }
    if (matchedPending) {
      pendingViewRequestRef.current = null;
    }
    if (
      (matchedPending && viewRequestInFlightKeyRef.current === matchedPending.key) ||
      (matchedDesired && viewRequestInFlightKeyRef.current === matchedDesired.key)
    ) {
      viewRequestInFlightKeyRef.current = null;
    }
    return true;
  };

  const refreshBootstrap = (
    instanceId?: number,
    syncSelectedFromPayload = false,
    source?: ViewSource | null,
    viewRequest?: PendingViewRequest | null,
  ) => {
    const httpKey = viewRequest?.key ?? viewRequestKey(source ?? null, instanceId ?? null);
    if (httpBootstrapInFlightKeyRef.current === httpKey) {
      return Promise.resolve();
    }
    const now = Date.now();
    const lastStarted = httpBootstrapLastStartedRef.current;
    if (
      lastStarted?.key === httpKey &&
      now - lastStarted.at < HTTP_BOOTSTRAP_COOLDOWN_MS
    ) {
      return Promise.resolve();
    }
    httpBootstrapInFlightKeyRef.current = httpKey;
    httpBootstrapLastStartedRef.current = { key: httpKey, at: now };
    const requestId = ++bootstrapRequestRef.current;
    return api.bootstrap(instanceId, source)
      .then((payload) => {
        if (requestId !== bootstrapRequestRef.current) return;
        applyBootstrapIfCurrent(payload, {
          syncSelectedFromPayload,
          request: viewRequest ?? null,
          allowBackwardWindow: viewRequest != null,
        });
      })
      .catch(console.error)
      .finally(() => {
        if (httpBootstrapInFlightKeyRef.current === httpKey) {
          httpBootstrapInFlightKeyRef.current = null;
        }
        if (viewRequestInFlightKeyRef.current === httpKey) {
          viewRequestInFlightKeyRef.current = null;
        }
      });
  };

  const requestView = (source: ViewSource | null, fallbackInstanceId?: number) => {
    const useMirrorInstance = alternateModeEnabled && source != null;
    const requestedInstanceId = useMirrorInstance ? null : (fallbackInstanceId ?? null);
    const requestKey = viewRequestKey(source, requestedInstanceId);
    if (desiredViewRequestRef.current?.key === requestKey) return;
    if (pendingViewRequestRef.current?.key === requestKey) return;
    if (viewRequestInFlightKeyRef.current === requestKey) return;
    if (appliedViewKeyRef.current === requestKey) return;
    const request = {
      key: requestKey,
      source,
      instanceId: requestedInstanceId,
    };
    desiredViewRequestRef.current = request;
    pendingViewRequestRef.current = request;
    const wsStatusNow = useDash.getState().wsStatus;
    if (wsRef.current && wsStatusNow !== "closed") {
      viewRequestInFlightKeyRef.current = requestKey;
      wsRef.current.selectView(useMirrorInstance ? null : fallbackInstanceId, source);
      return;
    }
    viewRequestInFlightKeyRef.current = requestKey;
    refreshBootstrap(
      useMirrorInstance ? undefined : fallbackInstanceId,
      useMirrorInstance,
      source,
      request,
    );
  };

  useEffect(() => {
    if (!hasDashboardPayload) {
      previousModeRef.current = mode;
      return;
    }

    if (!sourceTransitionReadyRef.current) {
      previousModeRef.current = mode;
      sourceTransitionReadyRef.current = true;
      return;
    }

    const previous = previousModeRef.current;
    if (previous == null || previous === mode) {
      previousModeRef.current = mode;
      return;
    }

    previousModeRef.current = mode;
    if (transitionTimeoutRef.current != null) window.clearTimeout(transitionTimeoutRef.current);
    const now = Date.now();
    const firstShownAt = sourceTransitionFirstShownAtRef.current ?? now;
    sourceTransitionFirstShownAtRef.current = firstShownAt;
    setSourceTransition((current) => ({
      from: previous,
      to: mode,
      seq: (current?.seq ?? 0) + 1,
    }));
    const maxVisibleMs = reduceMotion
      ? SOURCE_TRANSITION_REDUCED_MAX_VISIBLE_MS
      : SOURCE_TRANSITION_MAX_VISIBLE_MS;
    const normalVisibleMs = reduceMotion
      ? SOURCE_TRANSITION_REDUCED_VISIBLE_MS
      : SOURCE_TRANSITION_VISIBLE_MS;
    const elapsedVisibleMs = now - firstShownAt;
    const timeoutMs = Math.max(0, Math.min(normalVisibleMs, maxVisibleMs - elapsedVisibleMs));
    transitionTimeoutRef.current = window.setTimeout(
      () => {
        sourceTransitionFirstShownAtRef.current = null;
        setSourceTransition(null);
        transitionTimeoutRef.current = null;
      },
      timeoutMs,
    );
  }, [hasDashboardPayload, mode, reduceMotion]);

  useEffect(() => {
    return () => {
      if (transitionTimeoutRef.current != null) {
        window.clearTimeout(transitionTimeoutRef.current);
      }
      sourceTransitionFirstShownAtRef.current = null;
    };
  }, []);

  useEffect(() => {
    // Allow overriding the selected instance via ?instance_id=N for dev / debugging
    // without surfacing the multi-instance reality in the UI.
    const urlId = new URLSearchParams(window.location.search).get("instance_id");
    const hasUrlId = urlId != null && !Number.isNaN(Number(urlId));
    const initialId = hasUrlId ? Number(urlId) : undefined;
    if (initialId != null && initialId !== selected) setSelected(initialId);
    // Without an explicit URL override, ask the backend for its configured
    // default instance. Live dashboards use this to follow the single-trader
    // synthetic live bucket (9999) instead of the store's historical 100 seed.
    const initialAlternate = useDash.getState().alternateModeEnabled;
    const initialRequest = initialAlternate
      ? {
          key: viewRequestKey("dry_run", null),
          source: "dry_run" as ViewSource,
          instanceId: null,
        }
      : null;
    if (initialRequest) {
      desiredViewRequestRef.current = initialRequest;
      pendingViewRequestRef.current = initialRequest;
    }
    refreshBootstrap(
      initialAlternate ? undefined : initialId,
      initialAlternate || !hasUrlId,
      initialAlternate ? "dry_run" : null,
      initialRequest,
    );
    api.instances().then(setAllInstances).catch(console.error);

    const ws = createWs();
    wsRef.current = ws;
    ws.subscribeStatus(setWsStatus);
    ws.subscribe((env) => {
      if (env.type === "bootstrap") {
        applyBootstrapIfCurrent(env.data);
        return;
      }
      applyEnvelope(env);
    });
    ws.connect();
    return () => ws.close();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!didHandleInitialSelection.current) {
      didHandleInitialSelection.current = true;
      return;
    }
    if (!hasDashboardPayload) return;
    requestView(alternateModeEnabled ? (viewSource ?? "dry_run") : null, selected);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selected, hasDashboardPayload]);

  useEffect(() => {
    if (!hasDashboardPayload) return;

    if (!alternateModeEnabled) {
      livePositionPinnedRef.current = false;
      setViewSource(null);
      setAlternateLastSwitchAt(null);
      requestView(null, selected);
      return;
    }

    if (livePositionOpen) {
      livePositionPinnedRef.current = true;
      if (viewSource !== "live") {
        setViewSource("live");
        setAlternateLastSwitchAt(Date.now());
        requestView("live");
      } else if (alternateLastSwitchAt == null) {
        setAlternateLastSwitchAt(Date.now());
      }
      return;
    }

    if (livePositionPinnedRef.current) {
      livePositionPinnedRef.current = false;
      setAlternateLastSwitchAt(Date.now());
    }

    const current = viewSource ?? "dry_run";
    if (!botLive && current !== "dry_run") {
      setViewSource("dry_run");
      setAlternateLastSwitchAt(Date.now());
      requestView("dry_run");
      return;
    }

    if (alternateLastSwitchAt == null) {
      setAlternateLastSwitchAt(Date.now());
      if (current !== "dry_run") {
        setViewSource("dry_run");
      }
      requestView("dry_run");
      return;
    }
    if (viewSource == null) {
      setViewSource("dry_run");
      requestView("dry_run");
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hasDashboardPayload, alternateModeEnabled, alternateLastSwitchAt, botLive, livePositionOpen, selected, wsStatus]);

  useEffect(() => {
    if (!alternateModeEnabled || !botLive) return;
    const id = window.setInterval(() => {
      const st = useDash.getState();
      if (!st.alternateModeEnabled || !st.liveness?.bot_live) return;
      if (st.liveness?.live_position_open) return;
      const now = Date.now();
      const last = st.alternateLastSwitchAt;
      if (last == null) {
        st.setAlternateLastSwitchAt(now);
        return;
      }
      if (now - last < ALTERNATE_VIEW_INTERVAL_MS) return;
      const next: ViewSource = st.viewSource === "live" ? "dry_run" : "live";
      st.setViewSource(next);
      st.setAlternateLastSwitchAt(now);
      requestView(next);
    }, 1000);
    return () => window.clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [alternateModeEnabled, botLive]);

  useEffect(() => {
    if (wsStatus === "closed") {
      viewRequestInFlightKeyRef.current = null;
      pendingViewRequestRef.current = null;
      const desired = desiredViewRequestRef.current;
      if (desired) {
        viewRequestInFlightKeyRef.current = desired.key;
        refreshBootstrap(
          desired.instanceId ?? undefined,
          desired.source != null && desired.instanceId === null,
          desired.source,
          desired,
        );
      }
      return;
    }
    if (wsStatus === "open") {
      const desired = desiredViewRequestRef.current;
      if (desired && viewRequestInFlightKeyRef.current !== desired.key) {
        viewRequestInFlightKeyRef.current = desired.key;
        wsRef.current?.selectView(desired.instanceId, desired.source);
      }
    }
  }, [wsStatus]);

  useEffect(() => {
    const resetAlternateTimer = () => {
      if (document.visibilityState !== "visible") return;
      const st = useDash.getState();
      pendingViewRequestRef.current = null;
      viewRequestInFlightKeyRef.current = null;
      if (sourceTransitionFirstShownAtRef.current != null) {
        if (transitionTimeoutRef.current != null) window.clearTimeout(transitionTimeoutRef.current);
        transitionTimeoutRef.current = null;
        sourceTransitionFirstShownAtRef.current = null;
        setSourceTransition(null);
      }
      if (st.alternateModeEnabled) {
        st.setAlternateLastSwitchAt(Date.now());
      }
    };
    document.addEventListener("visibilitychange", resetAlternateTimer);
    return () => document.removeEventListener("visibilitychange", resetAlternateTimer);
  }, []);

  useEffect(() => {
    if (!hasDashboardPayload || !alternateModeEnabled || viewSource == null) return;
    requestView(viewSource);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hasDashboardPayload, viewSource]);

  // Removed: previously sent selectInstance(old_selected) on reconnect, which
  // raced with the backend's initial bootstrap and caused flickering.
  // The backend sends its recommended bootstrap on every new WS connection;
  // the [selected] effect below re-sends selectInstance if selectedInstanceId
  // changes, which is sufficient.

  // Re-fetch bootstrap whenever the 15-min market window changes. The WS stream
  // publishes model.update only when a new log line is parsed, which can leave
  // the chart stuck at the 0.5 seed for up to a minute after window rollover
  // (or indefinitely if WS events are dropped). Bootstrap re-scopes all series
  // to the new window and unsticks the chart within one poll cycle.
  useEffect(() => {
    if (!windowStartIso) return;
    if (windowStartIso === lastBootstrapWindowStartRef.current) return;
    refreshBootstrap(
      alternateModeEnabled ? undefined : selected,
      alternateModeEnabled,
      alternateModeEnabled ? viewSource : null,
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [windowStartIso]);

  useEffect(() => {
    if (!windowStartIso && !windowEndIso) return;
    tickWindow();

    const id = window.setInterval(() => {
      tickWindow();

      const endTs = windowEndIso != null ? Date.parse(windowEndIso) : NaN;
      if (
        !Number.isFinite(endTs) ||
        Date.now() < endTs ||
        Date.now() < nextBoundaryRefreshAtRef.current ||
        boundaryRefreshInFlightRef.current
      ) {
        return;
      }

      boundaryRefreshInFlightRef.current = true;
      const st = useDash.getState();
      refreshBootstrap(
        st.alternateModeEnabled ? undefined : st.selectedInstanceId,
        st.alternateModeEnabled,
        st.alternateModeEnabled ? st.viewSource : null,
      ).finally(() => {
        boundaryRefreshInFlightRef.current = false;
        nextBoundaryRefreshAtRef.current = Date.now() + 10_000;
      });
    }, 1000);

    return () => window.clearInterval(id);
  }, [windowStartIso, windowEndIso, tickWindow]);

  const transitionTarget = sourceTransition?.to ?? mode;
  const transitionLabel =
    transitionTarget === "live" ? "REAL MONEY MODE" : "DRY RUN SIMULATION";
  const transitionFromLabel =
    sourceTransition?.from === "live" ? "REAL MONEY MODE" : "DRY RUN SIMULATION";
  const transitionPanelClass =
    transitionTarget === "live"
      ? "border-rose-300/70 bg-rose-950/80 text-rose-50 shadow-[0_0_42px_rgba(248,113,113,0.26)]"
      : "border-cyan-300/70 bg-cyan-950/80 text-cyan-50 shadow-[0_0_42px_rgba(34,211,238,0.24)]";
  const transitionWashClass =
    transitionTarget === "live"
      ? "from-rose-950/0 via-rose-500/20 to-rose-950/0"
      : "from-cyan-950/0 via-cyan-500/20 to-cyan-950/0";
  const transitionLineClass =
    transitionTarget === "live" ? "bg-rose-200/70" : "bg-cyan-200/70";
  const mainTransitionActive = sourceTransition != null;
  const mainAnimate = mainTransitionActive
    ? reduceMotion
      ? { opacity: 0.96 }
      : { opacity: 0.88 }
    : reduceMotion
      ? { opacity: 1 }
      : { opacity: 1 };

  return (
    <div className="min-h-screen w-full bg-ink-950 flex justify-center">
      {/* Lock to 16:9 for 1080p streaming; body scrolls on viewports shorter than the frame */}
      <div className="stream-frame flex flex-col">
        <TopStrip />

        <AnimatePresence mode="wait">
          {sourceTransition && (
            <motion.div
              key={sourceTransition.seq}
              role="status"
              aria-live="polite"
              aria-label={`Now showing ${transitionLabel} after ${transitionFromLabel}`}
              className="pointer-events-none absolute inset-x-4 bottom-[42px] top-[86px] z-10 overflow-hidden rounded-lg border border-white/5 bg-ink-950/18 backdrop-blur-[2px]"
              initial={
                reduceMotion
                  ? { opacity: 0 }
                  : { opacity: 0 }
              }
              animate={
                reduceMotion
                  ? { opacity: 1 }
                  : { opacity: 1 }
              }
              exit={
                reduceMotion
                  ? { opacity: 0 }
                  : { opacity: 0 }
              }
              transition={{
                duration: reduceMotion ? 0.18 : 0.55,
                ease: [0.22, 1, 0.36, 1],
              }}
            >
              {!reduceMotion && (
                <motion.div
                  className={`absolute inset-y-0 w-[42%] bg-gradient-to-r ${transitionWashClass}`}
                  initial={{ x: "-120%" }}
                  animate={{ x: "260%" }}
                  transition={{
                    duration: 1.7,
                    ease: [0.22, 1, 0.36, 1],
                  }}
                />
              )}
              <motion.div
                className={`absolute left-0 right-0 top-0 h-px ${transitionLineClass}`}
                initial={{ scaleX: 0, opacity: 0 }}
                animate={{ scaleX: 1, opacity: 1 }}
                exit={{ scaleX: 0, opacity: 0 }}
                transition={{
                  duration: reduceMotion ? 0.18 : 0.9,
                  ease: [0.22, 1, 0.36, 1],
                }}
                style={{ transformOrigin: "center" }}
              />
              <div
                className="absolute inset-0 flex items-center justify-center"
              >
                <div
                  className={`rounded-lg border px-6 py-3 text-center backdrop-blur-xl ${transitionPanelClass}`}
                >
                  <div className="text-[10px] uppercase tracking-[0.24em] opacity-75">
                    Now showing
                  </div>
                  <div className="mt-1 text-base font-black uppercase tracking-[0.14em]">
                    {transitionLabel}
                  </div>
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        <motion.main
          className="flex-1 min-h-0 px-4 py-3 grid grid-cols-[minmax(0,1fr)_320px] gap-3"
          animate={mainAnimate}
          transition={{
            duration: reduceMotion ? 0.18 : 0.85,
            ease: [0.22, 1, 0.36, 1],
          }}
        >
          <div className="min-h-0 grid grid-rows-[minmax(0,22fr)_minmax(0,22fr)_minmax(0,15fr)_minmax(0,27fr)] gap-3">
            {/* Row 1: probabilities + edge */}
            <section className="grid grid-cols-[minmax(0,2.35fr)_minmax(320px,0.9fr)] gap-3 min-h-0">
              <div className="min-h-0">
                <ProbabilityPanel />
              </div>
              <div className="min-h-0 self-start">
                <EdgeRatioPanel />
              </div>
            </section>

            {/* Row 2: live UP/DOWN price chart (full width) */}
            <section className="min-h-0">
              <PriceChart />
            </section>

            {/* Row 3: market / window / position */}
            <section className="grid grid-cols-3 gap-3 min-h-0">
              <MarketInfoCard />
              <WindowTimer />
              <PositionCard />
            </section>

            {/* Row 4: instance stats + equity */}
            <section className="grid grid-cols-3 gap-3 min-h-0">
              <div className="col-span-2 min-h-0">
                <InstanceStatsCard />
              </div>
              <div className="min-h-0">
                <EquitySparkline />
              </div>
            </section>
          </div>

          {/* Right rail */}
          <div className="min-h-0">
            <TradeFeed />
          </div>
        </motion.main>

        <Footer />
        <AnimationCoordinator />
      </div>
    </div>
  );
}
