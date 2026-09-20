import { AnimatePresence, motion } from "framer-motion";
import { useEffect, useRef, useState } from "react";
import { useDash } from "../../lib/store";
import { fmtMoney } from "../../lib/format";

const GIF_LOOPS = 2;
const GIF_TAIL_MS = 300; // breathing room so the last loop doesn't get cut

// Winning-trade pool — random pick per win event, deterministic by flash id.
const WIN_GIFS = [
  { src: "/dicaprio.gif",  loopMs: 4200 }, // DiCaprio toast: 84 frames × 50 ms
  { src: "/macmahon.gif",  loopMs: 3560 }, // McMahon money: 107 frames × ~33 ms
] as const;

// Trade-open pool — random pick per ENTRY event (deterministic by flash id so
// the render and the dismiss-timer agree without shared state).
const ENTRY_GIFS = [
  { src: "/andy-happening.gif", loopMs: 3760 }, // Andy "stay calm": 94 frames × 40 ms
  { src: "/caprio-finger.gif",  loopMs: 1870 }, // DiCaprio pointing: 28 frames × ~67 ms
] as const;

// Losing-trade pool — same pattern, deterministic by flash id.
const LOSS_GIFS = [
  { src: "/gosling-dive.gif", loopMs: 2000 }, // Gosling: 20 frames × 100 ms
  { src: "/escobar.gif",      loopMs: 1360 }, // sad Pablo: 34 frames × 40 ms
] as const;

function _hashIdx(id: string, mod: number): number {
  let h = 0;
  for (let i = 0; i < id.length; i++) h = ((h << 5) - h + id.charCodeAt(i)) | 0;
  return Math.abs(h) % mod;
}

function pickWinGif(id: string): (typeof WIN_GIFS)[number] {
  return WIN_GIFS[_hashIdx(id, WIN_GIFS.length)];
}

function pickEntryGif(id: string): (typeof ENTRY_GIFS)[number] {
  return ENTRY_GIFS[_hashIdx(id, ENTRY_GIFS.length)];
}

function pickLossGif(id: string): (typeof LOSS_GIFS)[number] {
  return LOSS_GIFS[_hashIdx(id, LOSS_GIFS.length)];
}

function dismissMsFor(loopMs: number): number {
  return loopMs * GIF_LOOPS + GIF_TAIL_MS;
}

// Floating toast-style burst for each new fill event
export default function AnimationCoordinator() {
  const flashes = useDash((s) => s.flashQueue);
  const consumeFlash = useDash((s) => s.consumeFlash);
  const timersRef = useRef<Map<string, number>>(new Map());
  const winFlash = flashes.find((f) => f.kind === "win");
  const winGif = winFlash ? pickWinGif(winFlash.id) : null;
  const lossFlash = flashes.find((f) => f.kind === "loss");
  const lossGif = lossFlash ? pickLossGif(lossFlash.id) : null;
  const entryFlash = flashes.find((f) => f.kind === "entry");
  const entryGif = entryFlash ? pickEntryGif(entryFlash.id) : null;

  useEffect(() => {
    const activeIds = new Set(flashes.map((f) => f.id));

    for (const f of flashes) {
      if (timersRef.current.has(f.id)) continue;
      let ms: number;
      if (f.kind === "win") {
        ms = dismissMsFor(pickWinGif(f.id).loopMs);
      } else if (f.kind === "loss") {
        ms = dismissMsFor(pickLossGif(f.id).loopMs);
      } else if (f.kind === "entry") {
        ms = dismissMsFor(pickEntryGif(f.id).loopMs);
      } else {
        ms = 3500;
      }
      const timerId = window.setTimeout(() => {
        timersRef.current.delete(f.id);
        consumeFlash(f.id);
      }, ms);
      timersRef.current.set(f.id, timerId);
    }

    for (const [id, timerId] of timersRef.current.entries()) {
      if (activeIds.has(id)) continue;
      clearTimeout(timerId);
      timersRef.current.delete(id);
    }
  }, [flashes, consumeFlash]);

  useEffect(() => {
    return () => {
      for (const timerId of timersRef.current.values()) {
        clearTimeout(timerId);
      }
      timersRef.current.clear();
    };
  }, []);

  return (
    <div className="absolute inset-0 z-[70] overflow-hidden pointer-events-none">
      {/* Win celebration — random gif from WIN_GIFS */}
      <AnimatePresence>
        {winFlash && winGif && (
          <motion.div
            key={winFlash.id}
            initial={{ opacity: 0, scale: 0.6, rotate: -4 }}
            animate={{ opacity: 1, scale: 1, rotate: 0 }}
            exit={{ opacity: 0, scale: 0.85, rotate: 3 }}
            transition={{ duration: 0.45, ease: [0.22, 1, 0.36, 1] }}
            className="absolute inset-0 flex items-center justify-center"
          >
            <div className="relative flex flex-col items-center">
              <div className="absolute inset-0 -m-10 rounded-full bg-emerald-500/20 blur-3xl" />
              <LoopingGif
                src={winGif.src}
                loopMs={winGif.loopMs}
                className="relative w-[26vw] max-w-[420px] min-w-[260px] rounded-2xl border-2 border-emerald-400/60 shadow-[0_0_60px_rgba(52,211,153,0.55)]"
              />
              <div className="relative -mt-4 px-4 py-1.5 rounded-full bg-ink-950/90 border border-emerald-400/60 font-mono text-emerald-200 text-lg shadow-2xl">
                WIN {fmtMoney(winFlash.amount ?? 0)}
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Losing-trade commiseration — random gif from LOSS_GIFS */}
      <AnimatePresence>
        {lossFlash && lossGif && (
          <motion.div
            key={lossFlash.id}
            initial={{ opacity: 0, scale: 0.6, rotate: -4 }}
            animate={{ opacity: 1, scale: 1, rotate: 0 }}
            exit={{ opacity: 0, scale: 0.85, rotate: 3 }}
            transition={{ duration: 0.45, ease: [0.22, 1, 0.36, 1] }}
            className="absolute inset-0 flex items-center justify-center"
          >
            <div className="relative flex flex-col items-center">
              <div className="absolute inset-0 -m-10 rounded-full bg-rose-500/20 blur-3xl" />
              <LoopingGif
                src={lossGif.src}
                loopMs={lossGif.loopMs}
                className="relative w-[26vw] max-w-[420px] min-w-[260px] rounded-2xl border-2 border-rose-400/60 shadow-[0_0_60px_rgba(251,113,133,0.55)]"
              />
              <div className="relative -mt-4 px-4 py-1.5 rounded-full bg-ink-950/90 border border-rose-400/60 font-mono text-rose-200 text-lg shadow-2xl">
                LOSS {fmtMoney(lossFlash.amount ?? 0)}
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Trade-open burst — random gif from ENTRY_GIFS, color tied to direction */}
      <AnimatePresence>
        {entryFlash && entryGif && (
          <motion.div
            key={entryFlash.id}
            initial={{ opacity: 0, scale: 0.6, rotate: -4 }}
            animate={{ opacity: 1, scale: 1, rotate: 0 }}
            exit={{ opacity: 0, scale: 0.85, rotate: 3 }}
            transition={{ duration: 0.45, ease: [0.22, 1, 0.36, 1] }}
            className="absolute inset-0 flex items-center justify-center"
          >
            <div className="relative flex flex-col items-center">
              <div
                className={`absolute inset-0 -m-10 rounded-full blur-3xl ${
                  entryFlash.direction === "DOWN"
                    ? "bg-rose-500/20"
                    : "bg-emerald-500/20"
                }`}
              />
              <LoopingGif
                src={entryGif.src}
                loopMs={entryGif.loopMs}
                className={`relative w-[26vw] max-w-[420px] min-w-[260px] rounded-2xl border-2 shadow-[0_0_60px_rgba(52,211,153,0.55)] ${
                  entryFlash.direction === "DOWN"
                    ? "border-rose-400/60 shadow-[0_0_60px_rgba(251,113,133,0.55)]"
                    : "border-emerald-400/60 shadow-[0_0_60px_rgba(52,211,153,0.55)]"
                }`}
              />
              <div
                className={`relative -mt-4 px-4 py-1.5 rounded-full bg-ink-950/90 border font-mono text-lg shadow-2xl ${
                  entryFlash.direction === "DOWN"
                    ? "border-rose-400/60 text-rose-200"
                    : "border-emerald-400/60 text-emerald-200"
                }`}
              >
                trade opening !{entryFlash.direction ? ` ${entryFlash.direction}` : ""}
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      <div className="absolute top-16 right-4 z-[71] flex flex-col gap-2">
      <AnimatePresence>
        {flashes.map((f) => {
          const palette =
            f.kind === "win"
              ? {
                  bg: "bg-emerald-950/92",
                  border: "border-emerald-300/70",
                  text: "text-emerald-50",
                  accent: "text-emerald-200",
                  glow: "shadow-[0_0_32px_rgba(52,211,153,0.30)]",
                }
              : f.kind === "loss"
                ? {
                    bg: "bg-rose-950/92",
                    border: "border-rose-300/70",
                    text: "text-rose-50",
                    accent: "text-rose-200",
                    glow: "shadow-[0_0_32px_rgba(251,113,133,0.28)]",
                  }
                : {
                    bg:
                      f.direction === "DOWN"
                        ? "bg-rose-950/92"
                        : "bg-emerald-950/92",
                    border:
                      f.direction === "DOWN"
                        ? "border-rose-300/70"
                        : "border-emerald-300/70",
                    text:
                      f.direction === "DOWN"
                        ? "text-rose-50"
                        : "text-emerald-50",
                    accent:
                      f.direction === "DOWN"
                        ? "text-rose-200"
                        : "text-emerald-200",
                    glow:
                      f.direction === "DOWN"
                        ? "shadow-[0_0_28px_rgba(251,113,133,0.22)]"
                        : "shadow-[0_0_28px_rgba(52,211,153,0.22)]",
                  };
          const title =
            f.kind === "entry"
              ? `Opened ${f.direction ?? ""}`
              : f.kind === "win"
                ? "Win"
                : "Loss";
          return (
            <motion.div
              key={f.id}
              initial={{ opacity: 0, x: 40, scale: 0.96 }}
              animate={{ opacity: 1, x: 0, scale: 1 }}
              exit={{ opacity: 0, x: 40, scale: 0.96 }}
              transition={{ duration: 0.4, ease: "easeOut" }}
              className={`card ${palette.bg} ${palette.border} ${palette.text} ${palette.glow} px-5 py-4 min-w-[260px] backdrop-blur-md ring-1 ring-black/30 shadow-2xl`}
            >
              <div className={`text-[11px] uppercase tracking-[0.22em] ${palette.accent} opacity-85`}>
                {f.kind === "entry" ? "position" : "realized"}
              </div>
              <div className="flex items-baseline justify-between mt-0.5">
                <span className="text-base font-semibold">{title}</span>
                {f.amount != null && (
                  <span className={`font-mono text-xl font-semibold ${palette.accent}`}>
                    {fmtMoney(f.amount)}
                  </span>
                )}
              </div>
            </motion.div>
          );
        })}
      </AnimatePresence>
      </div>
    </div>
  );
}

// Remounts the <img> at each loop boundary so the gif visibly replays for
// `loops` iterations even if the file's loop-count header is 1. The final
// playthrough runs uninterrupted to its end before the parent dismisses.
function LoopingGif({
  src,
  loopMs,
  loops = GIF_LOOPS,
  className,
}: {
  src: string;
  loopMs: number;
  loops?: number;
  className?: string;
}) {
  const [loop, setLoop] = useState(0);
  useEffect(() => {
    if (loop >= loops - 1) return;
    const id = window.setTimeout(() => setLoop((n) => n + 1), loopMs);
    return () => clearTimeout(id);
  }, [loop, loops, loopMs]);
  return (
    <img
      key={loop}
      src={loop === 0 ? src : `${src}?r=${loop}`}
      alt=""
      className={className}
    />
  );
}
