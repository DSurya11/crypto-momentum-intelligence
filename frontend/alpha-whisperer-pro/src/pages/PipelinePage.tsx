import { useState, useCallback, useRef, useEffect } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { backendApi, type PipelineStep } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Play, CheckCircle2, XCircle, Loader2, ChevronDown, ChevronUp } from "lucide-react";
import { cn } from "@/lib/utils";
import { toast } from "@/hooks/use-toast";

/* ── step markers ───────────────────────────────────────────────── */
const STEP_MARKERS: { id: string; name: string; pattern: RegExp }[] = [
  { id: "ingest", name: "Ingest Swaps", pattern: /\[1\/5\]|Ingesting swaps|ingest/i },
  { id: "prices", name: "Build Prices", pattern: /\[2\/5\]|Deriving.*price/i },
  { id: "metrics", name: "Build Metrics", pattern: /\[3\/5\]|Aggregating/i },
  { id: "features", name: "Build Features", pattern: /\[4\/5\]|Building features/i },
  { id: "labels", name: "Build Labels", pattern: /\[5\/5\]|Building labels/i },
  { id: "picks", name: "Generate Picks", pattern: /LIVE PICKS|Running.*live_top_coins|\[PICKS\]/i },
];

function deriveSteps(logs: string[], running: boolean, ok: boolean | null): PipelineStep[] {
  const steps = STEP_MARKERS.map((m) => ({ ...m, status: "pending" as PipelineStep["status"] }));
  let lastHit = -1;
  for (const line of logs) {
    for (let i = 0; i < STEP_MARKERS.length; i++) {
      if (STEP_MARKERS[i].pattern.test(line)) lastHit = Math.max(lastHit, i);
    }
  }
  if (lastHit >= 0) {
    for (let i = 0; i < lastHit; i++) steps[i].status = "success";
    steps[lastHit].status = running ? "running" : ok === false ? "failed" : "success";
  }
  if (!running && ok !== null) {
    steps.forEach((s) => {
      if (s.status === "pending" || s.status === "running") s.status = ok ? "success" : "failed";
    });
  }
  return steps;
}

function progressPct(steps: PipelineStep[]): number {
  const done = steps.filter((s) => s.status === "success").length;
  const active = steps.filter((s) => s.status === "running").length;
  return Math.round(((done + active * 0.5) / steps.length) * 100);
}

/* ── component ──────────────────────────────────────────────────── */
export default function PipelinePage() {
  const [steps, setSteps] = useState<PipelineStep[]>(STEP_MARKERS.map((m) => ({ ...m, status: "pending" })));
  const [running, setRunning] = useState(false);
  const [logs, setLogs] = useState<string[]>([]);
  const [showLogs, setShowLogs] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const lineCountRef = useRef(0);
  const logsRef = useRef<string[]>([]);
  const logsEndRef = useRef<HTMLDivElement>(null);
  const startTimeRef = useRef<number | null>(null);
  const [elapsed, setElapsed] = useState(0);

  // elapsed timer
  useEffect(() => {
    if (!running) return;
    startTimeRef.current = Date.now();
    const t = setInterval(() => setElapsed(Math.floor((Date.now() - (startTimeRef.current ?? Date.now())) / 1000)), 1000);
    return () => clearInterval(t);
  }, [running]);

  // auto-scroll
  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [logs]);

  // cleanup
  useEffect(() => {
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, []);

  const startPolling = useCallback(() => {
    lineCountRef.current = 0;
    pollRef.current = setInterval(async () => {
      try {
        const status = await backendApi.cycleStatus(lineCountRef.current);
        if (status.newLines.length > 0) {
          const updated = [...logsRef.current, ...status.newLines];
          logsRef.current = updated;
          setLogs(updated);
          lineCountRef.current = status.totalLines;
        }
        setSteps(deriveSteps(logsRef.current, status.running, status.ok));
        if (!status.running) {
          if (pollRef.current) clearInterval(pollRef.current);
          pollRef.current = null;
          setRunning(false);
          toast({
            title: status.ok ? "Pipeline Complete" : "Pipeline Failed",
            description: status.ok ? "Live cycle executed successfully." : status.error || "Run cycle returned error.",
            variant: status.ok ? undefined : "destructive",
          });
        }
      } catch {
        /* network blip */
      }
    }, 1500);
  }, []);

  const runPipeline = useCallback(async () => {
    setRunning(true);
    setElapsed(0);
    logsRef.current = [];
    setLogs([]);
    setSteps(STEP_MARKERS.map((m) => ({ ...m, status: "pending" })));
    logsRef.current = [`[${new Date().toLocaleTimeString()}] Triggering run cycle...`];
    setLogs(logsRef.current);
    try {
      const result = await backendApi.runCycle({ tickCount: 1, topN: 50, marketApi: "coinstats" });
      if (result.alreadyRunning) {
        toast({ title: "Already Running", description: "A pipeline cycle is already in progress.", variant: "destructive" });
      }
      if (result.ok || result.alreadyRunning) {
        logsRef.current = [...logsRef.current, `[${new Date().toLocaleTimeString()}] Cycle started — streaming...`];
        setLogs(logsRef.current);
        startPolling();
      } else {
        setRunning(false);
        setSteps((prev) => prev.map((s) => ({ ...s, status: "failed" })));
        toast({ title: "Pipeline Failed", description: result.error || "Could not start.", variant: "destructive" });
      }
    } catch (err) {
      setRunning(false);
      setSteps((prev) => prev.map((s) => ({ ...s, status: "failed" })));
      logsRef.current = [...logsRef.current, String(err)];
      setLogs(logsRef.current);
      toast({ title: "Pipeline Failed", description: "Could not call backend.", variant: "destructive" });
    }
  }, [startPolling]);

  const pct = progressPct(steps);
  const finished = !running && steps.some((s) => s.status === "success" || s.status === "failed");
  const allSuccess = steps.every((s) => s.status === "success");
  const anyFailed = steps.some((s) => s.status === "failed");
  const fmtElapsed = `${Math.floor(elapsed / 60)}:${String(elapsed % 60).padStart(2, "0")}`;

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold">Run Pipeline</h1>
            <p className="text-sm text-muted-foreground mt-1">Ingest → Build → Score → Pick</p>
          </div>
          <Button
            onClick={runPipeline}
            disabled={running}
            className="relative overflow-hidden bg-gradient-to-r from-primary to-accent text-primary-foreground hover:opacity-90 px-8 h-11"
          >
            {running ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Play className="h-4 w-4 mr-2" />}
            {running ? "Running..." : "Run Complete Cycle"}
          </Button>
        </div>

        {/* ── Main progress card ──────────────────────────────────── */}
        <div className="glass-card rounded-2xl overflow-hidden">
          {/* Overall progress bar */}
          <div className="px-6 pt-6 pb-2">
            <div className="flex items-center justify-between mb-2">
              <span className="text-sm font-medium text-muted-foreground">
                {running ? "Processing..." : finished ? (allSuccess ? "Completed" : "Failed") : "Ready"}
              </span>
              <div className="flex items-center gap-3">
                {(running || finished) && (
                  <span className="text-xs font-mono text-muted-foreground">{fmtElapsed}</span>
                )}
                <span className={cn(
                  "text-sm font-bold tabular-nums",
                  allSuccess && "text-emerald-400",
                  anyFailed && "text-red-400",
                  running && "text-primary",
                )}>
                  {pct}%
                </span>
              </div>
            </div>

            {/* Animated bar */}
            <div className="relative h-3 w-full rounded-full bg-muted/60 overflow-hidden">
              <div
                className={cn(
                  "absolute inset-y-0 left-0 rounded-full transition-all duration-700 ease-out",
                  allSuccess && "bg-gradient-to-r from-emerald-500 to-emerald-400",
                  anyFailed && "bg-gradient-to-r from-red-600 to-red-400",
                  running && "bg-gradient-to-r from-primary via-accent to-primary bg-[length:200%_100%] animate-[shimmer_2s_linear_infinite]",
                  !running && !finished && "bg-muted",
                )}
                style={{ width: `${pct}%` }}
              />
              {running && (
                <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/10 to-transparent animate-[shimmer_2s_linear_infinite] bg-[length:200%_100%]" />
              )}
            </div>
          </div>

          {/* Step pills row */}
          <div className="px-6 py-5">
            <div className="flex items-center gap-1">
              {steps.map((step, i) => {
                const isActive = step.status === "running";
                const isDone = step.status === "success";
                const isFail = step.status === "failed";
                return (
                  <div key={step.id} className="flex items-center flex-1 min-w-0">
                    {/* connector line */}
                    {i > 0 && (
                      <div className={cn(
                        "h-[2px] flex-shrink-0 w-3 sm:w-6 transition-colors duration-500",
                        isDone || isFail ? (isFail ? "bg-red-500/50" : "bg-emerald-500/50") : isActive ? "bg-primary/40" : "bg-muted/50",
                      )} />
                    )}
                    {/* pill */}
                    <div
                      className={cn(
                        "flex items-center gap-1.5 px-2 sm:px-3 py-1.5 rounded-lg border text-xs font-medium transition-all duration-500 whitespace-nowrap min-w-0",
                        isDone && "border-emerald-500/30 bg-emerald-500/10 text-emerald-400",
                        isFail && "border-red-500/30 bg-red-500/10 text-red-400",
                        isActive && "border-primary/40 bg-primary/10 text-primary ring-2 ring-primary/20 shadow-[0_0_16px_-4px] shadow-primary/30",
                        !isDone && !isFail && !isActive && "border-border/30 bg-muted/20 text-muted-foreground",
                      )}
                    >
                      {isDone && <CheckCircle2 className="h-3.5 w-3.5 flex-shrink-0" />}
                      {isFail && <XCircle className="h-3.5 w-3.5 flex-shrink-0" />}
                      {isActive && <Loader2 className="h-3.5 w-3.5 flex-shrink-0 animate-spin" />}
                      {!isDone && !isFail && !isActive && (
                        <span className="h-3.5 w-3.5 flex-shrink-0 rounded-full border border-current opacity-40" />
                      )}
                      <span className="hidden sm:inline truncate">{step.name}</span>
                      <span className="sm:hidden truncate">{step.name.split(" ").pop()}</span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        {/* ── Collapsible logs ────────────────────────────────────── */}
        {logs.length > 0 && (
          <div className="glass-card rounded-2xl overflow-hidden">
            <button
              className="w-full flex items-center justify-between px-6 py-4 hover:bg-muted/20 transition-colors"
              onClick={() => setShowLogs(!showLogs)}
            >
              <div className="flex items-center gap-2">
                <h2 className="font-semibold text-sm">Logs</h2>
                <span className="text-[10px] font-mono text-muted-foreground bg-muted/40 px-1.5 py-0.5 rounded">{logs.length} lines</span>
              </div>
              {showLogs ? <ChevronUp className="h-4 w-4 text-muted-foreground" /> : <ChevronDown className="h-4 w-4 text-muted-foreground" />}
            </button>
            {showLogs && (
              <div className="px-6 pb-6">
                <div className="bg-[hsl(220,20%,6%)] rounded-xl p-4 font-mono text-[11px] leading-5 space-y-0.5 max-h-72 overflow-y-auto border border-border/20">
                  {logs.map((log, i) => (
                    <p
                      key={i}
                      className={cn(
                        "px-2 py-0.5 rounded",
                        log.includes("FAILED") || log.includes("Error") || log.includes("error")
                          ? "text-red-400 bg-red-500/5"
                          : log.includes("OK") || log.includes("complete") || log.includes("SUCCESS")
                          ? "text-emerald-400"
                          : "text-zinc-500",
                      )}
                    >
                      {log}
                    </p>
                  ))}
                  <div ref={logsEndRef} />
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* shimmer keyframe */}
      <style>{`
        @keyframes shimmer {
          0% { background-position: 200% 0; }
          100% { background-position: -200% 0; }
        }
      `}</style>
    </DashboardLayout>
  );
}
