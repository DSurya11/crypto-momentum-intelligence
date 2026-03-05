import { useQuery } from "@tanstack/react-query";
import { DashboardLayout } from "@/components/DashboardLayout";
import { StatCard } from "@/components/StatCard";
import { backendApi } from "@/lib/api";
import { formatInrFromUsd } from "@/lib/currency";
import { exportToCsv } from "@/lib/export-csv";
import { LabelBadge } from "@/components/LabelBadge";
import { ChainBadge } from "@/components/ChainBadge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Download, Trophy, TrendingUp, Target, TrendingDown, BarChart3, Zap, Brain } from "lucide-react";
import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, BarChart, Bar, Cell } from "recharts";
import { cn } from "@/lib/utils";

const CHAIN_COLORS: Record<string, string> = {
  base: "#3b82f6",
  eth: "#6366f1",
  solana: "#a855f7",
  bsc: "#eab308",
};

const REC_COLORS: Record<string, string> = {
  strong_buy: "#22c55e",
  buy: "#4ade80",
  neutral: "#94a3b8",
  sell: "#ef4444",
};

export default function PerformancePage() {
  const { data, isLoading } = useQuery({ queryKey: ["performance"], queryFn: () => backendApi.performance(200), refetchInterval: 15000 });
  const { data: impData } = useQuery({ queryKey: ["feature-importance"], queryFn: backendApi.featureImportance, refetchInterval: 30000 });
  const { data: threshData } = useQuery({ queryKey: ["thresholds"], queryFn: backendApi.thresholds, refetchInterval: 30000 });
  const rows = data?.rows ?? [];
  const chart = data?.cumulative ?? [];
  const summary = data?.summary ?? { winRate: 0, avgReturn2h: 0, total: 0 };
  const chainBreakdown = data?.chainBreakdown ?? {};
  const recBreakdown = data?.recBreakdown ?? {};

  const chainChartData = Object.entries(chainBreakdown).map(([chain, s]) => ({
    chain: chain.toUpperCase(),
    winRate: +s.winRate.toFixed(1),
    avgReturn: +s.avgReturn.toFixed(2),
    total: s.total,
    fill: CHAIN_COLORS[chain] ?? "#64748b",
  }));

  const recChartData = Object.entries(recBreakdown).map(([rec, s]) => ({
    rec: rec.replace("_", " "),
    winRate: +s.winRate.toFixed(1),
    avgReturn: +s.avgReturn.toFixed(2),
    total: s.total,
    fill: REC_COLORS[rec] ?? "#64748b",
  }));

  const impFeatures = impData?.features ?? {};
  const impChartData = Object.entries(impFeatures)
    .sort(([, a], [, b]) => b - a)
    .map(([name, pct]) => ({
      name: name.replace(/_/g, " "),
      importance: pct,
      fill: pct >= 15 ? "#22c55e" : pct >= 8 ? "#3b82f6" : "#64748b",
    }));

  if (isLoading) {
    return (
      <DashboardLayout>
        <div className="text-sm text-muted-foreground">Loading performance…</div>
      </DashboardLayout>
    );
  }

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold">Performance</h1>
            <p className="text-sm text-muted-foreground mt-1">Model accuracy, chain breakdown & verified outcomes</p>
          </div>
          <Button variant="outline" size="sm" onClick={() => exportToCsv("performance.csv", rows as any)}>
            <Download className="h-4 w-4 mr-1.5" />
            Export CSV
          </Button>
        </div>

        {/* ── Global stats ──────────────────────────────────────── */}
        <div className="grid grid-cols-3 gap-4">
          <StatCard title="Win Rate (2h)" value={`${summary.winRate.toFixed(1)}%`} icon={<Trophy className="h-5 w-5" />} />
          <StatCard title="Avg Return (2h)" value={`${summary.avgReturn2h.toFixed(2)}%`} icon={<TrendingUp className="h-5 w-5" />} trend={summary.avgReturn2h} />
          <StatCard title="Total Verified" value={summary.total} icon={<Target className="h-5 w-5" />} />
        </div>

        {/* ── Chain performance cards ───────────────────────────── */}
        {Object.keys(chainBreakdown).length > 0 && (
          <div>
            <h2 className="font-semibold mb-3 flex items-center gap-2">
              <BarChart3 className="h-4 w-4 text-primary" />
              Performance by Chain
            </h2>
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
              {Object.entries(chainBreakdown).map(([chain, s]) => (
                <div key={chain} className="glass-card rounded-xl p-4 space-y-3">
                  <div className="flex items-center justify-between">
                    <ChainBadge chain={chain} />
                    <span className="text-xs text-muted-foreground">{s.total} picks</span>
                  </div>
                  <div className="space-y-1.5">
                    <div className="flex justify-between text-sm">
                      <span className="text-muted-foreground">Win Rate</span>
                      <span className={cn("font-semibold tabular-nums", s.winRate >= 50 ? "text-emerald-400" : "text-red-400")}>
                        {s.winRate.toFixed(1)}%
                      </span>
                    </div>
                    <div className="relative h-1.5 rounded-full bg-muted/40 overflow-hidden">
                      <div
                        className={cn("h-full rounded-full transition-all", s.winRate >= 50 ? "bg-emerald-500" : "bg-red-500")}
                        style={{ width: `${Math.min(s.winRate, 100)}%` }}
                      />
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-muted-foreground">
                        Avg Return
                        {(s as any).outliers > 0 && (
                          <span className="ml-1 text-[10px] text-amber-400" title={`${(s as any).outliers} outlier(s) capped at ±500% for avg`}>⚠</span>
                        )}
                      </span>
                      <span className={cn("font-mono text-xs", s.avgReturn >= 0 ? "text-emerald-400" : "text-red-400")}>
                        {s.avgReturn >= 0 ? "+" : ""}{s.avgReturn.toFixed(2)}%
                      </span>
                    </div>
                    <div className="flex justify-between text-[11px] text-muted-foreground">
                      <span>Best: <span className="text-emerald-400">{s.bestReturn >= 0 ? "+" : ""}{s.bestReturn.toFixed(1)}%</span></span>
                      <span>Worst: <span className="text-red-400">{s.worstReturn >= 0 ? "+" : ""}{s.worstReturn.toFixed(1)}%</span></span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* ── Model recommendation accuracy ────────────────────── */}
        {Object.keys(recBreakdown).length > 0 && (
          <div>
            <h2 className="font-semibold mb-3 flex items-center gap-2">
              <Zap className="h-4 w-4 text-accent" />
              Model Accuracy by Recommendation
            </h2>
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
              {Object.entries(recBreakdown).map(([rec, s]) => {
                const label = rec as "strong_buy" | "buy" | "neutral" | "sell";
                const worked = s.winRate >= 50;
                return (
                  <div key={rec} className={cn(
                    "glass-card rounded-xl p-4 space-y-3 border",
                    worked ? "border-emerald-500/20" : "border-red-500/20"
                  )}>
                    <div className="flex items-center justify-between">
                      <LabelBadge label={label} />
                      <span className={cn(
                        "text-[10px] font-semibold px-2 py-0.5 rounded-full",
                        worked ? "bg-emerald-500/10 text-emerald-400" : "bg-red-500/10 text-red-400"
                      )}>
                        {worked ? "MODEL WORKED" : "NEEDS TUNING"}
                      </span>
                    </div>
                    <div className="space-y-1.5">
                      <div className="flex justify-between text-sm">
                        <span className="text-muted-foreground">Win Rate</span>
                        <span className={cn("font-semibold tabular-nums", s.winRate >= 50 ? "text-emerald-400" : "text-red-400")}>
                          {s.winRate.toFixed(1)}%
                        </span>
                      </div>
                      <div className="relative h-1.5 rounded-full bg-muted/40 overflow-hidden">
                        <div
                          className={cn("h-full rounded-full transition-all", s.winRate >= 50 ? "bg-emerald-500" : "bg-red-500")}
                          style={{ width: `${Math.min(s.winRate, 100)}%` }}
                        />
                      </div>
                      <div className="grid grid-cols-2 gap-x-2 text-[11px]">
                        <div className="text-muted-foreground">Total: <span className="text-foreground">{s.total}</span></div>
                        <div className="text-muted-foreground">Wins: <span className="text-emerald-400">{s.wins}</span></div>
                        {/* For sell: positive avg price = model was wrong → red. For buy: positive = good → green */}
                        <div className="text-muted-foreground">
                          Avg{(s as any).outliers > 0 && <span className="ml-0.5 text-amber-400" title={`${(s as any).outliers} outlier(s) capped at ±500%`}>⚠</span>}:{" "}
                          <span className={
                            label === "sell"
                              ? (s.avgReturn <= 0 ? "text-emerald-400" : "text-red-400")
                              : (s.avgReturn >= 0 ? "text-emerald-400" : "text-red-400")
                          }>{s.avgReturn >= 0 ? "+" : ""}{s.avgReturn.toFixed(2)}%</span>
                        </div>
                        {/* For sell: best model outcome = price fell most = worstReturn. For buy: bestReturn */}
                        <div className="text-muted-foreground">Best: <span className="text-emerald-400">{
                          label === "sell"
                            ? `${s.worstReturn.toFixed(1)}%`
                            : `+${s.bestReturn.toFixed(1)}%`
                        }</span></div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* ── Charts row ───────────────────────────────────────── */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {/* Cumulative return */}
          <div className="glass-card rounded-xl p-6">
            <h2 className="font-semibold mb-4 flex items-center gap-2">
              <TrendingUp className="h-4 w-4 text-primary" />
              Cumulative Return
            </h2>
            <div className="h-52">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={chart}>
                  <defs>
                    <linearGradient id="cumGrad" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="hsl(185, 80%, 50%)" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="hsl(185, 80%, 50%)" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="hsl(220, 14%, 18%)" />
                  <XAxis dataKey="date" tick={{ fill: "hsl(215, 12%, 52%)", fontSize: 11 }} axisLine={false} tickLine={false} />
                  <YAxis tick={{ fill: "hsl(215, 12%, 52%)", fontSize: 11 }} axisLine={false} tickLine={false} tickFormatter={v => `${v}%`} />
                  <Tooltip
                    contentStyle={{ background: "hsl(220, 18%, 10%)", border: "1px solid hsl(220, 14%, 18%)", borderRadius: "8px", color: "hsl(210, 20%, 92%)" }}
                    formatter={(v: number) => [`${v.toFixed(2)}%`, "Return"]}
                  />
                  <Area type="monotone" dataKey="cumReturn" stroke="hsl(185, 80%, 50%)" fill="url(#cumGrad)" strokeWidth={2} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Win rate by chain bar chart */}
          {chainChartData.length > 0 && (
            <div className="glass-card rounded-xl p-6">
              <h2 className="font-semibold mb-4 flex items-center gap-2">
                <BarChart3 className="h-4 w-4 text-accent" />
                Win Rate by Chain
              </h2>
              <div className="h-52">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={chainChartData} barCategoryGap="20%">
                    <CartesianGrid strokeDasharray="3 3" stroke="hsl(220, 14%, 18%)" />
                    <XAxis dataKey="chain" tick={{ fill: "hsl(215, 12%, 52%)", fontSize: 11 }} axisLine={false} tickLine={false} />
                    <YAxis tick={{ fill: "hsl(215, 12%, 52%)", fontSize: 11 }} axisLine={false} tickLine={false} tickFormatter={v => `${v}%`} domain={[0, 100]} />
                    <Tooltip
                      contentStyle={{ background: "hsl(220, 18%, 10%)", border: "1px solid hsl(220, 14%, 18%)", borderRadius: "8px", color: "hsl(210, 20%, 92%)" }}
                      formatter={(v: number, _name: string, props: any) => [`${v}% (${props.payload.total} picks)`, "Win Rate"]}
                    />
                    <Bar dataKey="winRate" radius={[6, 6, 0, 0]}>
                      {chainChartData.map((d, i) => (<Cell key={i} fill={d.fill} fillOpacity={0.8} />))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}
        </div>

        {/* ── Adaptive Score Thresholds ──────────────────────────── */}
        <div className="glass-card rounded-xl p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="font-semibold flex items-center gap-2">
              <Zap className="h-4 w-4 text-primary" />
              Score Thresholds
            </h2>
            <div className="text-[11px] text-muted-foreground space-x-3">
              {threshData?.calibrated
                ? <span className="text-green-400">Auto-calibrated from {threshData.sampleSize.toLocaleString()} picks</span>
                : <span className="text-yellow-400">Using defaults — need 150+ verified picks to calibrate</span>}
              {threshData?.calibratedAt && <span>at {new Date(threshData.calibratedAt).toLocaleTimeString()}</span>}
            </div>
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
            {[
              { label: "Strong Buy", value: threshData?.strongBuy ?? 35, color: "text-green-400", bg: "bg-green-400/10", desc: "≥ this score" },
              { label: "Buy",        value: threshData?.buy        ?? 27, color: "text-blue-400",  bg: "bg-blue-400/10",  desc: "≥ this score" },
              { label: "Neutral",    value: threshData?.neutral    ?? 20, color: "text-slate-400", bg: "bg-slate-400/10", desc: "≥ this score" },
              { label: "Sell",       value: threshData?.neutral    ?? 20, color: "text-red-400",   bg: "bg-red-400/10",   desc: "< neutral" },
            ].map(({ label, value, color, bg, desc }) => (
              <div key={label} className={`rounded-lg p-4 ${bg} flex flex-col gap-1`}>
                <span className={`text-xs font-medium ${color}`}>{label}</span>
                <span className={`text-2xl font-bold font-mono ${color}`}>
                  {label === "Sell" ? `< ${value.toFixed(1)}` : `${value.toFixed(1)}%`}
                </span>
                <span className="text-[10px] text-muted-foreground">{desc}</span>
              </div>
            ))}
          </div>
          <p className="text-[10px] text-muted-foreground mt-3">
            Thresholds auto-adjust every cycle based on which score ranges historically achieve ≥58% (strong buy), ≥52% (buy), ≥44% (neutral) real win rates.
          </p>
        </div>

        {/* ── Feature Importance ────────────────────────────────── */}
        {impChartData.length > 0 && (
          <div className="glass-card rounded-xl p-6">
            <div className="flex items-center justify-between mb-4">
              <h2 className="font-semibold flex items-center gap-2">
                <Brain className="h-4 w-4 text-primary" />
                Feature Importance
              </h2>
              <div className="text-[11px] text-muted-foreground space-x-3">
                {impData?.model && <span>Model: <span className="text-foreground">{impData.model}</span></span>}
                {impData?.trainRows ? <span>Train: <span className="text-foreground">{impData.trainRows.toLocaleString()}</span> rows</span> : null}
                {impData?.timestamp && <span>Updated: <span className="text-foreground">{new Date(impData.timestamp).toLocaleTimeString()}</span></span>}
              </div>
            </div>
            <div style={{ height: Math.max(180, impChartData.length * 32) }}>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={impChartData} layout="vertical" barCategoryGap="16%">
                  <CartesianGrid strokeDasharray="3 3" stroke="hsl(220, 14%, 18%)" horizontal={false} />
                  <XAxis type="number" tick={{ fill: "hsl(215, 12%, 52%)", fontSize: 11 }} axisLine={false} tickLine={false} tickFormatter={v => `${v}%`} domain={[0, 'auto']} />
                  <YAxis type="category" dataKey="name" tick={{ fill: "hsl(215, 12%, 52%)", fontSize: 11 }} axisLine={false} tickLine={false} width={180} />
                  <Tooltip
                    contentStyle={{ background: "hsl(220, 18%, 10%)", border: "1px solid hsl(220, 14%, 18%)", borderRadius: "8px", color: "hsl(210, 20%, 92%)" }}
                    formatter={(v: number) => [`${v.toFixed(1)}%`, "Importance"]}
                  />
                  <Bar dataKey="importance" radius={[0, 6, 6, 0]}>
                    {impChartData.map((d, i) => (<Cell key={i} fill={d.fill} fillOpacity={0.85} />))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
            <p className="text-[10px] text-muted-foreground mt-2">Higher % = model relies more on this feature for predictions. Green ≥15%, Blue ≥8%, Grey &lt;8%.</p>
          </div>
        )}

        {/* ── History table ─────────────────────────────────────── */}
        <div className="glass-card rounded-xl overflow-hidden">
          <div className="p-4 border-b border-border/50">
            <h2 className="font-semibold">Per-Token History</h2>
          </div>
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow className="border-border/50 hover:bg-transparent">
                  <TableHead>Token</TableHead>
                  <TableHead>Chain</TableHead>
                  <TableHead>Score</TableHead>
                  <TableHead>Model Call</TableHead>
                  <TableHead>Picked At</TableHead>
                  <TableHead>Entry Price</TableHead>
                  <TableHead>Price +5m</TableHead>
                  <TableHead>Price +10m</TableHead>
                  <TableHead>Price +2h</TableHead>
                  <TableHead>Eff. Return</TableHead>
                  <TableHead>Result</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map((p, i) => (
                  <TableRow key={i} className="border-border/30 hover:bg-muted/30">
                    <TableCell className="font-semibold">{p.symbol}</TableCell>
                    <TableCell><ChainBadge chain={p.chain} /></TableCell>
                    <TableCell className="font-mono text-sm">
                      {p.scorePct !== undefined ? `${p.scorePct.toFixed(1)}%` : "—"}
                    </TableCell>
                    <TableCell><LabelBadge label={p.recommendation} /></TableCell>
                    <TableCell className="text-sm text-muted-foreground">{new Date(p.pickedAt).toLocaleString()}</TableCell>
                    <TableCell className="font-mono text-sm">{formatInrFromUsd(p.pickedPrice)}</TableCell>
                    <TableCell className="font-mono text-sm">{p.price5m !== null ? formatInrFromUsd(p.price5m) : "—"}</TableCell>
                    <TableCell className="font-mono text-sm">{p.price10m !== null ? formatInrFromUsd(p.price10m) : "—"}</TableCell>
                    <TableCell className="font-mono text-sm">{p.price2h !== null ? formatInrFromUsd(p.price2h) : "—"}</TableCell>
                    <TableCell className={`font-mono text-sm ${(p.effectiveReturn2h ?? 0) >= 0 ? "text-emerald-400" : "text-red-400"}`}>
                      {p.effectiveReturn2h !== null ? `${p.effectiveReturn2h >= 0 ? "+" : ""}${p.effectiveReturn2h.toFixed(2)}%` : "—"}
                      {p.isOutlier && (p.recommendation === "buy" || p.recommendation === "strong_buy") && (
                        <span className="ml-1.5 inline-flex items-center text-[10px] font-semibold text-amber-400 bg-amber-500/10 px-1.5 py-0.5 rounded" title="Extreme return — capped at ±500% for avg calculation">
                          ⚠ outlier
                        </span>
                      )}
                    </TableCell>
                    <TableCell>
                      {p.verified ? (
                        (() => {
                          // For sell picks: model said "avoid" — Win = price fell (correct), Loss = price rose (missed gainer)
                          // For buy/strong_buy/neutral: Win = price rose
                          const ret = p.effectiveReturn2h ?? 0;
                          const isSell = p.recommendation === "sell";
                          const isModelWin = isSell ? ret <= 0 : ret > 0;
                          return isModelWin ? (
                            <span className="inline-flex items-center gap-1 text-[10px] font-semibold text-emerald-400 bg-emerald-500/10 px-2 py-0.5 rounded-full">
                              <TrendingUp className="h-3 w-3" /> Win
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1 text-[10px] font-semibold text-red-400 bg-red-500/10 px-2 py-0.5 rounded-full">
                              <TrendingDown className="h-3 w-3" /> Loss
                            </span>
                          );
                        })()
                      ) : (
                        <span className="text-xs text-muted-foreground">Pending</span>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </div>
      </div>
    </DashboardLayout>
  );
}
