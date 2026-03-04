import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { DashboardLayout } from "@/components/DashboardLayout";
import { StatCard } from "@/components/StatCard";
import { StatusBadge } from "@/components/StatusBadge";
import { ScoreBadge } from "@/components/ScoreBadge";
import { LabelBadge } from "@/components/LabelBadge";
import { ChainBadge } from "@/components/ChainBadge";
import { ChainToggleGroup } from "@/components/ChainToggleGroup";
import { backendApi } from "@/lib/api";
import { formatInrFromUsd } from "@/lib/currency";
import { useCopyToClipboard } from "@/hooks/use-copy-clipboard";
import { Activity, Trophy, Target, Clock, Copy, ExternalLink } from "lucide-react";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

export default function DashboardPage() {
  const { copy } = useCopyToClipboard();
  const [topN, setTopN] = useState("20");
  const [selectedChains, setSelectedChains] = useState<string[]>([]);
  const { data: status } = useQuery({ queryKey: ["health"], queryFn: backendApi.health, refetchInterval: 15000 });
  const { data: picksData } = useQuery({
    queryKey: ["latest-picks", topN, selectedChains],
    queryFn: () => backendApi.latestPicks(parseInt(topN, 10), selectedChains),
    refetchInterval: 10000,
  });
  const picks = picksData?.rows ?? [];

  const GECKO_CHAIN_SLUG: Record<string, string> = {
    base: "base",
    eth: "eth",
    solana: "solana",
    bsc: "bsc",
  };

  const tokenUrl = (chain: string, address: string) => {
    const slug = GECKO_CHAIN_SLUG[chain] ?? chain;
    return `https://www.geckoterminal.com/${slug}/pools/${address}`;
  };

  if (!status) {
    return (
      <DashboardLayout>
        <div className="text-sm text-muted-foreground">Loading dashboard…</div>
      </DashboardLayout>
    );
  }

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold">Dashboard</h1>
          <p className="text-sm text-muted-foreground mt-1">Latest model run summary & top picks</p>
        </div>

        {/* Status badges */}
        <div className="flex flex-wrap gap-6">
          <StatusBadge status={status.database} label="Database" />
          <StatusBadge status={status.marketApi} label="Market API" />
          <StatusBadge status={status.pipeline} label="Pipeline" />
        </div>

        {/* Stats */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <StatCard title="Total Picks" value={status.totalPicks} icon={<Activity className="h-5 w-5" />} />
          <StatCard title="Win Rate" value={status.winRate !== null ? `${status.winRate.toFixed(1)}%` : "—"} icon={<Trophy className="h-5 w-5" />} />
          <StatCard title="Top Score" value={picks[0]?.modelScore?.toFixed(1) ?? "—"} icon={<Target className="h-5 w-5" />} />
          <StatCard title="Last Run" value={status.lastRun ? new Date(status.lastRun).toLocaleTimeString() : "—"} icon={<Clock className="h-5 w-5" />} subtitle="Latest snapshot" />
        </div>

        {/* Picks table */}
        <div className="glass-card rounded-xl overflow-hidden">
          <div className="p-4 border-b border-border/50 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <h2 className="font-semibold">Top Momentum Picks</h2>
              <p className="text-xs text-muted-foreground">Picked at {picksData?.pickedAt ? new Date(picksData.pickedAt).toLocaleString() : "—"}</p>
            </div>
            <div className="flex items-center gap-3">
              <Select value={topN} onValueChange={setTopN}>
                <SelectTrigger className="w-28 bg-muted/50"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="10">Top 10</SelectItem>
                  <SelectItem value="20">Top 20</SelectItem>
                  <SelectItem value="50">Top 50</SelectItem>
                  <SelectItem value="100">Top 100</SelectItem>
                </SelectContent>
              </Select>
              <ChainToggleGroup selected={selectedChains} onChange={setSelectedChains} />
            </div>
          </div>
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow className="border-border/50 hover:bg-transparent">
                  <TableHead className="w-12">#</TableHead>
                  <TableHead>Token</TableHead>
                  <TableHead>Chain</TableHead>
                  <TableHead className="hidden md:table-cell">Address</TableHead>
                  <TableHead>Score</TableHead>
                  <TableHead>Price</TableHead>
                  <TableHead className="hidden sm:table-cell">24h</TableHead>
                  <TableHead>Label</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {picks.map((pick) => (
                  <TableRow key={pick.rank} className="border-border/30 hover:bg-muted/30">
                    <TableCell className="font-mono text-muted-foreground">{pick.rank}</TableCell>
                    <TableCell>
                      <a
                        href={tokenUrl(pick.chain, pick.tokenAddress)}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="group flex items-center gap-1.5 hover:text-primary transition-colors"
                      >
                        <div>
                          <span className="font-semibold">{pick.symbol}</span>
                          <span className="text-xs text-muted-foreground ml-2 hidden sm:inline">{pick.name}</span>
                        </div>
                        <ExternalLink className="h-3 w-3 text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity" />
                      </a>
                    </TableCell>
                    <TableCell><ChainBadge chain={pick.chain} /></TableCell>
                    <TableCell className="hidden md:table-cell">
                      <button onClick={() => copy(pick.tokenAddress)} className="flex items-center gap-1 text-xs font-mono text-muted-foreground hover:text-foreground transition-colors">
                        {pick.tokenAddress.slice(0, 6)}...{pick.tokenAddress.slice(-4)}
                        <Copy className="h-3 w-3" />
                      </button>
                    </TableCell>
                    <TableCell><ScoreBadge score={pick.modelScore} /></TableCell>
                    <TableCell className="font-mono text-sm">{pick.currentPrice !== null ? formatInrFromUsd(pick.currentPrice) : "—"}</TableCell>
                    <TableCell className={`hidden sm:table-cell font-mono text-sm ${(pick.effectiveChange ?? 0) >= 0 ? "text-success" : "text-destructive"}`}>
                      {pick.effectiveChange !== null ? `${pick.effectiveChange >= 0 ? "+" : ""}${pick.effectiveChange.toFixed(2)}%` : "—"}
                    </TableCell>
                    <TableCell><LabelBadge label={pick.label} /></TableCell>
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
