import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { DashboardLayout } from "@/components/DashboardLayout";
import { ScoreBadge } from "@/components/ScoreBadge";
import { LabelBadge } from "@/components/LabelBadge";
import { ChainBadge } from "@/components/ChainBadge";
import { ChainToggleGroup } from "@/components/ChainToggleGroup";
import { backendApi } from "@/lib/api";
import { formatInrFromUsd } from "@/lib/currency";
import { exportToCsv } from "@/lib/export-csv";
import { useCopyToClipboard } from "@/hooks/use-copy-clipboard";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Copy, Download, RefreshCw, ExternalLink } from "lucide-react";

export default function LivePicksPage() {
  const { copy } = useCopyToClipboard();
  const [topN, setTopN] = useState("20");
  const [labelFilter, setLabelFilter] = useState("all");
  const [selectedChains, setSelectedChains] = useState<string[]>([]);
  const { data, refetch, isFetching } = useQuery({
    queryKey: ["latest-picks", topN, selectedChains],
    queryFn: () => backendApi.latestPicks(parseInt(topN, 10), selectedChains),
    refetchInterval: 10000,
  });
  const picks = data?.rows ?? [];

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

  const filtered = picks
    .filter((p) => labelFilter === "all" || p.label === labelFilter);

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold">Live Picks</h1>
            <p className="text-sm text-muted-foreground mt-1">Real-time refreshed momentum picks</p>
          </div>
          <div className="flex items-center gap-3">
            <Button variant="outline" size="sm" onClick={() => refetch()} disabled={isFetching}>
              <RefreshCw className={`h-4 w-4 mr-1.5 ${isFetching ? "animate-spin" : ""}`} />
              Refresh
            </Button>
            <Button variant="outline" size="sm" onClick={() => exportToCsv("picks.csv", filtered as any)}>
              <Download className="h-4 w-4 mr-1.5" />
              Export CSV
            </Button>
          </div>
        </div>

        {/* Filters */}
        <div className="flex flex-wrap gap-3">
          <Select value={topN} onValueChange={setTopN}>
            <SelectTrigger className="w-32 bg-muted/50"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="3">Top 3</SelectItem>
              <SelectItem value="5">Top 5</SelectItem>
              <SelectItem value="10">Top 10</SelectItem>
              <SelectItem value="20">Top 20</SelectItem>
              <SelectItem value="50">Top 50</SelectItem>
              <SelectItem value="100">Top 100</SelectItem>
            </SelectContent>
          </Select>
          <ChainToggleGroup selected={selectedChains} onChange={setSelectedChains} />
          <Select value={labelFilter} onValueChange={setLabelFilter}>
            <SelectTrigger className="w-36 bg-muted/50"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Labels</SelectItem>
              <SelectItem value="strong_buy">Strong Buy</SelectItem>
              <SelectItem value="buy">Buy</SelectItem>
              <SelectItem value="neutral">Neutral</SelectItem>
              <SelectItem value="sell">Sell</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {/* Table */}
        <div className="glass-card rounded-xl overflow-hidden">
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
                {filtered.length === 0 ? (
                  <TableRow><TableCell colSpan={8} className="text-center text-muted-foreground py-12">No picks match your filters.</TableCell></TableRow>
                ) : (
                  filtered.map((pick) => (
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
                      <TableCell className={`hidden sm:table-cell font-mono text-sm ${(pick.priceChange24h ?? 0) >= 0 ? "text-success" : "text-destructive"}`}>
                        {pick.priceChange24h !== null ? `${pick.priceChange24h >= 0 ? "+" : ""}${pick.priceChange24h.toFixed(2)}%` : "—"}
                      </TableCell>
                      <TableCell><LabelBadge label={pick.label} /></TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </div>
        </div>
      </div>
    </DashboardLayout>
  );
}
