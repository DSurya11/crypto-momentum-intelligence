import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { DashboardLayout } from "@/components/DashboardLayout";
import { StatCard } from "@/components/StatCard";
import { backendApi, MemeRadarResult, MemeRadarCoin } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Flame,
  TrendingUp,
  TrendingDown,
  Minus,
  RefreshCw,
  ExternalLink,
  MessageSquare,
  ArrowUp,
  Radar,
  Sparkles,
  Clock,
  Coins,
  DollarSign,
  BarChart3,
  Twitter,
  Hash,
} from "lucide-react";
import { cn } from "@/lib/utils";

/* ---- Growth phase badge ---- */
const PHASE_CONFIG: Record<string, { label: string; icon: typeof TrendingUp; class: string }> = {
  rising: { label: "Rising", icon: TrendingUp, class: "bg-green-500/20 text-green-400 border-green-500/30" },
  peaking: { label: "Peaking", icon: Minus, class: "bg-yellow-500/20 text-yellow-400 border-yellow-500/30" },
  declining: { label: "Declining", icon: TrendingDown, class: "bg-red-500/20 text-red-400 border-red-500/30" },
};

function GrowthBadge({ phase }: { phase: string }) {
  const cfg = PHASE_CONFIG[phase] ?? PHASE_CONFIG.declining;
  const Icon = cfg.icon;
  return (
    <Badge variant="outline" className={cn("text-xs font-mono gap-1", cfg.class)}>
      <Icon className="h-3 w-3" />
      {cfg.label}
    </Badge>
  );
}

/* ---- Virality bar ---- */
function ViralityBar({ score }: { score: number }) {
  const pct = Math.min(score, 100);
  const color = pct >= 70 ? "bg-green-500" : pct >= 40 ? "bg-yellow-500" : "bg-blue-500";
  return (
    <div className="flex items-center gap-2">
      <div className="w-24 h-2 rounded-full bg-muted overflow-hidden">
        <div className={cn("h-full rounded-full transition-all", color)} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs font-mono text-muted-foreground">{score.toFixed(0)}</span>
    </div>
  );
}

/* ---- Coin card (CoinStats data) ---- */
function CoinCard({ coin }: { coin: MemeRadarCoin }) {
  const priceStr = coin.price != null
    ? coin.price < 0.01
      ? `$${coin.price.toFixed(6)}`
      : `$${coin.price.toLocaleString(undefined, { maximumFractionDigits: 4 })}`
    : "—";
  const change = coin.priceChange24h;
  const mcap = coin.marketCap
    ? coin.marketCap >= 1e9
      ? `$${(coin.marketCap / 1e9).toFixed(2)}B`
      : coin.marketCap >= 1e6
        ? `$${(coin.marketCap / 1e6).toFixed(1)}M`
        : `$${coin.marketCap.toLocaleString()}`
    : null;
  const vol = coin.volume24h
    ? coin.volume24h >= 1e6
      ? `$${(coin.volume24h / 1e6).toFixed(1)}M`
      : `$${coin.volume24h.toLocaleString()}`
    : null;

  const coinStatsUrl = `https://coinstats.app/coins/${coin.id}`;

  return (
    <a
      href={coinStatsUrl}
      target="_blank"
      rel="noreferrer"
      className="flex items-center gap-3 p-3 rounded-lg bg-primary/5 border border-primary/10 hover:bg-primary/10 hover:border-primary/25 transition-colors"
    >
      {coin.icon ? (
        <img src={coin.icon} alt={coin.symbol} className="h-8 w-8 rounded-full flex-shrink-0" />
      ) : (
        <div className="h-8 w-8 rounded-full bg-muted flex items-center justify-center text-xs font-bold flex-shrink-0">
          {coin.symbol.slice(0, 2).toUpperCase()}
        </div>
      )}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="font-mono font-semibold text-sm">{coin.symbol.toUpperCase()}</span>
          <span className="text-xs text-muted-foreground truncate">{coin.name}</span>
          {coin.rank && <Badge variant="secondary" className="text-[10px] px-1 py-0">#{coin.rank}</Badge>}
          <ExternalLink className="h-3 w-3 text-muted-foreground" />
        </div>
        <div className="flex items-center gap-3 mt-0.5">
          <span className="text-xs font-mono">{priceStr}</span>
          {change != null && (
            <span className={cn("text-xs font-mono", change >= 0 ? "text-green-400" : "text-red-400")}>
              {change >= 0 ? "+" : ""}{change.toFixed(2)}%
            </span>
          )}
          {mcap && <span className="text-[10px] text-muted-foreground flex items-center gap-0.5"><DollarSign className="h-2.5 w-2.5" />{mcap}</span>}
          {vol && <span className="text-[10px] text-muted-foreground flex items-center gap-0.5"><BarChart3 className="h-2.5 w-2.5" />{vol}</span>}
        </div>
      </div>
    </a>
  );
}

/* ---- Meme card ---- */
function MemeCard({ meme }: { meme: MemeRadarResult }) {
  return (
    <div className="glass-card rounded-lg p-5 hover:border-primary/30 transition-all space-y-3">
      {/* Header row */}
      <div className="flex items-start gap-3">
        {meme.thumbnail && (
          <img
            src={meme.thumbnail}
            alt=""
            className="h-16 w-16 rounded-md object-cover flex-shrink-0 bg-muted"
            onError={(e) => (e.currentTarget.style.display = "none")}
          />
        )}
        <div className="flex-1 min-w-0">
          <a
            href={meme.permalink}
            target="_blank"
            rel="noreferrer"
            className="font-medium text-sm hover:text-primary transition-colors line-clamp-2"
          >
            {meme.title}
          </a>
          <div className="flex items-center gap-2 mt-1">
            {meme.source === "x" ? (
              <Badge variant="outline" className="text-xs font-mono gap-1 bg-sky-500/10 text-sky-400 border-sky-500/30">
                <Twitter className="h-3 w-3" /> X
              </Badge>
            ) : (
              <Badge variant="outline" className="text-xs font-mono gap-1">
                <Hash className="h-3 w-3" /> r/{meme.subreddit}
              </Badge>
            )}
            <GrowthBadge phase={meme.growthPhase} />
          </div>
        </div>
      </div>

      {/* Stats row */}
      <div className="flex flex-wrap items-center gap-3 text-xs text-muted-foreground">
        <span className="flex items-center gap-1">
          <ArrowUp className="h-3 w-3" /> {meme.score.toLocaleString()}
        </span>
        <span className="flex items-center gap-1">
          <MessageSquare className="h-3 w-3" /> {meme.numComments.toLocaleString()}
        </span>
        <span className="flex items-center gap-1">
          <Clock className="h-3 w-3" /> {meme.ageHours < 1 ? `${Math.round(meme.ageHours * 60)}m` : `${meme.ageHours.toFixed(1)}h`} ago
        </span>
        <span className="flex items-center gap-1">
          <Flame className="h-3 w-3 text-orange-400" /> {meme.upvoteVelocity.toFixed(0)}/hr
        </span>
        {meme.crossSubCount > 1 && (
          <span className="flex items-center gap-1 text-yellow-400">
            <Radar className="h-3 w-3" /> {meme.crossSubCount} subs
          </span>
        )}
      </div>

      {/* Virality */}
      <div className="flex items-center gap-3">
        <span className="text-xs text-muted-foreground w-16">Virality</span>
        <ViralityBar score={meme.viralityScore} />
      </div>

      {/* Keywords */}
      {meme.keywords.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {meme.keywords.map((kw) => (
            <span key={kw} className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-muted text-muted-foreground">
              {kw}
            </span>
          ))}
        </div>
      )}

      {/* Related coins — always present since we only show memes with matches */}
      <div className="space-y-2">
        <p className="text-xs text-muted-foreground flex items-center gap-1">
          <Coins className="h-3 w-3" /> Matched tokens
        </p>
        <div className="space-y-2">
          {meme.relatedCoins.map((coin) => (
            <CoinCard key={coin.id} coin={coin} />
          ))}
        </div>
      </div>
    </div>
  );
}

/* ====================================================================== */
/* Page                                                                    */
/* ====================================================================== */
export default function MemeRadarPage() {
  const [isRefreshing, setIsRefreshing] = useState(false);

  const { data, isLoading, refetch } = useQuery({
    queryKey: ["meme-radar"],
    queryFn: () => backendApi.memeRadar(false),
    // Poll every 5s while backend is still refreshing, otherwise every 15min
    refetchInterval: (query) => {
      const d = query.state.data as any;
      return d?.refreshing || d?.loading ? 5000 : 15 * 60 * 1000;
    },
    retry: 1,
  });

  const isBackendRefreshing = data?.refreshing === true || data?.loading === true;
  const hasData = (data?.results?.length ?? 0) > 0;
  const results = data?.results ?? [];
  const totalScanned = data?.totalScanned ?? 0;
  const totalViable = data?.totalViable ?? 0;
  const memesSearched = data?.memesSearched ?? 0;

  const risingCount = results.filter((r) => r.growthPhase === "rising").length;
  const avgVirality = results.length ? results.reduce((s, r) => s + r.viralityScore, 0) / results.length : 0;
  const totalCoins = results.reduce((s, r) => s + r.relatedCoins.length, 0);

  const handleRefresh = async () => {
    setIsRefreshing(true);
    try {
      await backendApi.memeRadar(true);
      await refetch();
    } finally {
      setIsRefreshing(false);
    }
  };

  return (
    <DashboardLayout>
      {/* Page header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Radar className="h-6 w-6 text-primary" />
            Meme Radar
          </h1>
          <p className="text-sm text-muted-foreground mt-1">
            Trending memes from Reddit & X matched to crypto tokens via CoinStats
          </p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={handleRefresh}
          disabled={isRefreshing || isBackendRefreshing}
          className="gap-2"
        >
          <RefreshCw className={cn("h-4 w-4", (isRefreshing || isBackendRefreshing) && "animate-spin")} />
          {isRefreshing || isBackendRefreshing ? "Scanning…" : "Refresh"}
        </Button>
      </div>

      {/* Stat cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        <StatCard title="Memes Scanned" value={totalScanned} icon={<Flame className="h-5 w-5" />} />
        <StatCard title="Memes Searched" value={memesSearched} icon={<Radar className="h-5 w-5" />} subtitle="checked CoinStats" />
        <StatCard title="Meme + Coin Matches" value={results.length} icon={<Coins className="h-5 w-5" />} subtitle={`${totalCoins} tokens found`} />
        <StatCard title="Rising Now" value={risingCount} icon={<Sparkles className="h-5 w-5" />} subtitle={`avg virality ${avgVirality.toFixed(0)}`} />
      </div>

      {/* Full loading state — no cache yet */}
      {isBackendRefreshing && !hasData && (
        <div className="flex flex-col items-center justify-center py-20 gap-3">
          <RefreshCw className="h-8 w-8 text-primary animate-spin" />
          <p className="text-sm text-muted-foreground">Scanning Reddit, X & CoinStats…</p>
          <p className="text-xs text-muted-foreground">This takes ~30–60 seconds on first load</p>
        </div>
      )}

      {/* Background-refresh banner — stale data visible while new scan runs */}
      {isBackendRefreshing && hasData && (
        <div className="flex items-center gap-2 px-4 py-2 mb-4 rounded-md bg-muted/60 text-xs text-muted-foreground border border-border">
          <RefreshCw className="h-3 w-3 animate-spin shrink-0" />
          Refreshing meme data in background — showing last cached results
        </div>
      )}

      {/* Results grid */}
      {hasData && (
        <div className="space-y-6">
          {/* Meme + Coin cards */}
          <div>
            <h2 className="text-lg font-semibold mb-3 flex items-center gap-2">
              <Flame className="h-5 w-5 text-orange-400" />
              Meme–Coin Matches
              <Badge variant="secondary" className="ml-1">{results.length}</Badge>
            </h2>
            <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
              {results.map((meme, i) => (
                <MemeCard key={`${meme.subreddit}-${i}`} meme={meme} />
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Empty state — scan finished but no matches */}
      {!isBackendRefreshing && !hasData && !isLoading && (
        <div className="flex flex-col items-center justify-center py-20 gap-3 text-muted-foreground">
          <Radar className="h-12 w-12 opacity-30" />
          <p className="text-lg">No meme–coin matches found</p>
          <p className="text-sm">Only memes with an exact matching crypto token are shown</p>
        </div>
      )}

      {/* Footer info */}
      {data?.timestamp && (
        <p className="text-xs text-muted-foreground text-center mt-6">
          Last scan: {new Date(data.timestamp).toLocaleString()} — Results cached for 5 minutes
        </p>
      )}
    </DashboardLayout>
  );
}
