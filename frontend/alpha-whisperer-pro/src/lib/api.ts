export type HealthStatus = "online" | "offline" | "degraded";

export interface CryptoPick {
  rank: number;
  symbol: string;
  name: string;
  tokenAddress: string;
  chain: string;
  modelScore: number;
  currentPrice: number | null;
  priceChange24h: number | null;
  effectiveChange: number | null;
  pickedAt: string;
  verifyAfterMinutes: number;
  label: "strong_buy" | "buy" | "neutral" | "sell";
  marketCap?: number | null;
  volume24h?: number | null;
}

export interface SystemStatus {
  database: HealthStatus;
  marketApi: HealthStatus;
  pipeline: HealthStatus;
  lastRun: string | null;
  totalPicks: number;
  winRate: number | null;
  swaps: number;
  labels: number;
  latestBucket: string | null;
}

export interface PerformanceRecord {
  symbol: string;
  name: string;
  chain: string;
  pickedAt: string;
  recommendation: "strong_buy" | "buy" | "neutral" | "sell";
  scorePct: number;
  pickedPrice: number;
  price5m: number | null;
  price10m: number | null;
  price2h: number | null;
  futurePoints: number;
  return5m: number | null;
  return10m: number | null;
  return2h: number | null;
  effectiveReturn5m: number | null;
  effectiveReturn10m: number | null;
  effectiveReturn2h: number | null;
  verified: boolean;
}

export interface PipelineStep {
  id: string;
  name: string;
  status: "pending" | "running" | "success" | "failed";
  duration?: number;
  message?: string;
}

const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8001";

async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) throw new Error(`GET ${path} failed: ${res.status}`);
  return res.json() as Promise<T>;
}

async function apiPost<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`POST ${path} failed: ${res.status}`);
  return res.json() as Promise<T>;
}

export const backendApi = {
  health: () => apiGet<SystemStatus>("/api/health"),
  latestPicks: (topN = 10, chains: string[] = []) =>
    apiGet<{ pickedAt: string; elapsedMinutes: number; rows: CryptoPick[] }>(
      `/api/latest-picks?top_n=${topN}&chain=${encodeURIComponent(chains.length ? chains.join(",") : "all")}`
    ),
  verifyLatest: () => apiGet<{ pickedAt: string; now: string; elapsedMinutes: number; rows: Array<{ rank: number; symbol: string; name: string; tokenAddress: string; entryPrice: number | null; nowPrice: number | null; changePct: number | null }> }>("/api/verify-latest"),
  performance: (limit = 200) => apiGet<{
    rows: PerformanceRecord[];
    cumulative: Array<{ date: string; cumReturn: number }>;
    summary: { winRate: number; avgReturn2h: number; total: number };
    chainBreakdown?: Record<string, { total: number; wins: number; winRate: number; avgReturn: number; bestReturn: number; worstReturn: number }>;
    recBreakdown?: Record<string, { total: number; wins: number; winRate: number; avgReturn: number; bestReturn: number; worstReturn: number }>;
  }>(`/api/performance?limit=${limit}&labels=strong_buy,buy,neutral,sell&verified_only=true`),
  settings: () => apiGet<{ env: Array<{ key: string; value: string; masked: boolean }> }>("/api/settings"),
  featureImportance: () => apiGet<{ features: Record<string, number>; timestamp: string | null; model: string | null; featureSet: string | null; trainRows: number; scoringRows: number }>("/api/feature-importance"),
  runCycle: (payload: { tickCount: number; topN: number; marketApi: string }) => apiPost<{ ok: boolean; started?: boolean; alreadyRunning?: boolean; error?: string }>("/api/run-cycle", payload),
  cycleStatus: (sinceLine = 0) => apiGet<{ running: boolean; startedAt: string | null; finishedAt: string | null; ok: boolean | null; code: number | null; error: string | null; totalLines: number; newLines: string[] }>(`/api/cycle-status?since_line=${sinceLine}`),
  memeRadar: (refresh = false) => apiGet<MemeRadarResponse>(`/api/meme-radar${refresh ? "?refresh=true" : ""}`),
};

/* ---- Meme Radar types ---- */
export interface MemeRadarCoin {
  id: string;
  symbol: string;
  name: string;
  rank: number | null;
  icon: string;
  price: number | null;
  priceChange24h: number | null;
  marketCap: number | null;
  volume24h: number | null;
  matchKeyword?: string;
  matchType?: string;
}

export interface MemeRadarResult {
  title: string;
  subreddit: string;
  source: "reddit" | "x";
  url: string;
  permalink: string;
  score: number;
  numComments: number;
  ageHours: number;
  upvoteVelocity: number;
  commentRatio: number;
  viralityScore: number;
  growthPhase: "rising" | "peaking" | "declining";
  keywords: string[];
  crossSubCount: number;
  relatedCoins: MemeRadarCoin[];
  thumbnail: string;
}

export interface MemeRadarResponse {
  timestamp: string;
  totalScanned: number;
  totalViable: number;
  memesSearched: number;
  results: MemeRadarResult[];
  refreshing?: boolean;
  loading?: boolean;
}
