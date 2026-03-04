import { cn } from "@/lib/utils";

const chainConfig: Record<string, { label: string; className: string }> = {
  base: { label: "Base", className: "text-blue-400 bg-blue-500/10 border-blue-500/30" },
  eth: { label: "ETH", className: "text-indigo-400 bg-indigo-500/10 border-indigo-500/30" },
  solana: { label: "SOL", className: "text-purple-400 bg-purple-500/10 border-purple-500/30" },
  bsc: { label: "BSC", className: "text-yellow-400 bg-yellow-500/10 border-yellow-500/30" },
  polygon_pos: { label: "MATIC", className: "text-violet-400 bg-violet-500/10 border-violet-500/30" },
  arbitrum: { label: "ARB", className: "text-cyan-400 bg-cyan-500/10 border-cyan-500/30" },
  optimism: { label: "OP", className: "text-red-400 bg-red-500/10 border-red-500/30" },
  avalanche: { label: "AVAX", className: "text-rose-400 bg-rose-500/10 border-rose-500/30" },
};

const fallback = { label: "?", className: "text-muted-foreground bg-muted border-border" };

export function ChainBadge({ chain }: { chain: string }) {
  const config = chainConfig[chain?.toLowerCase()] ?? { ...fallback, label: chain?.toUpperCase() ?? "?" };
  return (
    <span className={cn("text-[10px] font-semibold uppercase tracking-wider px-1.5 py-0.5 rounded border", config.className)}>
      {config.label}
    </span>
  );
}
