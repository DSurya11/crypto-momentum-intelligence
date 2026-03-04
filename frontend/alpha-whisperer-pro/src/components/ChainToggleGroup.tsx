import { cn } from "@/lib/utils";

const CHAINS = [
  { value: "base", label: "Base", className: "data-[on=true]:bg-blue-500/20 data-[on=true]:border-blue-500/40 data-[on=true]:text-blue-400" },
  { value: "eth", label: "ETH", className: "data-[on=true]:bg-indigo-500/20 data-[on=true]:border-indigo-500/40 data-[on=true]:text-indigo-400" },
  { value: "solana", label: "SOL", className: "data-[on=true]:bg-purple-500/20 data-[on=true]:border-purple-500/40 data-[on=true]:text-purple-400" },
  { value: "bsc", label: "BSC", className: "data-[on=true]:bg-yellow-500/20 data-[on=true]:border-yellow-500/40 data-[on=true]:text-yellow-400" },
] as const;

interface ChainToggleGroupProps {
  selected: string[];
  onChange: (chains: string[]) => void;
}

export function ChainToggleGroup({ selected, onChange }: ChainToggleGroupProps) {
  const allOn = selected.length === 0 || selected.length === CHAINS.length;

  const toggleAll = () => onChange([]);
  const toggle = (chain: string) => {
    if (allOn) {
      // switching from "all" → only this chain
      onChange([chain]);
      return;
    }
    const next = selected.includes(chain) ? selected.filter((c) => c !== chain) : [...selected, chain];
    // if all selected or none remaining, reset to "all"
    if (next.length === 0 || next.length === CHAINS.length) {
      onChange([]);
    } else {
      onChange(next);
    }
  };

  return (
    <div className="flex items-center gap-1.5">
      {/* All button */}
      <button
        data-on={allOn}
        onClick={toggleAll}
        className={cn(
          "px-3 py-1.5 text-xs font-semibold rounded-lg border transition-all duration-200 uppercase tracking-wider",
          "border-border/30 bg-muted/20 text-muted-foreground",
          "data-[on=true]:bg-primary/15 data-[on=true]:border-primary/40 data-[on=true]:text-primary",
          "hover:bg-muted/40",
        )}
      >
        All
      </button>

      {/* Per-chain toggles */}
      {CHAINS.map((c) => {
        const on = allOn || selected.includes(c.value);
        return (
          <button
            key={c.value}
            data-on={!allOn && on}
            onClick={() => toggle(c.value)}
            className={cn(
              "px-3 py-1.5 text-xs font-semibold rounded-lg border transition-all duration-200 uppercase tracking-wider",
              "border-border/30 bg-muted/20 text-muted-foreground hover:bg-muted/40",
              c.className,
            )}
          >
            {c.label}
          </button>
        );
      })}
    </div>
  );
}
