import { cn } from "@/lib/utils";

interface LabelBadgeProps {
  label: "strong_buy" | "buy" | "neutral" | "sell";
}

const labelConfig = {
  strong_buy: { text: "Strong Buy", className: "text-success bg-success/10 border-success/30" },
  buy: { text: "Buy", className: "text-primary bg-primary/10 border-primary/30" },
  neutral: { text: "Neutral", className: "text-muted-foreground bg-muted border-border" },
  sell: { text: "Sell", className: "text-destructive bg-destructive/10 border-destructive/30" },
};

export function LabelBadge({ label }: LabelBadgeProps) {
  const config = labelConfig[label];
  return (
    <span className={cn("text-xs font-medium px-2 py-0.5 rounded-full border", config.className)}>
      {config.text}
    </span>
  );
}
