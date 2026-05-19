import { cn } from "@/lib/utils";

interface ScoreBadgeProps {
  score: number;
  className?: string;
}

export function ScoreBadge({ score, className }: ScoreBadgeProps) {
  // ← CHANGED: normalize to 0-100 display regardless of whether score arrives as 0-1 or 0-100
  const displayScore = score <= 1.0 ? score * 100 : score;

  const getColor = () => {
    if (displayScore >= 90) return "text-success border-success/30 bg-success/10";
    if (displayScore >= 75) return "text-primary border-primary/30 bg-primary/10";
    if (displayScore >= 60) return "text-warning border-warning/30 bg-warning/10";
    return "text-destructive border-destructive/30 bg-destructive/10";
  };

  return (
    <span className={cn("font-mono text-sm font-semibold px-2 py-0.5 rounded border", getColor(), className)}>
      {displayScore.toFixed(1)}
    </span>
  );
}