import { cn } from "@/lib/utils";

interface ScoreBadgeProps {
  score: number;
  className?: string;
}

export function ScoreBadge({ score, className }: ScoreBadgeProps) {
  const getColor = () => {
    if (score >= 90) return "text-success border-success/30 bg-success/10";
    if (score >= 75) return "text-primary border-primary/30 bg-primary/10";
    if (score >= 60) return "text-warning border-warning/30 bg-warning/10";
    return "text-destructive border-destructive/30 bg-destructive/10";
  };

  return (
    <span className={cn("font-mono text-sm font-semibold px-2 py-0.5 rounded border", getColor(), className)}>
      {score.toFixed(1)}
    </span>
  );
}
