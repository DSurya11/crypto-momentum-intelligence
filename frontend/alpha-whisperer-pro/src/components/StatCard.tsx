import { ReactNode } from "react";
import { cn } from "@/lib/utils";

interface StatCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  icon?: ReactNode;
  trend?: number;
  className?: string;
}

export function StatCard({ title, value, subtitle, icon, trend, className }: StatCardProps) {
  return (
    <div className={cn("glass-card rounded-lg p-5 glow-primary transition-all hover:border-primary/30", className)}>
      <div className="flex items-start justify-between">
        <div>
          <p className="text-sm text-muted-foreground">{title}</p>
          <p className="text-2xl font-bold font-mono mt-1">{value}</p>
          {subtitle && <p className="text-xs text-muted-foreground mt-1">{subtitle}</p>}
          {trend !== undefined && (
            <p className={cn("text-xs font-mono mt-1", trend >= 0 ? "text-success" : "text-destructive")}>
              {trend >= 0 ? "+" : ""}{trend.toFixed(2)}%
            </p>
          )}
        </div>
        {icon && <div className="text-primary/60">{icon}</div>}
      </div>
    </div>
  );
}
