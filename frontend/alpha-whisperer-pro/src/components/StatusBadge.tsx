import { cn } from "@/lib/utils";

interface StatusBadgeProps {
  status: "online" | "offline" | "degraded";
  label: string;
}

export function StatusBadge({ status, label }: StatusBadgeProps) {
  return (
    <div className="flex items-center gap-2">
      <span
        className={cn(
          "h-2 w-2 rounded-full",
          status === "online" && "bg-success animate-pulse",
          status === "degraded" && "bg-warning",
          status === "offline" && "bg-destructive"
        )}
      />
      <span className="text-sm text-muted-foreground">{label}</span>
      <span
        className={cn(
          "text-xs font-mono font-medium",
          status === "online" && "text-success",
          status === "degraded" && "text-warning",
          status === "offline" && "text-destructive"
        )}
      >
        {status}
      </span>
    </div>
  );
}
