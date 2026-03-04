import { useQuery } from "@tanstack/react-query";
import { DashboardLayout } from "@/components/DashboardLayout";
import { StatusBadge } from "@/components/StatusBadge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { backendApi } from "@/lib/api";
import { toast } from "@/hooks/use-toast";
import { ShieldCheck, RefreshCw } from "lucide-react";
import { useState } from "react";

export default function SettingsPage() {
  const [checking, setChecking] = useState(false);
  const healthQ = useQuery({ queryKey: ["health"], queryFn: () => backendApi.health(), refetchInterval: 15000 });
  const settingsQ = useQuery({ queryKey: ["settings"], queryFn: () => backendApi.settings(), refetchInterval: 30000 });

  const healthCheck = async () => {
    setChecking(true);
    try {
      const refreshed = await healthQ.refetch();
      if (refreshed.data && refreshed.data.database !== "offline" && refreshed.data.pipeline !== "offline") {
        toast({ title: "Health Check Passed", description: "Core services are reachable." });
      } else {
        toast({ title: "Health Check Warning", description: "One or more services look degraded/offline." });
      }
    } catch {
      toast({ title: "Health Check Failed", description: "Could not reach backend health endpoint." });
    } finally {
      setChecking(false);
    }
  };

  if (healthQ.isLoading || settingsQ.isLoading) {
    return (
      <DashboardLayout>
        <div className="text-sm text-muted-foreground">Loading settings…</div>
      </DashboardLayout>
    );
  }

  const systemStatus = healthQ.data ?? {
    database: "offline",
    marketApi: "offline",
    pipeline: "offline",
    lastRun: null,
    totalPicks: 0,
    winRate: null,
    swaps: 0,
    labels: 0,
    latestBucket: null,
  };
  const envFields = settingsQ.data?.env ?? [];

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold">Settings</h1>
          <p className="text-sm text-muted-foreground mt-1">Environment configuration & API health</p>
        </div>

        {/* System Status */}
        <div className="glass-card rounded-xl p-6 space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="font-semibold">System Status</h2>
            <Button variant="outline" size="sm" onClick={healthCheck} disabled={checking}>
              {checking ? <RefreshCw className="h-4 w-4 mr-1.5 animate-spin" /> : <ShieldCheck className="h-4 w-4 mr-1.5" />}
              Health Check
            </Button>
          </div>
          <div className="flex flex-wrap gap-6">
            <StatusBadge status={systemStatus.database} label="Database" />
            <StatusBadge status={systemStatus.marketApi} label="Market API" />
            <StatusBadge status={systemStatus.pipeline} label="Pipeline" />
          </div>
        </div>

        {/* Environment */}
        <div className="glass-card rounded-xl p-6 space-y-4">
          <h2 className="font-semibold">Environment Variables</h2>
          <p className="text-xs text-muted-foreground">Secrets are masked. Modify through your deployment environment.</p>
          <div className="space-y-3">
            {envFields.map((field) => (
              <div key={field.key}>
                <Label className="text-xs font-mono text-muted-foreground">{field.key}</Label>
                <Input
                  value={field.value}
                  readOnly
                  className="mt-1 bg-muted/50 font-mono text-sm"
                />
              </div>
            ))}
          </div>
        </div>
      </div>
    </DashboardLayout>
  );
}
