import { useState, useCallback } from "react";
import { toast } from "@/hooks/use-toast";

export function useCopyToClipboard() {
  const [copied, setCopied] = useState(false);

  const copy = useCallback((text: string) => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      toast({ title: "Copied!", description: "Address copied to clipboard" });
      setTimeout(() => setCopied(false), 2000);
    });
  }, []);

  return { copied, copy };
}
