import type { BootstrapPayload, LeaderboardRow, ViewSource } from "./types";
import { dashboardToken } from "./auth";

async function get<T>(path: string): Promise<T> {
  const token = dashboardToken();
  const headers = token ? { "X-Dashboard-Token": token } : undefined;
  const r = await fetch(path, { cache: "no-store", headers });
  if (!r.ok) throw new Error(`${path}: ${r.status}`);
  return r.json() as Promise<T>;
}

export const api = {
  health: () =>
    get<{
      ok: boolean;
      mode: string;
      bot_live: boolean;
    }>("/api/health"),
  bootstrap: (instanceId?: number, source?: ViewSource | null) =>
    get<BootstrapPayload>(
      `/api/bootstrap${query({ instance_id: instanceId, source })}`,
    ),
  instances: () => get<LeaderboardRow[]>("/api/instances"),
  instanceDetail: (id: number, source?: ViewSource | null) =>
    get<BootstrapPayload>(`/api/instance/${id}${query({ source })}`),
};

function query(params: Record<string, string | number | null | undefined>): string {
  const qs = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value == null) continue;
    qs.set(key, String(value));
  }
  const text = qs.toString();
  return text ? `?${text}` : "";
}
