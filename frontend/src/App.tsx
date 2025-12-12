import { useEffect, useMemo, useState } from "react";
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";
import { motion } from "framer-motion";

type DeviceKey = "bulb" | "fan" | "iron";

interface ProbabilityEntry {
  label: number;
  probability: number;
  device_states: Record<DeviceKey, boolean>;
}

interface PredictionPayload {
  label: number;
  device_states: Record<DeviceKey, boolean>;
  probabilities: ProbabilityEntry[];
}

interface RawPayload {
  timestamp?: string;
  [key: string]: unknown;
}

interface LivePacket {
  timestamp?: string;
  features?: Record<string, number>;
  raw?: RawPayload;
  source?: { broker: string; topic: string };
  prediction: PredictionPayload;
}

interface HistoryPoint {
  time: string;
  power: number;
}

interface StatusResponse {
  mqtt: { broker: string; topic: string; connected: boolean; messages_seen: number };
  history_length: number;
  total_messages: number;
  latest_timestamp?: string | null;
  devices: Record<string, { label: string; nominal_w: number }>;
}

const DEVICE_CATALOG: Record<DeviceKey, { label: string; nominal: number }> = {
  bulb: { label: "Bulb", nominal: 60 },
  fan: { label: "Fan", nominal: 90 },
  iron: { label: "Iron", nominal: 1100 },
};

const LABEL_SUMMARY: Record<number, string> = {
  1: "Bulb only",
  2: "Fan only",
  3: "Bulb + Fan",
  4: "Iron only",
  5: "Bulb + Iron",
  6: "Fan + Iron",
  7: "All devices",
};

const HTTP_BASE = (import.meta.env.VITE_BACKEND_URL ?? "http://localhost:8000").replace(/\/$/, "");
const WS_BASE = HTTP_BASE.startsWith("https") ? `wss${HTTP_BASE.slice(5)}` : HTTP_BASE.replace("http", "ws");
const WS_URL = `${WS_BASE}/ws/live`;

type ConnectionState = "connecting" | "open" | "closed";

export default function NILMRealtimeUI() {
  const [latest, setLatest] = useState<LivePacket | null>(null);
  const [history, setHistory] = useState<HistoryPoint[]>([]);
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [connectionState, setConnectionState] = useState<ConnectionState>("connecting");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let retryHandle: number | null = null;

    const connect = () => {
      setConnectionState("connecting");
      setErrorMessage(null);
      ws = new WebSocket(WS_URL);
      ws.onopen = () => setConnectionState("open");
      ws.onclose = () => {
        setConnectionState("closed");
        retryHandle = window.setTimeout(connect, 3000);
      };
      ws.onerror = () => {
        setErrorMessage("WebSocket error");
        ws?.close();
      };
      ws.onmessage = (event) => {
        try {
          const packet: LivePacket = JSON.parse(event.data);
          setLatest(packet);
          const resolvedTs = packet.timestamp ?? (packet.raw?.timestamp ?? new Date().toISOString());
          const point: HistoryPoint = {
            time: new Date(resolvedTs).toLocaleTimeString(),
            power: Number(packet.features?.active_power ?? 0),
          };
          setHistory((prev) => {
            const next = [...prev, point];
            if (next.length > 60) next.shift();
            return next;
          });
        } catch (err) {
          console.error("Failed to parse live payload", err);
        }
      };
    };

    connect();

    return () => {
      if (retryHandle) window.clearTimeout(retryHandle);
      ws?.close();
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function bootstrapHistory() {
      try {
        const res = await fetch(`${HTTP_BASE}/api/history?limit=40`);
        if (!res.ok) throw new Error(`History request failed with ${res.status}`);
        const data = await res.json();
        if (cancelled) return;
        const items = (data.items ?? []) as LivePacket[];
        const mapped: HistoryPoint[] = items.map((item) => ({
          time: new Date(item.timestamp ?? item.raw?.timestamp ?? Date.now()).toLocaleTimeString(),
          power: Number(item.features?.active_power ?? 0),
        }));
        setHistory(mapped);
      } catch (err) {
        console.warn(err);
      }
    }

    bootstrapHistory();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function fetchStatus() {
      try {
        const res = await fetch(`${HTTP_BASE}/api/status`);
        if (!res.ok) throw new Error("Status request failed");
        const data = (await res.json()) as StatusResponse;
        if (!cancelled) setStatus(data);
      } catch (err) {
        console.warn(err);
      }
    }

    fetchStatus();
    const interval = window.setInterval(fetchStatus, 10000);
    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, []);

  const deviceStates = latest?.prediction.device_states ?? { bulb: false, fan: false, iron: false };
  const aggregatedPower = Number(latest?.features?.active_power ?? 0).toFixed(2);
  const voltage = Number(latest?.features?.voltage ?? 0).toFixed(2);
  const current = Number(latest?.features?.current ?? 0).toFixed(3);
  const pf = Number(latest?.features?.power_factor ?? 0).toFixed(3);
  const topLabel = latest?.prediction.label ?? null;
  const topProb = latest?.prediction.probabilities?.[0]?.probability ?? 0;

  const probabilityRows = useMemo(() => {
    const rows = latest?.prediction.probabilities ?? [];
    return rows.slice(0, 5);
  }, [latest]);

  const connectionChip = {
    connecting: "bg-amber-500/20 text-amber-400",
    open: "bg-emerald-500/15 text-emerald-400",
    closed: "bg-rose-500/15 text-rose-400",
  }[connectionState];

  return (
    <div className="min-h-screen bg-slate-950 text-slate-50">
      <div className="mx-auto max-w-6xl px-6 py-8">
        <header className="mb-8 flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <p className="text-sm uppercase tracking-[0.3em] text-slate-500">NILM Control Room</p>
            <h1 className="mt-2 text-3xl font-semibold text-white">Live disaggregation stream</h1>
            <p className="mt-1 text-sm text-slate-400">Broker: {status?.mqtt.broker ?? "..."} • Topic: {status?.mqtt.topic ?? "..."}</p>
          </div>
          <div className="flex flex-wrap items-center gap-3 text-sm">
            <span className={`rounded-full px-4 py-1 font-medium ${connectionChip}`}>{connectionState}</span>
            <span className="rounded-full bg-slate-900 px-4 py-1 text-slate-300">
              Messages seen: {status?.mqtt.messages_seen ?? 0}
            </span>
          </div>
        </header>

        {errorMessage && <div className="mb-6 rounded-xl border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-100">{errorMessage}</div>}

        <section className="grid gap-6 lg:grid-cols-3">
          <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-6 lg:col-span-2">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
              <div>
                <p className="text-xs uppercase tracking-[0.2em] text-slate-500">Predicted Cluster</p>
                <h2 className="mt-2 text-3xl font-semibold text-white">
                  {topLabel ? LABEL_SUMMARY[topLabel] ?? `Class ${topLabel}` : "Waiting for signal"}
                </h2>
                {topLabel && (
                  <p className="text-sm text-slate-400">Confidence {(topProb * 100).toFixed(1)}%</p>
                )}
              </div>
              <div className="grid grid-cols-2 gap-4 text-sm">
                <div className="rounded-2xl bg-slate-950/60 px-4 py-3">
                  <p className="text-xs text-slate-500">Voltage</p>
                  <p className="text-2xl font-semibold text-white">{voltage} V</p>
                </div>
                <div className="rounded-2xl bg-slate-950/60 px-4 py-3">
                  <p className="text-xs text-slate-500">Current</p>
                  <p className="text-2xl font-semibold text-white">{current} A</p>
                </div>
                <div className="rounded-2xl bg-slate-950/60 px-4 py-3">
                  <p className="text-xs text-slate-500">Active Power</p>
                  <p className="text-2xl font-semibold text-white">{aggregatedPower} W</p>
                </div>
                <div className="rounded-2xl bg-slate-950/60 px-4 py-3">
                  <p className="text-xs text-slate-500">Power Factor</p>
                  <p className="text-2xl font-semibold text-white">{pf}</p>
                </div>
              </div>
            </div>

            <div className="mt-6 h-52 w-full">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={history} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                  <XAxis dataKey="time" tick={{ fill: "#94a3b8", fontSize: 10 }} minTickGap={20} />
                  <YAxis
                    tick={{ fill: "#94a3b8", fontSize: 10 }}
                    domain={[0, (dataMax?: number) => Math.max(400, (dataMax ?? 0) + 200)]}
                  />
                  <Tooltip contentStyle={{ backgroundColor: "#020617", borderRadius: 12, border: "1px solid rgba(255,255,255,0.05)" }} />
                  <Line type="monotone" dataKey="power" stroke="#38bdf8" strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Live Devices</p>
                <p className="text-sm text-slate-400">Decoded from RF prediction</p>
              </div>
              <span className="text-2xl font-semibold">{Object.values(deviceStates).filter(Boolean).length}</span>
            </div>
            <div className="mt-4 space-y-3">
              {(Object.keys(DEVICE_CATALOG) as DeviceKey[]).map((key) => {
                const device = DEVICE_CATALOG[key];
                const active = deviceStates[key];
                return (
                  <motion.div
                    key={key}
                    layout
                    className={`flex items-center justify-between rounded-2xl border px-4 py-3 text-sm ${
                      active ? "border-emerald-500/40 bg-emerald-500/10" : "border-slate-800 bg-slate-950/60"
                    }`}
                  >
                    <div>
                      <p className="text-xs text-slate-400">{device.label}</p>
                      <p className="text-lg font-semibold text-white">{device.nominal} W</p>
                    </div>
                    <span className={`rounded-full px-4 py-1 text-xs font-semibold ${active ? "bg-emerald-400 text-slate-900" : "bg-slate-800 text-slate-400"}`}>
                      {active ? "ON" : "OFF"}
                    </span>
                  </motion.div>
                );
              })}
            </div>
          </div>
        </section>

        <section className="mt-6 grid gap-6 lg:grid-cols-3">
          <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-6 lg:col-span-2">
            <div className="mb-4 flex items-center justify-between">
              <p className="text-sm font-semibold text-slate-200">Class probabilities</p>
              <p className="text-xs text-slate-500">Top {probabilityRows.length} hypotheses</p>
            </div>
            <div className="space-y-3">
              {probabilityRows.map((row) => (
                <div key={row.label} className="flex items-center gap-3">
                  <div className="w-28 text-xs font-medium text-slate-400">{LABEL_SUMMARY[row.label] ?? `Class ${row.label}`}</div>
                  <div className="h-2 flex-1 rounded-full bg-slate-800">
                    <div className="h-full rounded-full bg-sky-400" style={{ width: `${(row.probability * 100).toFixed(1)}%` }} />
                  </div>
                  <div className="w-16 text-right text-sm font-semibold text-white">{(row.probability * 100).toFixed(1)}%</div>
                </div>
              ))}
              {!probabilityRows.length && <p className="text-sm text-slate-500">Waiting for inference...</p>}
            </div>
          </div>

          <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-6">
            <p className="mb-3 text-sm font-semibold text-slate-200">Raw payload</p>
            <div className="rounded-2xl border border-slate-800 bg-slate-950/80 p-4 text-xs text-slate-300">
              <pre>{latest ? JSON.stringify(latest.raw, null, 2) : "Awaiting first message..."}</pre>
            </div>
          </div>
        </section>

        <footer className="mt-10 text-xs text-slate-500">
          Last sample: {latest?.timestamp ?? latest?.raw?.timestamp ?? "n/a"} • Backend {status?.mqtt.connected ? "online" : "offline"}
        </footer>
      </div>
    </div>
  );
}
