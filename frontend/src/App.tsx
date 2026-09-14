import { ChaosPanel } from "./components/ChaosPanel";
import { Hero } from "./components/Hero";
import { Inspector } from "./components/Inspector";
import { Summary } from "./components/Summary";
import { Terminal } from "./components/Terminal";
import { Topology } from "./graph/Topology";
import { useLiveState } from "./useLiveState";
import "./styles.css";

export default function App() {
  const {
    cluster,
    jobs,
    events,
    selectedJob,
    setSelectedJobId,
    connected,
    summary,
    setSummary,
    toast,
    refresh,
  } = useLiveState();

  return (
    <div className="app">
      <Hero cluster={cluster} />
      <div className="main">
        <ChaosPanel
          cluster={cluster}
          jobs={jobs}
          onSubmitted={() => {
            void refresh();
          }}
          onSelectJob={setSelectedJobId}
        />
        <Topology cluster={cluster} jobs={jobs} events={events} onSelectJob={setSelectedJobId} />
        <div style={{ display: "flex", flexDirection: "column", gap: 12, minHeight: 0 }}>
          <Terminal events={events} />
          <Inspector job={selectedJob} />
        </div>
      </div>
      <footer className="footer panel">
        <span>
          <span className={connected ? "dot" : "dot off"}>●</span>{" "}
          {connected ? "EVENT BUS LIVE" : "RECONNECTING"}
        </span>
        <span>
          RabbitMQ {cluster.rabbitmq} · Redis {cluster.redis} · dup deliveries{" "}
          {cluster.metrics.jobs_redelivered_total ?? 0} · dup side effects{" "}
          {cluster.metrics.duplicate_side_effects ?? 0}
        </span>
        <span>FTQ CONTROL ROOM</span>
      </footer>
      {toast && <div className="toast">{toast}</div>}
      {summary && cluster.chaos?.status === "complete" && (
        <Summary summary={summary} onClose={() => setSummary(null)} />
      )}
    </div>
  );
}
