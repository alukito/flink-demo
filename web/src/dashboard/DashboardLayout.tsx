import { useEffect, useMemo, useState } from 'react';
import { Link, Outlet, useLocation } from 'react-router-dom';
import { Button } from '../components/ui/Button';
import { SignalTrace } from '../components/ui/SignalTrace';
import { useDashboard } from './DashboardContext';
import { DASHBOARD_STEPS, dashboardAdjacentPath } from './dashboardRoutes';

function connectionLabel(connectionState: ReturnType<typeof useDashboard>['connectionState']): string {
  switch (connectionState) {
    case 'live':
      return 'Live';
    case 'reconnecting':
      return 'Reconnecting…';
    case 'connecting':
      return 'Connecting…';
  }
}

function wibTime(now: Date): string {
  return new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Jakarta',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(now);
}

export function DashboardLayout() {
  const { clearAll, connectionState, events = [] } = useDashboard();
  const { pathname } = useLocation();
  const [now, setNow] = useState(() => new Date());
  const [confirmingClear, setConfirmingClear] = useState(false);
  const currentStep = useMemo(
    () => DASHBOARD_STEPS.find((step) => step.path === pathname),
    [pathname],
  );
  const previousPath = dashboardAdjacentPath(pathname, -1);
  const nextPath = dashboardAdjacentPath(pathname, 1);

  useEffect(() => {
    const timer = window.setInterval(() => setNow(new Date()), 1_000);
    return () => window.clearInterval(timer);
  }, []);

  const confirmClear = () => {
    clearAll();
    setConfirmingClear(false);
  };

  return (
    <div className="dashboard-shell">
      <header className="dashboard-shell__header">
        <div className="dashboard-shell__identity">
          <span className="dashboard-shell__kicker">Flink demo</span>
          <h1>Stream Processing Dashboard</h1>
        </div>
        <div className="dashboard-shell__status">
          <span className="dashboard-connection" data-state={connectionState} role="status">
            <span className="dashboard-connection__dot" aria-hidden="true" />
            {connectionLabel(connectionState)} connection
          </span>
          <SignalTrace pulseKey={events.length} />
          <time dateTime={now.toISOString()} className="dashboard-clock">
            {wibTime(now)} WIB
          </time>
          <Button variant="secondary" onClick={() => setConfirmingClear(true)}>Clear dashboard</Button>
        </div>
      </header>

      <div className="dashboard-shell__body">
        <nav className="dashboard-steps" aria-label="Dashboard levels">
          {DASHBOARD_STEPS.map((step) => (
            <Link
              className="dashboard-steps__link"
              key={step.path}
              to={step.path}
              aria-current={step.path === pathname ? 'page' : undefined}
            >
              <span className="dashboard-steps__number">{step.number}</span>
              <span>
                <span className="dashboard-steps__label">{step.shortLabel}</span>
                <span className="dashboard-steps__eyebrow">{step.eyebrow}</span>
              </span>
            </Link>
          ))}
        </nav>

        <main className="dashboard-canvas">
          {currentStep && <p className="dashboard-canvas__eyebrow">{currentStep.eyebrow}</p>}
          <Outlet />
        </main>
      </div>

      <nav className="dashboard-adjacent" aria-label="Adjacent dashboard levels">
        {previousPath ? <Link className="dashboard-adjacent__link" to={previousPath}>Previous level</Link> : <span />}
        {nextPath ? <Link className="dashboard-adjacent__link" to={nextPath}>Next level</Link> : <span />}
      </nav>

      {confirmingClear && (
        <div className="dashboard-clear-dialog__backdrop">
          <section className="dashboard-clear-dialog" role="dialog" aria-modal="true" aria-labelledby="dashboard-clear-title">
            <h2 id="dashboard-clear-title">Clear dashboard data?</h2>
            <p>This resets the live events, window metrics, and pattern signals across all three dashboard levels.</p>
            <div className="dashboard-clear-dialog__actions">
              <Button autoFocus variant="secondary" onClick={() => setConfirmingClear(false)}>Cancel</Button>
              <Button variant="danger" onClick={confirmClear}>Clear dashboard</Button>
            </div>
          </section>
        </div>
      )}
    </div>
  );
}
