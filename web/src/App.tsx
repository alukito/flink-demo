import { BrowserRouter, Navigate, Routes, Route } from 'react-router-dom';
import { SessionProvider } from './context/SessionContext';
import { EventProvider } from './context/EventContext';
import Landing from './pages/Landing';
import Seller from './pages/Seller';
import Buyer from './pages/Buyer';
import Shipper from './pages/Shipper';
import RequireRole from './components/RequireRole';
import { DashboardProvider } from './dashboard/DashboardContext';
import { DashboardLayout } from './dashboard/DashboardLayout';
import { DashboardLivePage } from './pages/dashboard/DashboardLivePage';
import { DashboardWindowsPage } from './pages/dashboard/DashboardWindowsPage';
import { DashboardPatternsPage } from './pages/dashboard/DashboardPatternsPage';

function App() {
  return (
    <SessionProvider>
      <EventProvider>
        <BrowserRouter>
          <Routes>
            <Route path="/" element={<Landing />} />
            <Route path="/seller" element={<RequireRole role="seller"><Seller /></RequireRole>} />
            <Route path="/buyer" element={<RequireRole role="buyer"><Buyer /></RequireRole>} />
            <Route path="/shipper" element={<RequireRole role="shipper"><Shipper /></RequireRole>} />
            <Route path="/dashboard" element={<DashboardProvider><DashboardLayout /></DashboardProvider>}>
              <Route index element={<Navigate to="live" replace />} />
              <Route path="live" element={<DashboardLivePage />} />
              <Route path="windows" element={<DashboardWindowsPage />} />
              <Route path="patterns" element={<DashboardPatternsPage />} />
            </Route>
          </Routes>
        </BrowserRouter>
      </EventProvider>
    </SessionProvider>
  );
}

export default App;
