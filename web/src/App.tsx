import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { SessionProvider } from './context/SessionContext';
import { EventProvider } from './context/EventContext';
import Landing from './pages/Landing';
import Seller from './pages/Seller';
import Buyer from './pages/Buyer';
import Shipper from './pages/Shipper';
import Dashboard from './pages/Dashboard';
import RequireRole from './components/RequireRole';

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
            <Route path="/dashboard" element={<Dashboard />} />
          </Routes>
        </BrowserRouter>
      </EventProvider>
    </SessionProvider>
  );
}

export default App;
