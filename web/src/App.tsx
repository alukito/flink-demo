import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { SessionProvider } from './context/SessionContext';
import { EventProvider } from './context/EventContext';
import Landing from './pages/Landing';
import Seller from './pages/Seller';
import Buyer from './pages/Buyer';
import Shipper from './pages/Shipper';
import Dashboard from './pages/Dashboard';

function App() {
  return (
    <SessionProvider>
      <EventProvider>
        <BrowserRouter>
          <Routes>
            <Route path="/" element={<Landing />} />
            <Route path="/seller" element={<Seller />} />
            <Route path="/buyer" element={<Buyer />} />
            <Route path="/shipper" element={<Shipper />} />
            <Route path="/dashboard" element={<Dashboard />} />
          </Routes>
        </BrowserRouter>
      </EventProvider>
    </SessionProvider>
  );
}

export default App;
