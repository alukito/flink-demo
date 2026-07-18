import { useNavigate } from 'react-router-dom';
import { useSession } from '../context/SessionContext';

export default function Shipper() {
  const { name, clearSession } = useSession();
  const navigate = useNavigate();

  const handleLogout = () => {
    clearSession();
    navigate('/');
  };

  if (!name) {
    navigate('/');
    return null;
  }

  return (
    <div style={{ padding: '20px' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h1>Shipper: {name}</h1>
        <button onClick={handleLogout}>Logout</button>
      </div>
      <p>Job board will be implemented in Phase 2.</p>
    </div>
  );
}
