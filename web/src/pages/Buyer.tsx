import { useNavigate } from 'react-router-dom';
import { useSession } from '../context/SessionContext';

export default function Buyer() {
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
        <h1>Buyer: {name}</h1>
        <button onClick={handleLogout}>Logout</button>
      </div>
      <p>Product catalog and cart will be implemented in Phase 2.</p>
    </div>
  );
}
