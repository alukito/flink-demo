import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { createSession, type SessionResponse } from '../api/client';
import { useSession, type Role } from '../context/SessionContext';

export default function Landing() {
  const [name, setName] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();
  const { setSession } = useSession();

  const handleRoleSelect = async (role: Role) => {
    if (!name.trim()) {
      setError('Please enter your name first');
      return;
    }
    setLoading(true);
    setError('');
    try {
      const resp: SessionResponse = await createSession(name.trim(), role);
      setSession(resp.token, resp.name, role);
      navigate(`/${role}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create session');
    } finally {
      setLoading(false);
    }
  };

  const handleDashboard = () => {
    navigate('/dashboard');
  };

  return (
    <div style={{ maxWidth: '600px', margin: '100px auto', textAlign: 'center' }}>
      <h1>Stream Processing Demo</h1>
      <p>E-commerce simulation with 3 levels of stream processing</p>

      <div style={{ margin: '40px 0' }}>
        <input
          type="text"
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="Enter your display name"
          style={{ padding: '12px', fontSize: '16px', width: '300px' }}
          disabled={loading}
        />
      </div>

      {error && <p style={{ color: 'red' }}>{error}</p>}

      <div style={{ display: 'flex', gap: '16px', justifyContent: 'center' }}>
        <button
          onClick={() => handleRoleSelect('buyer')}
          disabled={loading}
          style={{ padding: '16px 32px', fontSize: '16px', cursor: 'pointer' }}
        >
          Buyer
        </button>
        <button
          onClick={() => handleRoleSelect('seller')}
          disabled={loading}
          style={{ padding: '16px 32px', fontSize: '16px', cursor: 'pointer' }}
        >
          Seller
        </button>
        <button
          onClick={() => handleRoleSelect('shipper')}
          disabled={loading}
          style={{ padding: '16px 32px', fontSize: '16px', cursor: 'pointer' }}
        >
          Shipper
        </button>
      </div>

      <div style={{ marginTop: '40px' }}>
        <button
          onClick={handleDashboard}
          style={{ padding: '12px 24px', fontSize: '14px', cursor: 'pointer' }}
        >
          View Dashboard (no login needed)
        </button>
      </div>
    </div>
  );
}
