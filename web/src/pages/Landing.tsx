import { useRef, useState, type FormEvent } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { createSession, type SessionResponse } from '../api/client';
import { useSession, type Role } from '../context/SessionContext';

const roles: { role: Role; label: string; description: string }[] = [
  { role: 'buyer', label: 'Shop as buyer', description: 'Browse, add, and place an order.' },
  { role: 'seller', label: 'Sell products', description: 'Add products and confirm orders.' },
  { role: 'shipper', label: 'Deliver orders', description: 'Pick jobs and complete delivery.' },
];

export default function Landing() {
  const [name, setName] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const nameInputRef = useRef<HTMLInputElement>(null);
  const navigate = useNavigate();
  const { setSession } = useSession();

  const handleRoleSelect = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const submitter = (event.nativeEvent as SubmitEvent).submitter as HTMLButtonElement | null;
    const role = submitter?.dataset.role as Role | undefined;
    if (!role) return;

    if (!name.trim()) {
      setError('Enter your display name to continue.');
      nameInputRef.current?.focus();
      return;
    }
    setLoading(true);
    setError('');
    try {
      const resp: SessionResponse = await createSession(name.trim(), role);
      setSession(resp.id, resp.token, resp.name, role);
      navigate(`/${role}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create session');
    } finally {
      setLoading(false);
    }
  };

  return (
    <main className="landing">
      <div className="landing__intro">
        <span className="landing__eyebrow">Market signal demo</span>
        <h1>Make the stream move.</h1>
        <p>Participant actions become live stream events for everyone to see.</p>
      </div>

      <form className="landing__form" onSubmit={handleRoleSelect} noValidate>
        <div className="landing__name-field">
          <label htmlFor="display-name">Display name</label>
          <input
            ref={nameInputRef}
            id="display-name"
            name="displayName"
            type="text"
            autoComplete="name"
            value={name}
            onChange={(event) => {
              setName(event.target.value);
              if (error === 'Enter your display name to continue.') setError('');
            }}
            placeholder="How others will see you"
            aria-describedby={error ? 'landing-error' : undefined}
            aria-invalid={Boolean(error)}
            disabled={loading}
          />
        </div>

        {error && <p id="landing-error" className="landing__error" role="alert">{error}</p>}

        <fieldset className="landing__roles" disabled={loading}>
          <legend>Choose how you want to join</legend>
          <div className="landing__role-grid">
            {roles.map(({ role, label, description }) => (
              <button
                key={role}
                className="button button--primary landing-role-card"
                type="submit"
                data-role={role}
              >
                <span className="landing-role-card__label">{label}</span>
                <span className="landing-role-card__description">{description}</span>
              </button>
            ))}
          </div>
        </fieldset>
      </form>

      <div className="landing__presenter">
        <span>Presenting the demo?</span>
        <Link className="landing-dashboard-link" to="/dashboard">Open presenter dashboard</Link>
      </div>
    </main>
  );
}
