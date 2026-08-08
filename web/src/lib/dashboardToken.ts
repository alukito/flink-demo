export interface LegacyDashboardTokenStorage {
  removeItem(key: string): void;
}

export type DashboardSessionCreator = (
  name: string,
  role: 'dashboard',
) => Promise<{ token: string }>;

export async function requestFreshDashboardToken(
  storage: LegacyDashboardTokenStorage,
  createDashboardSession: DashboardSessionCreator,
  name: string,
): Promise<string> {
  storage.removeItem('dash_token');
  const response = await createDashboardSession(name, 'dashboard');
  return response.token;
}
