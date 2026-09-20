export function dashboardToken(): string {
  const urlToken = new URLSearchParams(window.location.search).get("token");
  if (urlToken) {
    window.localStorage.setItem("dashboardApiToken", urlToken);
    return urlToken;
  }
  return (
    window.localStorage.getItem("dashboardApiToken") ||
    (import.meta as unknown as { env?: { VITE_DASHBOARD_API_TOKEN?: string } }).env
      ?.VITE_DASHBOARD_API_TOKEN ||
    ""
  );
}

export function tokenQuery(): string {
  const token = dashboardToken();
  return token ? `token=${encodeURIComponent(token)}` : "";
}
