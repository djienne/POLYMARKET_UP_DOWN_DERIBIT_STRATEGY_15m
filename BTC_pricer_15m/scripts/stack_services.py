"""Service names and dependencies shared by the stack's operations tools."""

SERVICE_CONTAINERS = {
    "redis": "btc_pricer_15m_redis",
    "spot": "btc_pricer_15m_spot",
    "calc": "btc_pricer_15m_calc",
    "grid": "btc_pricer_15m_grid",
    "live": "btc_pricer_15m_live",
    "docker-logs": "polymarket_15m_docker_logs",
    "dashboard": "polymarket_dashboard_15m",
    "watchdog": "btc_pricer_15m_watchdog",
}
STACK_SERVICES = tuple(SERVICE_CONTAINERS)
STOP_SERVICES = ("watchdog", "live", "dashboard", "docker-logs", "grid", "calc", "spot", "redis")
SERVICE_DEPENDENCIES = {
    "redis": (),
    "spot": ("redis",),
    "calc": ("redis", "spot"),
    "grid": ("redis", "spot", "calc"),
    "live": ("redis", "spot", "calc"),
    "docker-logs": (),
    "dashboard": ("redis", "spot", "docker-logs"),
    "watchdog": (),
}
