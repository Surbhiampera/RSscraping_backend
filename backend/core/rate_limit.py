import time
from collections import defaultdict
from fastapi import HTTPException, Request


def rate_limit(max_calls: int, period_seconds: int):
    history: dict[str, list[float]] = defaultdict(list)

    async def _rate_limit_dep(request: Request):
        client_ip = request.client.host if request.client else "unknown"
        now = time.time()
        cutoff = now - period_seconds

        # Prune old entries
        history[client_ip] = [t for t in history[client_ip] if t > cutoff]

        if len(history[client_ip]) >= max_calls:
            raise HTTPException(
                status_code=429,
                detail="Too many requests. Try again later.",
            )

        history[client_ip].append(now)

    return _rate_limit_dep
