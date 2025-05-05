"""Datetime helper functions."""

from datetime import datetime, timedelta
from typing import List

def go_days_back(d: datetime, n: int) -> datetime:
    """Get the day n days ago."""
    return d - timedelta(days=n)

def go_business_days_back(d: datetime, n: int) -> datetime:
    """Get the business day n business days ago."""
    i = 0
    while i < n:
        d = d - timedelta(days=1)
        if d.weekday() not in (5, 6):
            i += 1
    return d


def get_last_business_days(d: datetime, n: int) -> List[datetime]:
    """Get the last n business days."""
    res = []
    for i in range(0, n + 1):
        res.append(go_business_days_back(d, i))
    return res
