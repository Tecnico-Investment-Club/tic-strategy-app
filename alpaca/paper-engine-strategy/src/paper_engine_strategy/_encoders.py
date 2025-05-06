"""Type encoders for messages and databases."""

from datetime import date, datetime
from decimal import Decimal
import json
from typing import Any, Optional, Union


class MessagesEncoder(json.JSONEncoder):
    """Encodes json to decimal or string."""

    def default(self, obj: Any) -> Union[float, str, json.JSONEncoder]:
        """Casts object into decimal or string."""
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


def cast_money(s: str, cur: Any) -> Optional[Decimal]:
    """Casts money type to decimal."""
    if s is None:
        return None
    return Decimal(s.replace(",", "").replace("$", ""))  # Assume known locale


def quantize(value: Optional[Decimal], n_places: int = 6) -> Optional[Decimal]:
    """Rounds value given."""
    decimal_places = Decimal("1." + "0" * n_places)  # decimal places

    if isinstance(value, Decimal):
        return value.quantize(decimal_places)

    return None


def str_to_dt(dt: str) -> Optional[datetime]:
    """Converts string into datetime object."""
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(dt, fmt)
        except ValueError:
            pass

    return None


def dt_to_str(dt: datetime) -> Optional[str]:
    """Converts datetime into string object."""
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strftime(dt, fmt)
        except ValueError:
            pass

    return None
