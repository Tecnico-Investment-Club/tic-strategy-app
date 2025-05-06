"""Entity types."""

from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Tuple, Union

Record = Union[Tuple, List]
File = List[Record]

Key = Tuple
Keys = List[Key]

Message = Dict[str, Union[int, str, bool, datetime, Decimal, None]]
