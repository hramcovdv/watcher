""" Series Helper mode for InluxDB
"""
from typing import Any, Text, List, Dict, Optional, Iterator, Union
from datetime import datetime


class SeriesHelper():
    """ SeriesHelper class
    """
    def __init__(self, series: Text, tags: Optional[List] = None) -> None:
        """ Create Helper
        """
        self.points: List[Dict[Text, Any]] = []
        self.series: Text = series
        self.tags: List = tags or []

    def __iter__(self) -> Iterator:
        return iter(self.points)

    def add_point(self, _data_: Dict) -> None:
        """ Add single series point
        """
        current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        point_fields = {
            k: to_num(v)
            for k, v in _data_.items() if k not in self.tags
        }

        point_tags = {
            k: to_str(v)
            for k, v in _data_.items() if k in self.tags
        }

        point = {
            'measurement': self.series,
            'fields': point_fields,
            'tags': point_tags,
            'time': current_time
        }

        self.points.append(point)

    def add_points(self, _data_: List[Dict]) -> None:
        """ Add multiply series points
        """
        for item in _data_:
            self.add_point(item)


def to_str(value: Union[None, str, float, int, bytes]) -> Union[str]:
    """ Return string value
    """
    if isinstance(value, bytes):
        return value.decode() if value.isascii() else value.hex()

    return str(value)


def to_num(value: Union[None, str, float, int, bytes]) -> Union[float, int]:
    """ Return float or integer value
    """
    if isinstance(value, (float, int)):
        return value

    return float(value) if '.' in to_str(value) else int(value)
