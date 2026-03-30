from abc import ABC, abstractmethod
from typing import Any


class BaseBroker(ABC):
    @abstractmethod
    def place_order(self, order_request: Any) -> Any:
        raise NotImplementedError

    @abstractmethod
    def get_order_status(self, broker_order_id: str) -> Any:
        raise NotImplementedError

    @abstractmethod
    def cancel_order(self, broker_order_id: str) -> Any:
        raise NotImplementedError