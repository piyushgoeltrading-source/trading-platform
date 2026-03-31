# app/brokers/zerodha/__init__.py
# Zerodha broker package — exports the concrete broker class for use by BrokerFactory.
from app.brokers.zerodha.client import ZerodhaBroker

__all__ = ["ZerodhaBroker"]
