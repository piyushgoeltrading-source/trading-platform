# app/brokers/nuvama/__init__.py
# Nuvama broker package — exports the concrete broker class for use by BrokerFactory.
from app.brokers.nuvama.client import NuvamaBroker

__all__ = ["NuvamaBroker"]
