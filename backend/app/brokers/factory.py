"""
app/brokers/factory.py

BrokerFactory — PiyushTrade
============================
Returns the correct BaseBroker subclass instance for a given User based on
user.broker (BrokerName enum).

Rules enforced here:
  - Only this module decides which concrete broker class to instantiate.
  - Importing concrete broker classes is deferred to inside get() to avoid
    circular imports as zerodha/ and nuvama/ grow.
  - Raises BrokerConfigError (ValueError subclass) for unknown / unconfigured
    broker values — never returns None.
  - Zero live-order logic lives here. This is pure routing.
  - get() is synchronous; callers running in async context should use
    asyncio.to_thread() or FastAPI's run_in_executor at the API layer.

Usage:
    from app.brokers.factory import BrokerFactory

    broker = BrokerFactory.get(current_user)
    result = broker.place_order(order_request)
"""

from __future__ import annotations

from app.core.logging import get_structured_logger
from app.models.user import BrokerName, User

logger = get_structured_logger(__name__)


class BrokerConfigError(ValueError):
    """
    Raised when a user's broker field has no registered implementation.

    Inherits ValueError so callers can catch it broadly if needed, while
    still being distinguishable from generic ValueErrors.
    """


class BrokerFactory:
    """
    Static factory — no instantiation required.

    Concrete broker classes are imported lazily inside get() so that missing
    optional dependencies (kiteconnect, APIConnect) only raise ImportError
    when the relevant broker is actually requested, not at startup.
    """

    @staticmethod
    def get(user: User):
        """
        Return the broker implementation for the given user.

        Args:
            user: Authenticated User ORM instance. Must have a non-None
                  ``broker`` field (BrokerName enum value).

        Returns:
            An instance of a BaseBroker subclass (ZerodhaBroker or
            NuvamaBroker) initialised for this user.

        Raises:
            BrokerConfigError: user.broker is None or not a supported value.
            ImportError:        Broker SDK not installed (e.g. kiteconnect,
                                APIConnect).
        """
        broker_name: BrokerName | None = user.broker

        if broker_name is None:
            logger.error(
                "broker_factory_no_broker_set",
                extra={"user_id": user.id, "email": user.email},
            )
            raise BrokerConfigError(
                f"User {user.id} has no broker configured. "
                "Set user.broker before placing orders."
            )

        if broker_name == BrokerName.zerodha:
            # Deferred import — kiteconnect is an optional dependency
            from app.brokers.zerodha.client import ZerodhaBroker  # noqa: PLC0415

            logger.info(
                "broker_factory_resolved",
                extra={"user_id": user.id, "broker": BrokerName.zerodha.value},
            )
            return ZerodhaBroker(user_id=user.id)

        if broker_name == BrokerName.nuvama:
            # Deferred import — APIConnect==2.0.0 is an optional dependency
            from app.brokers.nuvama.client import NuvamaBroker  # noqa: PLC0415

            logger.info(
                "broker_factory_resolved",
                extra={"user_id": user.id, "broker": BrokerName.nuvama.value},
            )
            return NuvamaBroker(user_id=user.id)

        # Future brokers (e.g. BrokerName.kotak) get added here as new elif
        # blocks before this guard.
        logger.error(
            "broker_factory_unsupported_broker",
            extra={"user_id": user.id, "broker": str(broker_name)},
        )
        raise BrokerConfigError(
            f"Broker '{broker_name}' is not yet implemented. "
            "Register it in BrokerFactory.get() and add a concrete subclass."
        )
