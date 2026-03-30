from app.brokers.base_broker import BaseBroker


class ZerodhaBroker(BaseBroker):
    def place_order(self, order_request):
        raise NotImplementedError

    def get_order_status(self, broker_order_id: str):
        raise NotImplementedError

    def cancel_order(self, broker_order_id: str):
        raise NotImplementedError