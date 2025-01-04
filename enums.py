from enum import Enum

class OrderStatus(Enum):
    PENDING = 'Pending'
    PLACED = 'Placed'
    EXECUTED = 'Executed'
    CANCELLED = 'Cancelled'
    FAILED = 'Failed'

class OrderType(Enum):
    BUY = 'Buy'
    SELL = 'Sell'