class RateProcessor(object):
    def __init__(self, qty_col: str = None, amount_col: str = None):
        self.qty_col = qty_col
        self.amount_col = amount_col

    def run(self, qty: float, amount: float) -> float:
        rate = amount / qty if qty > 0 else float("NaN")
        return rate
