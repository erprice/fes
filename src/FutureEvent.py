class FutureEvent:
    def __init__(self, id_, payload, expiration):
        self.id_ = id_
        self.payload = payload
        self.expiration = expiration

    def __repr__(self):
        return '%s: {"id_" : %s, "expiration" : %s, "payload" : %s}' % (
            self.__class__.__name__, self.id_, self.expiration, self.payload)
