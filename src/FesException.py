class FesException(Exception):
    def __init__(self, value):
        self.value = value
        super(FesException, self).__init__()
    def __str__(self):
        return repr(self.value)
