class RateLimitError(Exception):
    def __init__(self, message='The current IP is rate limited.'):
        self.message = message
        super().__init__(self.message)


class CompanyNotFound(Exception):
    def __init__(self, business_id):
        self.message = f'{business_id} was not found.'
        super().__init__(self.message)
