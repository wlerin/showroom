
class ShowroomException(Exception):
    pass


class ShowroomStopRequest(ShowroomException):
    pass


class ShowroomDownloadError(ShowroomException):
    def __init__(self, process=None):
        super().__init__()
        self.process = process
