

# TODO: Message design
class ShowroomMessage(object):
    def __init__(self, ident, query, *, content: dict=None):
        self._query = query
        self._ident = ident
        self._content = content

    @property
    def query(self):
        return self._query

    # TODO: settle on a name for this before using it anywhere
    @property
    def ident(self):
        return self._ident

    @property
    def content(self):
        return self._content

    def set_content(self, new_content):
        # TODO: content validation?
        self._content = new_content

    def json(self):
        # content is already a dict
        # although some items might not be, e.g. datetime
        # this exists basically to mimic requests Response
        # TODO: handle datetime and other objects json can't
        # though that wouldn't happen here
        return self._content

