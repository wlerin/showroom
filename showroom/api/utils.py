import re

_csrf_re = re.compile("SrGlobal.csrfToken = \\'([\\w\\d-]+)\\';")


def get_csrf_token(text):
    m = _csrf_re.search(text)
    if m:
        return m.group(1)
    else:
        # TODO: error
        return
