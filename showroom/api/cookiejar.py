from requests.cookies import RequestsCookieJar as _RequestsCookieJar
from http.cookiejar import Cookie as _Cookie

_cookie_attrs = dict(
    version=0, name="", value="",
    port=None, domain='', path='/',
    secure=False, expires=None, discard=False,
    comment=None, comment_url=None,
    rfc2109=False,
)
_bool_attrs = (
    ('port_specified', lambda c: bool(c['port'])),
    ('domain_specified', lambda c: bool(c['domain'])),
    ('domain_initial_dot', lambda c: c['domain'].startswith('.')),
    ('path_specified', lambda c: bool(c['path'])),
)


class ClientCookieJar(_RequestsCookieJar):
    """
    Custom CookieJar that can be saved to/loaded from dicts
    """
    # TODO: bring this into line with RequestsCookieJar (it was formerly inheriting http.cookielib.CookieJar)
    @staticmethod
    def cookie_to_dict(cookie):
        dct = {}
        for attr in _cookie_attrs:
            val = getattr(cookie, attr)
            if val == _cookie_attrs[attr]:
                # don't store default values
                continue
            dct[attr] = getattr(cookie, attr)
        if cookie._rest:
            dct['rest'] = cookie._rest
        return dct

    @staticmethod
    def cookie_from_dict(dct):
        """
        Constructs a cookie from a dict.

        Fills in any missing parameters from a set of defaults.

        This method was based on Requests' "create_cookie"
        function, originally written by Miguel Turner (dhagrow):
        https://github.com/dhagrow/requests/blame/develop/requests/packages/oreos/cookiejar.py#L126
        """
        if 'name' not in dct or 'value' not in dct:
            raise TypeError('Cookie dictionary must contain name and value')

        cookie_kwargs = _cookie_attrs.copy()
        cookie_kwargs['rest'] = {}

        extra_args = set(dct) - set(cookie_kwargs)
        if extra_args:
            err = 'Unexpected keys in Cookie dictionary: {}'
            raise TypeError(err.format(sorted(extra_args)))

        cookie_kwargs.update(dct)
        for key, func in _bool_attrs:
            cookie_kwargs[key] = func(cookie_kwargs)

        return _Cookie(**cookie_kwargs)

    def __init__(self, cookies=None, policy=None):
        _RequestsCookieJar.__init__(self, policy)
        if cookies:
            self.update(cookies)

    def update(self, cookies):
        """
        Updates from a dictionary of cookies, optionally nested by domain and/or path

        May raise TypeError if cookie_dict is invalid or contains unexpected keys
        """
        self._update(cookies, domain=None, path=None)

    def _update(self, cookies, domain=None, path=None):
        if not cookies:
            return

        for key in cookies:
            # will fail if path or cookie.name == 'name'
            # as is, this check allows mixed nesting
            # e.g. cookies and domains at the same level
            if 'name' not in cookies[key]:
                if domain is not None:
                    if path is not None:
                        err = 'No Cookies found in dictionary'
                        raise TypeError(err)
                    else:
                        self._update(cookies[key], domain=domain, path=key)
                else:
                    self._update(cookies[key], domain=domain)
            else:
                self.set_cookie(self.cookie_from_dict(cookies[key]))

    @property
    def expires_earliest(self):
        # if len(self) > 0:
        #     # sometimes a cookie has no expiration?
        #     return min([cookie.expires for cookie in self if cookie.expires])
        # return None
        # Compatibility Note: the default argument was added to min() in Python 3.4
        return min([(cookie.expires or 0) for cookie in self], default=None)

    def to_dict(self, ignore_domain=False, ignore_path=False):
        """
        Returns a dict representation of the CookieJar

        If more than one domain exists, or more than one path in
        each domain, cookies will be nested under their respective
        domain/path. Otherwise all cookies will be stored at the
        topmost level.

        Nesting can be disabled with ignore_domain and ignore_path

        Examples:

            One domain, one path:
            {
                cookie1.name: {key: val, ...},
                cookie2.name: {key: val, ...},
                ...
            }

            Multiple domains, one path per domain:
            {
                domain1: {
                    cookie1.name: {key: val, ...},
                    ...
                },
                domain2: {
                    cookie1.name: {key: val, ...},
                    ...
                },
                ...
            }

            One domain, multiple paths:
            {
                path1: {
                    cookie1.name: {key: val, ...},
                    ...
                },
                path2: {
                    cookie1.name: {key: val, ...},
                    ...
                },
                ...
            }

            Multiple domains, multiple paths per domain:
            {
                domain1: {
                    path1: {
                        cookie1.name: {key: val, ...},
                        ...
                    },
                    ...
                },
                ...
            }

        set_cookies_from_dict can handle any of the above variants.
        """
        target = cookie_dict = {}

        if not ignore_domain and len(self._cookies) > 1:
            nest_domain = True
        else:
            nest_domain = False

        for domain in self._cookies:
            if nest_domain:
                target = cookie_dict[domain] = {}

            if not ignore_path and len(self._cookies[domain]) > 1:
                nest_path = True
            else:
                nest_path = False

            for path in self._cookies[domain]:
                if nest_path:
                    target = target[path] = {}

                for name in self._cookies[domain][path]:
                    cookie = self._cookies[domain][path][name]
                    target[name] = self.cookie_to_dict(cookie)
        return cookie_dict
