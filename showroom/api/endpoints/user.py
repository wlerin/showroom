

class UserEndpointsMixin:
    # no auth needed
    def user_profile(self, user_id, room_id=None):
        """
        Can be called with just user_id, room_id only seems to affect the url field
        
        :param user_id: 
        :param room_id: 
        :return: 
        """
        endpoint = "/api/user/profile"
        result = self._api_get(endpoint, params={"user_id": user_id, "room_id": room_id})
        return result

    # auth required for meaningful result
    def is_birthday_registered(self):
        """
        Whether the currently logged in user has registered their birthday
        
        Returns true without auth
        :return: 
        """
        endpoint = "/api/user/is_birthday_registered"
        result = self._api_get(endpoint)
        return result.get('is_birthday_registered')

    # auth required
    def account_info(self):
        """
        Returns data about currently logged in user's account
        
        Requires auth, otherwise it 404s
        :return: 
        """
        endpoint = "/api/account/"
        result = self._api_get(endpoint)
        return result

    # post methods, none of these are truly working
    def update_gift_use_flag(self, flag_type, flag):
        """
        No idea what this does.
        
        :param flag_type: 
        :param flag: 
        :return: 
        """
        endpoint = "/api/user/update_gift_use_flg"
        result = self._api_post(endpoint, data=dict(type=flag_type, flg=flag))
        return result

    def register_birthday(self, year, month, day):
        """
        Registers currently logged in user's birthday
        
        Shouldn't return anything, but what happens if it has already been registered?
        
        :param year: 
        :param month: 
        :param day: 
        :return: 
        """
        endpoint = "/api/user/register_birthday"
        self._api_post(endpoint, data=dict(year=year, month=month, day=day))

    # authenticate
    def login(self, username, password, captcha_word=None, csrf_token=None):
        """
        Not implemented.
        
        Requires entering a captcha.
        
        :param username: 
        :param password: 
        :param captcha_word: 
        :param csrf_token: 
        :return: 
        """
        endpoint = "/user/login"
        # TODO: probably need to fetch the login page to load the captcha and get the csrf token
        result = self._api_post(endpoint, data=dict(
            account_id=username,
            password=password,
            captcha_word=captcha_word,
            csrf_token=csrf_token
        ))
        return result

