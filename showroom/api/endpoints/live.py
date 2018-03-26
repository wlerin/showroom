# TODO: should these return the full dict? i.e. not get() the inner data when there's only one top field
# doing so at least makes the return type consistent (always dict, unless it's like, is_live)

class LiveEndpointsMixin:
    """
    For endpoints in */api/live/*
    """
    def bradaru_texts(self, room_id):
        """
        Returns a list of canned responses, e.g.:
        [
            {"text":"Bravo time will end in 30 seconds!","id":1}, 
            {"text":"How did you like the performance?","id":2}, ...
        ]
        
        :param room_id: numerical room_id (either as a str or an int)
        :return: 
        """
        endpoint = "/api/live/bradaru_texts"
        results = self._api_get(endpoint, params={"room_id": room_id})
        return results.get('texts')

    def comment_log(self, room_id, is_delay=None):
        """
        Get a live room's comment log
        
        :param room_id: 
        :param is_delay: 
        :return: a list of comment dicts
        """
        endpoint = "/api/live/comment_log"
        # TODO: test whether is_delay is valid (should be an int, not sure otherwise)
        # default is {"comment_log":[]}, if this returns None there was an error
        results = self._api_get(endpoint, params={"room_id": room_id, "is_delay": is_delay})
        return results.get('comment_log')

    def onlives(self):
        endpoint = "/api/live/onlives"
        results = self._api_get(endpoint)
        # TODO: helper method to make this simpler to use?
        return results.get("onlives")

    def onlive_num(self):
        endpoint = "/api/live/onlive_num"
        results = self._api_get(endpoint)
        return results.get("num")

    def telop(self, room_id):
        """
        Get a live room's current telop
        
        :param room_id: 
        :return: the telop as a string
        """
        endpoint = "/api/live/telop"
        results = self._api_get(endpoint, params={"room_id": room_id})
        return results.get('telop')

    def streaming_url(self, room_id):
        endpoint = "/api/live/streaming_url"
        results = self._api_get(endpoint, params={"room_id": room_id})
        return results.get('streaming_url_list')

    def current_user(self, room_id):
        endpoint = "/api/live/current_user"
        results = self._api_get(endpoint, params={"room_id": room_id})
        return results.get('streaming_url_list')

    def gift_list(self, room_id):
        endpoint = "/api/live/gift_list"
        results = self._api_get(endpoint, params={"room_id": room_id})
        # TODO: are there other names for the top field? e.g. performance_time, enquete_time
        return results.get("bravo_time")

    def gift_log(self, room_id):
        endpoint = "/api/live/gift_log"
        results = self._api_get(endpoint, params={"room_id": room_id})
        # TODO: are there other names for the top field? e.g. performance_time, enquete_time
        return results.get("gift_log")

    def summary_ranking(self, room_id):
        endpoint = "/api/live/summary_ranking"
        results = self._api_get(endpoint, params={"room_id": room_id})
        # TODO: are there other names for the top field? e.g. performance_time, enquete_time
        return results.get("ranking")

    def live_info(self, room_id):
        endpoint = "/api/live/live_info"
        results = self._api_get(endpoint, params={"room_id": room_id})
        # TODO: helper methods to get specific fields from this
        # TODO: document fields, e.g. what do the different values of live_status mean
        return results

    def stage_user_list(self, room_id):
        endpoint = "/api/live/stage_user_list"
        results = self._api_get(endpoint, params={"room_id": room_id})
        return results.get('stage_user_list')

    def stage_gift_list(self, room_id):
        endpoint = "/api/live/stage_gift_list"
        results = self._api_get(endpoint, params={"room_id": room_id})
        return results.get('stage_gift_list')

    def polling(self, room_id):
        endpoint = "/api/live/polling"
        results = self._api_get(endpoint, params={"room_id": room_id})
        return results.get('stage_gift_list')

    def enquete_result(self, room_id):
        endpoint = "/api/live/enquete_result"
        results = self._api_get(endpoint, params={"room_id": room_id})
        # TODO: make this human readable (need to find a recent poll)
        # {"l":[],"v":"v1","i":"https://image.showroom-live.com/showroom-prod/assets/img/gift"}
        return results

    def poll_result(self, room_id):
        return self.enquete_result(room_id)

    def upcoming(self, genre_id):
        # TODO: helper methods for getting specific genre
        endpoint = "/api/live/upcoming"
        results = self._api_get(endpoint, params={"genre_id": genre_id})
        return results.get('upcomings')

    def verify_age(self):
        # does this require auth?
        endpoint = "/api/live/age_verification"
        result = self._api_post(endpoint, data=dict(
            room_id=int,
            year=int,
            month=int,
            day=int,
            csrf_token=self.csrf_token
        ))
        return result

    def send_free_gift(self, gift_id, live_id, num, is_delay=None, latitude=None, longitude=None, radius=None):
        """
        Requires auth
        
        valid free gift_ids = 1, 2, 1001, 1002, 1003
        
        TODO: are there other valid gift ids? e.g. for amateur rooms?
        :return: 
        """
        if not self.is_authenticated: return

        if int(gift_id) not in (1, 2, 1001, 1002, 1003):
            raise ValueError("Invalid Free Gift ID")
        if not (0 <= int(num) <= 10):
            raise ValueError("Invalid Free Gift Count")

        endpoint = "/api/live/gifting_free"
        result = self._api_post(endpoint, data=dict(
            gift_id=gift_id,
            live_id=live_id,
            num=num,
            is_delay=is_delay,
            lat=latitude,
            lon=longitude,
            rad=radius,
            csrf_token=self.csrf_token
        ))
        return result

    def send_paid_gift(self, gift_id, live_id, num, is_delay=None, latitude=None, longitude=None, radius=None):
        """
        Requires auth
        
        :return: 
        """
        if not self.is_authenticated: return
        endpoint = "/api/live/gifting_point_use"
        result = self._api_post(endpoint, data=dict(
            gift_id=gift_id,
            live_id=live_id,
            num=num,  # TODO: bounds check
            is_delay=is_delay,
            lat=latitude,
            lon=longitude,
            rad=radius,
            csrf_token=self.csrf_token
        ))
        return result

    def send_comment(self, live_id, comment, is_delay=None, latitude=None, longitude=None, radius=None):
        """
        Requires auth
        
        :return: 
        """
        if not self.is_authenticated: return
        endpoint = "/api/live/post_live_comment"

        result = self._api_post(endpoint, data=dict(
            live_id=live_id,
            comment=comment,  # TODO: character limit ?
            is_delay=is_delay,
            lat=latitude,
            lon=longitude,
            rad=radius,
            csrf_token=self.csrf_token
        ))
        return result
