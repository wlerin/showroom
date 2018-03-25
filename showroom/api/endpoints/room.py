

class RoomEndpointsMixin:
    """
    For endpoints in */api/room/* and */room/*
    
    As well as */api/event/* and */api/support/* since there's only one each
    and they go with event_and_support which is already here
    """
    # TODO: find more event endpoints, surely they exist?
    # e.g. there is /event/room_list?event_id={}, but this is very messy
    def event_and_support(self, room_id):
        """
        Gets support and event info for a room
        
        Result has three top fields:
            support
            regular_event
            event
            
        support is either none or has the following fields:
            is_achieved: bool
            goal_point: int
            tutorial_url: url  # "https://www.showroom-live.com/room/tutorial"
            title: str
            support_id: int
            next_level: int
            current_point: int
        
        regular_event is either null or ?
        
        event is either null or has the following fields:
            ranking: dict  # only for ranking events?
            event_name: str
            event_id: int
            event_description: str
            event_type: str  # TODO: list possible event types
            image: url
            ended_at: timestamp  # time the event ends, in UTC
            started_at: timestamp  # time the event started, in UTC
            tutorial_url: url  # "https://www.showroom-live.com/room/tutorial"
            event_url: url
            
        known event types:
            ranking
        
        event.ranking has the following fields:
            gap: int   # points between this room and the next
            point: int
            text: str
            next_rank: int
            rank: int
        
        :param room_id: 
        :return: 
        """
        endpoint = "/api/room/event_and_support"
        results = self._api_get(endpoint, params={"room_id": room_id})
        return results

    def contribution_ranking(self, room_id, event_id):
        endpoint = "/api/event/contribution_ranking"
        result = self._api_get(endpoint, params=dict(room_id=room_id, event_id=event_id))
        # TODO: find example responses
        return result

    def support_users(self, room_id, support_id):
        endpoint = "/api/support/support_users"
        result = self._api_get(endpoint, params=dict(room_id=room_id, support_id=support_id))
        # TODO: find example responses
        return result

    def next_live(self, room_id):
        """
        Get the time of the next scheduled live
        
        :param room_id: 
        :return: UTC epoch time of the next live
        """
        # TODO: return datetime object instead?
        # or just return the result as is?
        endpoint = "/api/room/next_live"
        results = self._api_get(endpoint, params={"room_id": room_id})
        return results.get('epoch')

    def is_live(self, room_id):
        """
        Checks if a room is live
        
        :param room_id: 
        :return: bool
        """
        # WARNING: watch this endpoint for deprecation
        endpoint = "/room/is_live"
        result = self._api_get(endpoint, params={"room_id": room_id}, default={'ok': 0})
        return bool(result["ok"])

    def profile(self, room_id):
        """
        Get profile info for a room
        
        :param room_id: 
        :return: 
        """
        endpoint = "/api/room/profile"
        result = self._api_get(endpoint, params={"room_id": room_id})
        return result

    def follow(self, room_id, flag=None):
        """
        Requires auth
        :param room_id: 
        :param flag: 
        :return: 
        """
        if not self.is_authenticated: return
        endpoint = "/api/room/follow"
        # TODO: what does flag do? what does this return?
        response = self._api_post(endpoint, data={"room_id": room_id, "flag": flag})
        raise NotImplementedError
