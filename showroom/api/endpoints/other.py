

class OtherEndpointsMixin:
    """
    Various endpoints dealing with the Showroom service and menus
    """
    def avatar_server_settings(self):
        """
        Some info about where avatars are stored, e.g.:
        
        {
            "version": 41,
            "path": "https://image.showroom-live.com/showroom-prod/image/avatar/",
            "f_ext": "png"
        }
        :return: 
        """
        endpoint = "/api/avatar/server_settings"
        result = self._api_get(endpoint)
        return result

    def radio_images(self):
        """
        A list of background images for radio broadcasts
        
        :return: 
        """
        endpoint = "/api/radio_images"
        result = self._api_get(endpoint)
        return result.get('radio_images')

    def service_settings(self):
        """
        Global(?) default settings for showroom streams
        
        Includes avatar_server_settings in the avatar_url field        
        :return: 
        """
        endpoint = "/api/service_settings/"
        result = self._api_get(endpoint)
        return result

    def side_navigation_menu(self):
        """
        Gets contents of the side navigation menu, including language specific labels
        
        Three fields, the main one of interest is menu_list
        
        :return: 
        """
        endpoint = "/api/menu/side_navi"
        result = self._api_get(endpoint)
        return result

    def broadcast_menu(self):
        """
        No idea. Just returns {"menu_list":[]} for me. Maybe only returns something if you are streaming?
        
        :return: 
        """
        endpoint = "/api/menu/broadcast"
        result = self._api_get(endpoint)
        return result

    def talks(self):
        """
        Get lists of talks
        
        Three lists, of popular, live, and followed talks
        
        Formatted for display, so included in the lists are headers and messages to the user
        
        :return: 
        """
        endpoint = "/api/talk/talks"
        result = self._api_get(endpoint)
        return result.get('talks')

    def time_tables(self, started_at=None, order=None):
        """
        :param started_at:  int
        :param order:  str
        :return: 
        """
        # TODO: find out what valid values for order are. NEXT/PREV?
        endpoint = "/api/time_table/time_tables"
        result = self._api_get(endpoint, params=dict(started_at=started_at, order=order))
        return result
