

class VREndpointsMixin:
    def vr_room_maps(self, normal_room_id=None, vr_room_id=None):
        """
        If neither is given, returns a list of all vr room maps
        
        :param normal_room_id: 
        :param vr_room_id: 
        :return: 
        """
        endpoint = "/api/vr/room_maps"
        result = self._api_get(endpoint, params=dict(
            normal_room_id=normal_room_id,
            vr_room_id=vr_room_id
        ))
        return result

    def vr_camera_settings(self, room_id):
        endpoint = "/api/vr/camera_settings"
        result = self._api_get(endpoint, params={"room_id": room_id})
