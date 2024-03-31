# instead of a watcher per individual room
# a single watcher that communicates between Rooms and the Broadcast client


class Watcher:
    def __init__(self, index):
        self.index = index

    async def watch(self, room_url_key):
        room = self.index.find_room(url_key=room_url_key)

