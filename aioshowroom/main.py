import click
import asyncio
from .room import Room
from .watcher import Watcher
from showroom.index import ShowroomIndex
from showroom.settings import settings


# start with a basic version
@click.group()
def main():
    pass


@click.command()
def test():
    click.echo('Hello world')


@click.command()
@click.argument('room_url_key')
def watch(room_url_key):
    index = ShowroomIndex(settings.directory.index)
    room = index.find_room(url_key=room_url_key)
    room = Room(room)
    print(room.room_url_key, room.is_wanted(), room.live_id)
    # watcher = Watcher(index)
    # asyncio.run(watcher.watch(room_url_key))


main.add_command(watch)
if __name__ == "__main__":
    main()
