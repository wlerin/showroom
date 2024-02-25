from .constants import JAPANESE_INDEX


def translate(name):
	room = JAPANESE_INDEX.find_room(name=name)
	if not room:
		return name
	return room['engName']
