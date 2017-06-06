import json
from .constants import MAX_GAP


def merge_streams(streams):
    # returns a combination of all streams
    merged_stream = streams[0].copy()
    for stream in streams[1:]:
        merged_stream['files'].extend(stream['files'])
        for height in stream['heights']:
            if height not in merged_stream['heights']:
                merged_stream['heights'].append(height)
        merged_stream['end_time'] = stream['end_time']
        for key in ("total_size", "total_frames", "total_duration", "total_gaps"):
            merged_stream[key] += stream[key]
    return merged_stream


def unify_streams(room1, room2):
    """

    Args:
        room1: Room with fewer streams
        room2: Room with more streams, some streams may be merged

    Returns:
        Nothing, but modifies its arguments
    """
    if room1['total_duration'] >= room2['total_duration']:
        for stream in room1['streams']:
            start_time = stream['start_time']
            end_time = stream['end_time']
            overlapped = [s for s in room2['streams'] if start_time - MAX_GAP < s['start_time'] < end_time + MAX_GAP]
            if len(overlapped) == 1:
                continue
            elif len(overlapped) > 1:
                if overlapped[-1]['end_time'] <= end_time + MAX_GAP:
                    # room2's stream set entirely within the bounds of room1's stream
                    merge_streams(overlapped)
                    for stream in overlapped[1:]:
                        room2['streams'].remove(stream)
                else:
                    # SOMETHING WEIRD IS HAPPENING IN OZ
                    pass
            else:
                # ERROR
                pass

    else:
        # most likely streams are entirely absent
        # this isn't really what this function is for though...
        pass


def compare_rooms(main_room, alt_room):
    result = {}

    SIZE_IEC_MB = 2 ** 20

    # TODO: drastically reduce the tolerances for top priority (< 8 or so) rooms
    def calc_max_time_diff(duration, priority):
        factor = 1
        while factor < 5:
            if factor > priority / factor:
                break
            else:
                factor += 1
        return factor + int(duration * factor / (60 * 12))

    def calc_max_size_diff(duration, priority):
        factor = 1
        while factor < 5:
            if factor > priority / factor:
                break
            else:
                factor += 1

        return SIZE_IEC_MB + int(SIZE_IEC_MB * duration * factor / (12 * 60 ** 2))

    main_size = sum((s['total_size'] for s in main_room['streams']))
    alt_size = sum((s['total_size'] for s in alt_room['streams']))
    result = {
        "time_diff": alt_room['total_duration'] - main_room['total_duration'],
        "size_diff": alt_size - main_size,
        "main_room": main_room,
        "alt_room": alt_room,
    }

    # simplest check: compare total duration
    if main_room['total_duration'] + calc_max_time_diff(main_room['total_duration'], main_room['priority']) \
            >= alt_room['total_duration']:
        return None
    else:
        # print("{}'s room failed the duration test: {:.2f} - {:.2f} = {:.2f} seconds".format(
        #     main_room['name'],
        #     main_room['total_duration'],
        #     alt_room['total_duration'],
        #     main_room['total_duration'] - alt_room['total_duration']
        # ))
        pass

    if (main_size + calc_max_size_diff(main_room['total_duration'], main_room['priority'])
            >=
            alt_size
        ):
        return None
    else:
        # print("{}'s room failed the size test: {:.2f} - {:.2f} = {:.2f} MiB".format(
        #     main_room['name'],
        #     main_size / SIZE_IEC_MB,
        #     alt_size / SIZE_IEC_MB,
        #     (main_size - alt_size) / SIZE_IEC_MB
        # ))
        pass

    return result

    # TODO: more in-depth analysis

    # old_stream = None
    # alt_stream_list = sorted(alt_room['streams'], key=lambda x: x['start_time'])
    # for stream in main_room['streams']:
    #     if old_stream:
    #         stream = merge_streams((old_stream, stream))
    #         old_stream = None
    #
    #     alt_streams = [s for s in alt_stream_list if s['start_time'] < stream['end_time'] + MAX_GAP]
    #     if not alt_streams:
    #         # no matching streams, ignore? warn?
    #         continue
    #     elif alt_streams[-1]['end_time'] <= stream['end_time'] + MAX_GAP:
    #         # all streams entirely within bounds of main stream
    #         # except we never checked start_time
    #         for s in alt_streams:
    #             alt_stream_list.remove(s)
    #         alt_stream = merge_streams(alt_streams)
    #         pass
    #     else:
    #         # alt_stream extends beyond, *most likely* we need to merge the main_stream with the next one and retry
    #         old_stream = stream
    #         continue
    # if old_stream:
    #     # handle dangling stream
    #     pass

    #
    #
    #
    # TODO: check individual streams

    # return result


def compare_archives(main_file, alt_files, with_web=False):
    # The idea is to compare the main file to any additional files,
    # and/or against sr48.net, and print a human readable summary of
    # files in need of replacing/repair in the main archive
    # for now, mimics the process I use when checking by hand
    replacements = []
    date = main_file.rsplit('/', 1)[-1].split('_')[1]

    # Formatting results is only necessary before printing them out to file
    with open(main_file, encoding='utf8') as infp:
        main_data = json.load(infp)

    if main_data.get('partial') is True:
        print('Warning: Main archive is marked as incomplete')

    # temp = []
    # for group in main_data.keys():
    #     temp.append(VideoGroup(group, main_data[group]))
    # main_data = format_results(temp)

    alt_data = {}
    for alt_file in alt_files:
        prefix = alt_file.rsplit('/', 1)[-1].split('_', 1)[0]
        with open(alt_file, encoding='utf8') as infp:
            alt_data[prefix] = json.load(infp)

    if 'main' in alt_data:
        print('Using non-main check results as base')

    compare_results = {"replacements": [], "notes": [], }
    # TODO: flatten the groups, there's no need to loop through each one individually
    for group_name, main_group in main_data['groups'].items():
        for prefix, alt_archive in alt_data.items():
            if alt_archive['partial']:
                alt_partial = True
            else:
                alt_partial = False

            try:
                alt_group = alt_archive['groups'][group_name]
            except KeyError:
                print('{} was not found in {}'.format(group_name, prefix))
                continue

            # assumes 3.6.x ordered builtin dict, otherwise this is pointless
            # actually it's mostly pointless anyway
            main_room_list = sorted((room for room_id, room in
                                     main_group.items()), key=lambda x: x['handle'])
            alt_room_list = sorted((room for room_id, room in
                                    alt_group.items()), key=lambda x: x['handle'])
            main_rooms = {room['room_id']: room for room in main_room_list}
            alt_rooms = {room['room_id']: room for room in alt_room_list}

            while main_room_list or alt_room_list:
                if main_room_list:
                    room = main_room_list.pop(0)
                    alt_room = alt_rooms.get(room['room_id'])
                    if alt_room:
                        alt_room_list.remove(alt_room)
                    else:
                        if not alt_partial:
                            compare_results['notes'].append("Couldn't find {} in {}".format(room['handle'], prefix))
                        continue
                    # compare the rooms
                    result = compare_rooms(room, alt_room)
                    # TODO: fill in extra data before appending, e.g. prefix and group
                    if result:
                        result.update({'prefix': prefix, 'group': group_name})
                        compare_results['replacements'].append(result)
                else:
                    alt_room = alt_room_list.pop(0)
                    result = {
                        "time_diff": 0,
                        "size_diff": 0,
                        "main_room": alt_room,
                        "alt_room": alt_room,
                        "prefix": prefix,
                        "group": group_name
                    }
                    compare_results['replacements'].append(result)


    with open('compare_{}.json'.format(date), 'w', encoding='utf8') as outfp:
        json.dump(compare_results, outfp, ensure_ascii=False, indent=2)



    # TODO: print just the files to replace/copy
    # if len_main != len_add:
    #     print('Streams in base: {}   Streams in {}: {}'.format(len_main, prefix, len_add))
    '''
    main_index = add_index = 0
    while True:
        main_member, main_time = main_streams[main_index].rsplit(' ', 1)
        add_member, add_time = add_streams[add_index].rsplit(' ', 1)

        # this logic will only work when there are but two sources to compare between
        preferred = 'main'
        # This assumes both sources use the same index
        # If not there will be problems, hence the need for room_ids
        if main_member == add_member:
            # within 1 minute of each other
            # there really shouldn't be more than a minute difference unless one or the other failed
            if -1 <= int(main_time) - int(add_time) <= 1:
                if int(add_time) > int(main_time):
                    preferred = prefix
            # within 5 minutes of each other
            elif -5 <= int(main_time) - int(add_time) <= 5:
                if int(add_time) > int(main_time):
                    preferred = prefix
            # in the same hour
            elif -100 <= int(main_time) - int(add_time) <= 100:
                pass
            # fully separate streams, or failed recording on one end
            else:
                pass
    '''
