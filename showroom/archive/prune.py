# move unneeded files (according to compare results) to "unneeded" folder
# or, for the main archive, move files about to be replaced to a "replaced" folder
# the final step of actually moving/copying the files is left to be done manually
import os
import glob
import json
import shutil


def prune_folder(folder, needed_list):
    oldcwd = os.getcwd()
    os.chdir(folder)

    os.makedirs('unneeded'.format(folder), exist_ok=True)
    files = glob.glob('*.mp4'.format(folder))
    for file in files:
        # print(repr(file))
        if file not in needed_list:
            # print('{} -> {}'.format(file, 'unneeded/{}'.format(file)))
            shutil.move(file, 'unneeded/{}'.format(file))
        else:
            # print('Needed:', repr(file))
            pass

    os.chdir(oldcwd)


def get_needed_files_list(results):
    needed_list = []
    for room in results['replacements']:
        for stream in room['alt_room']['streams']:
            for file in stream['files']:
                if file['valid']:
                    needed_list.append(file['file']['name'])
    print(repr(needed_list))
    return needed_list


def prune_archive(archive_dir, compare_results):
    with open(compare_results, encoding='utf8') as infp:
        results = json.load(infp)

    needed_list = get_needed_files_list(results)
    for folder in glob.glob('{}/*'.format(archive_dir)):
        if os.path.isdir(folder):
            prune_folder(folder, needed_list)


def replace_folder(folder, replace_list, prefix):
    oldcwd = os.getcwd()
    os.chdir(folder)
    replaced_folder = 'replaced_{}'.format(prefix)
    os.makedirs(replaced_folder, exist_ok=True)
    files = glob.glob('*.mp4')
    for file in files:
        if file in replace_list:
            dest = '{}/{}'.format(replaced_folder, file)
            print('{} -> {}'.format(file, dest))
            shutil.move(file, dest)

    os.chdir(oldcwd)


def get_replacements_list(results):
    needed_list = []
    for room in results['replacements']:
        for stream in room['main_room']['streams']:
            for file in stream['files']:
                # no valid check here
                needed_list.append(file['file']['name'])
    return needed_list


def replace_archive(archive_dir, compare_results, prefix='none'):
    with open(compare_results, encoding='utf8') as infp:
        results = json.load(infp)

    replace_list = get_replacements_list(results)
    for folder in glob.glob('{}/*'.format(archive_dir)):
        if os.path.isdir(folder):
            replace_folder(folder, replace_list, prefix)

# TODO: move_archive using shutil
# TODO: package_archive using ... some kind of packager? making another folder? for easy upload to MEGA
