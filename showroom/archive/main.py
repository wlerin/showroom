import argparse
from .check import check_dirs, check_final
from .compare import compare_archives
from .prune import prune_archive, replace_archive
from .profile import scrape_profile_pics
from .trim import trim_videos


def kimi_dare_dispatch(**kwargs):
    from .shows.kimidare import kimi_dare_dispatch as _dispatch
    _dispatch(**kwargs)


def hls_dispatch(**kwargs):
    from .. import hls
    command = kwargs.pop('command')
    if command == 'simplify':
        target = kwargs.get('target')
        hls.simplify(target)
    elif command == 'merge':
        # TODO: this uses logging to print output
        # set up logging to console and convert the other two to logging output instead of print
        target = kwargs.get('target')
        force_yes = kwargs.get('force_yes')
        hls.merge_segments(target, force_yes=force_yes)
    elif command == 'check':
        target = kwargs.get('target')
        results = hls.check_missing(target)
        if len(results) > 1:
            print('Found multiple filename patterns in', target)
            for key, val in results.items():
                if val:
                    print(key, 'is missing', val)
                else:
                    print(key, 'has all expected files')
        else:
            for key, val in results.items():
                if val:
                    print(target, 'is missing', val)
                else:
                    print(target, 'has all expected files')


def build_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='subcommand')

    parser_check = subparsers.add_parser('check', help='Check directories, saving results to a file')
    parser_check.add_argument('dirs', nargs='+', help='Directories to check')
    parser_check.add_argument('--prefix', type=str, help='Archive name to prefix before check file', default='main')
    parser_check.add_argument('--output-dir', '-o', help='Output directory for results', default='.')
    parser_check.add_argument('--partial', action='store_true', help='Marks archive as incomplete')
    parser_check.set_defaults(func=check_dirs)

    parser_compare = subparsers.add_parser('compare', help='Compare check results, needs either 2+ files, or '
                                                           'the --with-web switch to compare against sr.gutas.net, '
                                                           'or both. NOT IMPLEMENTED')
    parser_compare.add_argument('main_file', help='Main archive check results')
    parser_compare.add_argument('alt_files', nargs='*', help='Files to compare against')
    parser_compare.add_argument('--with-web', action='store_true', help='Also check against sr.gutas.net')
    parser_compare.set_defaults(func=compare_archives)

    # http://stackoverflow.com/a/26379693/3380530 default subparser to minimise typing
    # parser_shows = subparsers.add_parser('show', help='Show-specific helper functions')
    # show_parsers = parser_shows.add_subparsers(dest='shows')

    parser_kimidare = subparsers.add_parser('kimidare', help='Kimi Dare-specific helper functions')
    parser_kimidare.add_argument('--update', '-u', action='store_true', help='Update the episode list')
    # work_dir, dest_dir, data
    parser_kimidare.add_argument('--trim', '-t', dest='trim_dir', help='Trim files in TRIM_DIR')
    parser_kimidare.add_argument('--output-dir', '-o', help='Destination for trimmed files')
    # TODO: put the episode list somewhere else
    parser_kimidare.set_defaults(func=kimi_dare_dispatch)

    parser_prune = subparsers.add_parser('prune', help='Move unneeded files to an "unneeded" folder')
    parser_prune.add_argument('archive_dir', help='Archive to prune (NOT the main archive!)')
    parser_prune.add_argument('compare_results', help='JSON describing comparison results')
    parser_prune.set_defaults(func=prune_archive)

    parser_replace = subparsers.add_parser('replace', help='Move files to be replaced to a "replaced" folder')
    parser_replace.add_argument('archive_dir', help='Archive to replace (the main archive)')
    parser_replace.add_argument('compare_results', help='JSON describing comparison results')
    parser_replace.set_defaults(func=replace_archive)

    parser_profile = subparsers.add_parser('profile', help='Download profile pics to a directory')
    parser_profile.add_argument('profile_dir', help='Directory in which to save profile pics')
    parser_profile.add_argument('-n', dest='photo_num', help='Which iteration of profile pics this is')
    parser_profile.set_defaults(func=scrape_profile_pics)

    parser_trim = subparsers.add_parser('trim', help='Trim some seconds from the start of a video or videos.')
    parser_trim.add_argument('--output-dir', '-o', help='Output directory for finished files. Required', default='.')
    parser_trim.add_argument('--trim-starts', '-s', action='append', help='Seconds from start to trim, for each video, use -1 for none', type=str)
    parser_trim.add_argument('--trim-ends', '-t', action='append', help='Time to cut the end of the video, repeat for each video.', type=str)
    parser_trim.add_argument('video_list', metavar='files', nargs='+', help='Files to trim')
    parser_trim.set_defaults(func=trim_videos)

    parser_hls = subparsers.add_parser('hls', help='Various commands for working with experimental hls recordings')
    parser_hls.add_argument('command', choices=('simplify', 'merge', 'check'), help='Command to run:\n'
                            'Check a single stream folder to see if any files are missing.\n'
                            'Merge a single stream folder into a TS file\n'
                            'Simplify a folder of streams, combining any that were obviously from the same broadcast.')
    parser_hls.add_argument('target', help='Target folder')
    parser_hls.add_argument('--force_yes', '-y', action='store_true', help='Force merger to ignore missing files')

    parser_final = subparsers.add_parser('final', help='Final check')
    parser_final.add_argument('dirs', nargs='+', help='Directories to check')
    # prefix is automatically final
    parser_final.add_argument('--output-dir', '-o', help='Output directory for results', default='.')
    parser_final.set_defaults(func=check_final)
    return parser


def main():
    main_args = ('func', 'subcommand')

    parser = build_parser()
    args = parser.parse_args()
    kwargs = {k: v for k, v in vars(args).items() if k not in main_args and v is not None}
    args.func(**kwargs)


if __name__ == "__main__":
    main()
