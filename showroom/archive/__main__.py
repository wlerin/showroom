import argparse
from .check import check_dirs
from .compare import compare_archives
from .shows.kimidare import kimi_dare_dispatch
from .prune import prune_archive, replace_archive


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

    return parser


def main():
    main_args = ('func', 'subcommand')

    parser = build_parser()
    args = parser.parse_args()
    kwargs = {k: v for k, v in vars(args).items() if k not in main_args}
    args.func(**kwargs)


if __name__ == "__main__":
    main()
