import argparse
from .check import check_dirs
from .compare import compare_archives


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
    parser_compare.add_argument('add_files', nargs='*', help='Files to compare against')
    parser_compare.add_argument('--with-web', action='store_true', help='Also check against sr.gutas.net')
    parser_compare.set_defaults(func=compare_archives)

    return parser


def main():
    main_args = ('func', 'subcommand')

    parser = build_parser()
    args = parser.parse_args()
    kwargs = {k: v for k, v in vars(args).items() if k not in main_args}
    args.func(**kwargs)


if __name__ == "__main__":
    main()
