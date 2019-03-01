import argparse
from .core import configure, download, build


class CommandLine(object):
    """Sets up command line parser and invokes main functions"""

    def __init__(self):
        parser = argparse.ArgumentParser(
            description='Downloads and builds DEA ARCOS data')
        subparsers = parser.add_subparsers(
            dest='subcommand', required=True,
            description='Subcommands required.')

        self.setup_download_parser(subparsers)
        self.setup_build_parser(subparsers)
        self.parser = parser

    def main(self):
        args = self.parser.parse_args()
        args.func(args)

    def setup_download_parser(self, subparsers):
        parser_download = subparsers.add_parser('download')
        parser_download.set_defaults(func=self.download)
        parser_download.add_argument(
            '--folder', required=True,
            help='Folder in which ARCOS PDFs will be stored (required)')
        parser_download.add_argument(
            '--overwrite', action='store_true',
            help='Remove all downloaded PDFs in folder before proceeding')

        parser_download_group = parser_download.add_mutually_exclusive_group()
        parser_download_group.required = False
        parser_download_group.add_argument(
            '--base-url',
            default='https://www.deadiversion.usdoj.gov/arcos/'
                    'retail_drug_summary/index.html',
            help='Specify here the root url for the website where the DEA'
                 'stores the ARCOS PDFs. By default this program will crawl'
                 'down to a depth of 2 to find all PDFs. (default:'
                 'https://www.deadiversion.usdoj.govarcos/retail_drug_summary/'
                 'index.html)')
        parser_download_group.add_argument(
            '--use-crawl-list', action='store_true',
            help="If for some reason crawling the DEA website doesn't work, or"
                 'you want to specify a specific list of URLs for the program'
                 'to download, you can override crawling for links with'
                 'config/download_list.')

    def setup_build_parser(self, subparsers):
        parser_build = subparsers.add_parser('build')
        parser_build.set_defaults(func=self.build)

    def download(self, args):
        print(args)
        download(folder=args.folder, overwrite=args.overwrite,
                 base_url=args.base_url, use_crawl_list=args.use_crawl_list)

    def build(self, args):
        build(folder=args.folder)


def main():
    command_line = CommandLine()
    command_line.main()


if __name__ == '__main__':
    main()
