import sys
import os
import optparse
import logging
from opencluster.factory import FactoryContext
from opencluster.configuration import Conf, setLogger

parser = optparse.OptionParser(usage="Usage: python %prog [options]")

def add_default_options():
    parser.disable_interspersed_args()
    parser.add_option("-e", "--config", type="string", default="/temp/opencluster/config.ini", help="path for configuration file")
    parser.add_option("-q", "--quiet", action="store_true",help="be quiet")
    parser.add_option("-v", "--verbose", action="store_true", help="show more useful log")

add_default_options()

def run():
    (options, _) = parser.parse_args()
    if not options:
        parser.print_help()
        sys.exit(2)

    Conf.setConfigFile(options.config)
    options.logLevel = (options.quiet and logging.ERROR
                        or options.verbose and logging.DEBUG
                        or logging.INFO)

    setLogger("default", "factory", options.logLevel)
    FactoryContext.startDefaultFactory()

if __name__ == '__main__':
    run()