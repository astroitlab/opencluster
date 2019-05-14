import sys
import os
import optparse
import socket
import logging
from opencluster.nodedaemon import NodeDademon
from opencluster.configuration import Conf, setLogger

parser = optparse.OptionParser(usage="Usage: python %prog [options]")

def add_default_options():
    parser.disable_interspersed_args()
    parser.add_option(
        "-e",
        "--config",
        type="string",
        default="/temp/opencluster/config.ini",
        help="path for configuration file")
    parser.add_option(
        "-H",
        "--host_ip",
        type="string",
        default="127.0.0.1",
        help="ip binding")
    parser.add_option("-q", "--quiet", action="store_true", help="be quiet")
    parser.add_option(
        "-v", "--verbose", action="store_true", help="show more useful log")

add_default_options()

def run():
    (options, _) = parser.parse_args()
    if not options:
        parser.print_help()
        sys.exit(2)

    Conf.setConfigFile(options.config)
    options.logLevel = (options.quiet and logging.ERROR
                        or options.verbose and logging.DEBUG or logging.INFO)

    node_ip_str = "".join(options.host_ip.split("."))
    setLogger("node", node_ip_str, options.logLevel)
    NodeDademon(options.host_ip, Conf.getNodeDefaultPort(), node_ip_str)

if __name__ == '__main__':
    run()