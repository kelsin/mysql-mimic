"""mysql-mimic version information"""

__version__ = "0.2.0"


def main(name):
    if name == "__main__":
        print(__version__)


main(__name__)
