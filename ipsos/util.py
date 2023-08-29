import datetime

class Logging:

    LOGLEVEL_ERROR = "ERROR  "
    LOGLEVEL_INFO = "INFO   "
    LOGLEVEL_WARN = "WARNING"

    def log( self, msg, loglevel = LOGLEVEL_ERROR ):
        print( datetime.datetime.now().isoformat( " " ) + "  " + loglevel + "  " + msg )