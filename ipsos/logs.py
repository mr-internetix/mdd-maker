import logging, copy, colorama, os

LOG_COLORS = {
    logging.ERROR: colorama.Fore.RED,
    logging.WARNING: colorama.Fore.YELLOW
}
logs_logger = False

colorama.init()

class ColorFormatter(logging.Formatter):
    """ 
    This class is only used to highlight warnings and errors in the visual studio code terminal to make them stand out. 
    """

    def format( self, record, *args, **kwargs ):
        # if the corresponding logger has children, they may receive modified
        # record, so we want to keep it intact
        new_record = copy.copy(record)
        if ( new_record.levelno in LOG_COLORS ):
            # we want levelname to be in different color, so let's modify it
            new_record.levelname = "{color_begin}{level}{color_end}".format(
                level=new_record.levelname,
                color_begin=LOG_COLORS[new_record.levelno],
                color_end=colorama.Style.RESET_ALL,
            )
            new_record.msg = "{color_begin}{msg}{color_end}".format(
                msg=new_record.msg,
                color_begin=LOG_COLORS[new_record.levelno],
                color_end=colorama.Style.RESET_ALL,
            )
    
        # now we can let standart formatting take care of the rest
        return super( ColorFormatter, self ).format( new_record, *args, **kwargs )


class Logs:
    """ 
    This class is a wrapper around the standard python logging. 

    Usage:
        log = ipsos.logs.Logs( verbose = verbose_mode, to_disc = True )

        example:
            log.logs.info( "info is for general logging statements, they do not appear if verbose = False." )
            log.logs.warning( "warning is for when you want to inform the user of something important, these always appear to the user regardless of the verbose setting." )
            log.logs.error( "error is for exception handling or validation failures, , these always appear to the user regardless of the verbose setting." )

    Args: 
        verbose (boolean): When True - generates extensive logging of the process (default = False)
        to_disc (boolean): When True - Writes out the logging to a file called dump.log in the same location as the script being run (default = False)

    Attributes: 
		
    Methods:
    """

    def __init__( self, name = '', verbose = False, to_disc = False ):
        global logs_logger
        
        # Set up the logger
        if ( name == '' ):
            self.logs = logging.getLogger('main')
        else:
            self.logs = logging.getLogger('main.' + name)
        
        if not( logs_logger ):
            logs_logger = True
            if ( verbose ):
                self.logs.setLevel(logging.INFO)
            else:
                self.logs.setLevel(logging.WARNING)

            if ( to_disc ):
                if ( os.path.exists( 'dump.log' ) ): os.remove( 'dump.log' )
                d_handler = logging.FileHandler('dump.log')
                d_format = logging.Formatter("%(asctime)s  %(name)s  %(levelname)s  %(message)s")
                d_format.datefmt = '%c'
                d_handler.setFormatter(d_format)
                self.logs.addHandler(d_handler)

            s_handler = logging.StreamHandler()
            s_format = ColorFormatter("%(asctime)s  %(name)s  %(levelname)s  %(message)s")
            s_format.datefmt = '%c'
            s_handler.setFormatter(s_format)
            self.logs.addHandler(s_handler)
