import subprocess, sys, os
import logging
import multiprocessing
import ipsos.logs
from multiprocessing import Pool, Value


class MultiProcessingExecutable:
    """ 
    This class contains methods for executing commands by distributing the commands across processes. 

    Usage:
        process = ipsos.processing.parallel.MultiProcessingExecutable( 3, verbose_mode )

        example:
            commands = []
            commands.append( [ "dmsrun.exe", "test/script.dms", "/s", "/break", "/dPART \"Script 1\"", "/dMDD \"C:\\Users\\user_name\\OneDrive - Ipsos\\Projects\\ipsos\\test\\part-1\\Original.part-1.mdd\"", "/dDDF \"C:\\Users\\user_name\\OneDrive - Ipsos\\Projects\\ipsos\\test\\part-1\\Original.part-1.ddf\"" ] )
            commands.append( [ "dmsrun.exe", "test/script.dms", "/s", "/break", "/dPART \"Script 2\"", "/dMDD \"C:\\Users\\user_name\\OneDrive - Ipsos\\Projects\\ipsos\\test\\part-2\\Original.part-2.mdd\"", "/dDDF \"C:\\Users\\user_name\\OneDrive - Ipsos\\Projects\\ipsos\\test\\part-2\\Original.part-2.ddf\"" ] )
            process = ipsos.processing.parallel.MultiProcessingExecutable( 3, verbose_mode )
            process.execute_and_wait( commands )

    Args: 
        pool_size (int): The number of workers.
        verbose (boolean): When True - generates extensive logging of the process (default = False)

    Methods:
        execute_and_wait( commands ): Execute multiple commands using a multiprocessing worker pool.
        execute( command ): Execute a single command.
    """
    def __init__( self, pool_size = 2, verbose = False ):
        self.workers = pool_size
        self.verbose = verbose

        # Set up the logger
        self.log = ipsos.logs.Logs( name = 'parallel', verbose = verbose )

    def setup_counter( self, cntr ):
        global process_count
        process_count = cntr

    def _remove_old_logs( self ):
        logs = os.listdir( './' )
        for log in logs:
            if ( log.startswith( '__multiprocessing-log-' ) ): 
                self.log.logs.warning( 'Deleting ' + log )
                os.remove( './' + log )

    def execute_and_wait( self, commands ):
        """
        This method executes multiple commands using a multiprocessing worker pool.
        
        Usage:
            commands = []
            commands.append( [ "dmsrun.exe", "test/script.dms", "/s", "/break", "/dPART \"Script 1\"", "/dMDD \"C:\\Users\\user_name\\OneDrive - Ipsos\\Projects\\ipsos\\test\\part-1\\Original.part-1.mdd\"", "/dDDF \"C:\\Users\\user_name\\OneDrive - Ipsos\\Projects\\ipsos\\test\\part-1\\Original.part-1.ddf\"" ] )
            commands.append( [ "dmsrun.exe", "test/script.dms", "/s", "/break", "/dPART \"Script 2\"", "/dMDD \"C:\\Users\\user_name\\OneDrive - Ipsos\\Projects\\ipsos\\test\\part-2\\Original.part-2.mdd\"", "/dDDF \"C:\\Users\\user_name\\OneDrive - Ipsos\\Projects\\ipsos\\test\\part-2\\Original.part-2.ddf\"" ] )
            process = ipsos.processing.parallel.MultiProcessingExecutable( 3, verbose_mode )
            process.execute_and_wait( commands )
        
        Args:
            commands (list): A list of commands to execute.
            
        Returns:
            None
        """
        self.log.logs.info( "Beginning parallel processing execute_and_wait method" )
        # Delete the log files if they exist
        self._remove_old_logs()

        # Reset the process counter
        process_count = Value( 'i', 0)

        with process_count.get_lock():
            process_count.value = 0

        try:
            pool = Pool( processes = self.workers, initializer=self.setup_counter, initargs=[ process_count ] )
            pool.map( self.execute, commands )
        except:
            self.log.logs.error( sys.exc_info()[0] )
            raise

    def execute( self, command ):
        """
        This method will execute a single command.

        Usage:
            command = ["dmsrun.exe", "test/script.dms", "/s", "/break", "/dPART \"Script 1\"", "/dMDD \"C:\\Users\\user_name\\OneDrive - Ipsos\\Projects\\ipsos\\test\\part-1\\Original.part-1.mdd\"", "/dDDF \"C:\\Users\\user_name\\OneDrive - Ipsos\\Projects\\ipsos\\test\\part-1\\Original.part-1.ddf\"" ]
            process = ipsos.processing.parallel.MultiProcessingExecutable( 3, verbose_mode )
            process.execute( command )
        
        Args:
            command (list or str): The command to execute.
        
        Returns:
            None
        """
        if ( len( command ) > 0 ):
            self.log.logs.info( "Executing " + ' '.join( command ) )
            process = subprocess.run( command, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
            
            process_id = ''
            if ( len( str( process.stdout ) ) > 0 or len( str( process.stderr ) ) > 0 ):
                # Check to see if process_count exists
                #   It will not exist if user calls this method directly
                if ( 'process_count' in globals() ):
                    with process_count.get_lock():
                        process_count.value += 1

                    # Create a 3-digit id, use it for both the logger and the log name
                    process_id = str( process_count.value ).zfill( 3 )
                else:
                    # Delete the log files if they exist
                    self._remove_old_logs()

                    process_id = '001'

            if ( len( str( process.stdout ) ) > 0 ):
                with open( '__multiprocessing-log-' + process_id + '.txt', 'w', encoding='utf8' ) as f:
                    f.writelines( ''.join( command ) + '\n\n' + str( process.stdout.decode() ).replace( '\r', '' ) )
            if ( len( str( process.stderr ) ) > 3 ):
                with open( '__multiprocessing-log-' + process_id + '.txt', 'a', encoding='utf8' ) as f:
                    f.writelines( ''.join( command ) + '\n\n' + str( process.stderr.decode() ).replace( '\r', '' ) )
