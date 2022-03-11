import logging
import subprocess

logger = logging.getLogger(__name__)


def run_cmd(args_list):
    """
        run linux commands
    """
    logger.info('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    logger.info(str(s_output) + "\n" + str(s_err))

    return s_return, s_output, s_err
