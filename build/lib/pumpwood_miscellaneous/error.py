"""Module to treat error using loguru."""
import traceback
import json
from loguru import logger
from pumpwood_communication.exceptions import PumpWoodException


def log_error(exc: Exception):
    """Log error with traceback using loguru."""
    list_trace_back_msg = []
    for tb in traceback.extract_tb(exc.__traceback__):
        filename = tb.filename
        line_no = tb.lineno
        trace_back_msg = "- {filename} | {line_no}"\
            .format(filename=filename, line_no=line_no)
        list_trace_back_msg.append(trace_back_msg)
    traceback_log = "\n".join(list_trace_back_msg)

    log_message_tmp = (
        "Treated error:"
        "\n{message}"
        "\nTraceback:\n{traceback_log}\n"
        "\n{error_dump}")
    message = None
    payload_data = None
    if isinstance(exc, PumpWoodException):
        exc_dict = exc.to_dict()
        message = exc_dict['message']
        try:
            payload_data = json.dumps(exc_dict, indent=2)
        except Exception:
            payload_data = 'Not possible to dump payload content'
        log_message = log_message_tmp.format(
            traceback_log=traceback_log,
            error_dump=payload_data, message=message)
        logger.warning(log_message)
    else:
        message = str(exc)
        payload_data = 'Untreated error'
        log_message = log_message_tmp.format(
            traceback_log=traceback_log,
            error_dump=payload_data, message=message)
        logger.error(log_message)
