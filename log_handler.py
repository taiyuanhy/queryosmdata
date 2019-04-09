# coding: utf-8
import logging
import logging.handlers
import os

def make_dir(make_dir_path):
    path = make_dir_path.strip()
    if not os.path.exists(path):
        os.makedirs(path)
    return path
"""设置日志 每隔一天进行分割
"""
def set_logger(logger):
    log_dir_name = "logs"
    log_file_folder = os.path.abspath(os.path.join(os.path.dirname(__file__))) + os.sep + log_dir_name
    make_dir(log_file_folder)
    log_level = logging.DEBUG
    handler = logging.handlers.TimedRotatingFileHandler('logs/info.log', when='D', interval=1, backupCount=100, encoding='utf-8')
    handler.suffix = "%Y-%m-%d-%H-%M.log"
    handler.setLevel(log_level)
    logging_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(lineno)s - %(message)s')
    handler.setFormatter(logging_format)
    logger.addHandler(handler)