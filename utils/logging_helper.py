import logging
import time

# possible characters: ❱ ✎ ➜
START_CHAR = '❱'
FORMAT_LOG = f'%(asctime)s [%(levelname)s]\t%(filename)s:%(lineno)d 📌 %(module)s.%(funcName)s {START_CHAR} %(message)s'


class FormatterMs(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            if '%f' in datefmt:
                datefmt = datefmt.replace('%f', '{ms}')
                s = time.strftime(datefmt, ct)
                s = s.replace('{ms}', f'{int(record.msecs):03d}')
                return s
        return super().formatTime(record, datefmt)


def setup_logging(level=logging.DEBUG, is_show_logger_name: bool = False):
    if is_show_logger_name:
        global FORMAT_LOG
        FORMAT_LOG = FORMAT_LOG.replace('\t', '\t%(name)s ')

    handler = logging.StreamHandler()
    handler.setFormatter(FormatterMs(FORMAT_LOG, datefmt=r'%Y-%m-%dT%H:%M:%S.%f%z'))
    logging.basicConfig(level=level, handlers=[handler])

    logging.getLogger('botocore').setLevel(logging.WARN)
    logging.getLogger('boto3').setLevel(logging.INFO)
    logging.getLogger('s3transfer').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.INFO)


def test_logging():
    setup_logging(is_show_logger_name=False)
    logging.info(f'Start {__file__}')


if __name__ == '__main__':
    test_logging()
