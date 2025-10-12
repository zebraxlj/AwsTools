import logging
import time

# possible characters: ❱ ✎ ➜
start_char = '❱'
format_log = f'%(asctime)s [%(levelname)s]\t%(filename)s:%(lineno)d 📌 %(module)s.%(funcName)s {start_char} %(message)s'


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


def setup_logging(level=logging.DEBUG):
    handler = logging.StreamHandler()
    handler.setFormatter(FormatterMs(format_log, datefmt=r'%Y-%m-%dT%H:%M:%S.%f%z'))
    logging.basicConfig(level=level, handlers=[handler])

    logging.getLogger('botocore').setLevel(logging.INFO)
    logging.getLogger('boto3').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.INFO)


def test_logging():
    setup_logging()
    logging.info(f'Start {__file__}')


if __name__ == '__main__':
    test_logging()
