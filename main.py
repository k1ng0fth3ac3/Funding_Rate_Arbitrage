from dbUpload import Upload
from logToDb import DBlogger

remote_server = False
terminal_print = True
exchange_count = 50


logger = DBlogger(remote_server=remote_server,terminal_print=terminal_print)


upload = Upload(logger=logger,remote_server=remote_server)
upload.funding_rates(exchange_count=exchange_count)
upload.calc_table()

