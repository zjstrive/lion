from sqlalchemy import create_engine
from prettytable import PrettyTable
import requests
import json
import logging
import datetime
import traceback
MonitorSnithStatus = {"OK": 'OK', "WARN": 'WARN', "CRITICAL": 'CRITICAL'}

PERIODS = {"TWO_HOUR": "TWO_HOUR",
           "HOUR": "HOUR",
           "HALF_HOUR": "HALF_HOUR",
           "TEN_MINUTES": "TEN_MINUTES",
           "DAILY": "DAILY",
           "WEEKLY": "WEEKLY",
           "MONTHLY": "MONTHLY"}

_PERIODS_VALUE = {
    "TWO_HOUR": datetime.timedelta(minutes=120),
    "HOUR": datetime.timedelta(minutes=60),
    "DAILY": datetime.timedelta(hours=24),
    "WEEKLY": datetime.timedelta(days=7),
    "MONTHLY": datetime.timedelta(days=31),
    "HALF_HOUR": datetime.timedelta(minutes=30),
    "TEN_MINUTES": datetime.timedelta(minutes=10),
}


class MonitorSnith(object):

    log = logging.getLogger(__name__)
    default_snith_url = "http://0.0.0.0:2332/api/app/{unique_id}/"

    def __init__(self, unique_id, periods=None):
        self.unique_id = unique_id
        self.last_report = None
        self.periods = periods
        if self.unique_id is None and periods is not None \
                and _PERIODS_VALUE.get(periods) is None:
            raise ValueError('Data is wrong')

    def _check_periods_time(self):
        if self.periods is None:
            return True
        elif datetime.datetime.now() - self.last_report >= _PERIODS_VALUE.\
                get(self.periods):
            return True
        return False

    def snith(self, status="OK", message=None, **statistics):
        """send a snith to monitoring.

        :param  status
        :param  message
        :param  statistics
        :rtype: True or False
        """
        try:
            response = None
            if MonitorSnithStatus.get(status) is None:
                raise ValueError('status format wrong')
            url = self.default_snith_url.format(unique_id=self.unique_id)
            message = message if message else ''
            statistics = json.dumps(statistics) if statistics else None
            data = {'status': status, 'statistics': statistics,
                    'message': message}
            if not self.last_report or self._check_periods_time():
                response = requests.put(url, data=data)
                if response.status_code == 405:
                    response = requests.post(url, data=data)
                self.last_report = datetime.datetime.now()
                return True
            else:
                return False
            response.raise_for_status()
        except:
            try:
                traceback.print_exc()
                print(response.reason)
                msg = "Failed with status: {0}, {1}".format(response.status_code,
                                                            response.reason)
                self.log.error(msg)
            except:
                traceback.print_exc()
            return False
        return True


def snitch(app_name=None, status="OK", message=None, **statistics):
    """decorator to send a snith to monitoring.
        Send snitch to monitoring, function name will be the app unique id
    :param  app_name  unique app name
    :param  status
    :param  message
    :param  statistics
    """
    def _monitorsnith(func):
        def wrapper(*args, **kwargs):
            func(*args, **kwargs)
            unique_id = app_name if app_name else func.__name__
            monitorSnith = MonitorSnith(unique_id)
            monitorSnith.snith(status, message, **statistics)
        return wrapper
    return _monitorsnith


def snitch_with_return(func):
    """decorator to send a snith to monitoring.
       Send snitch to monitoring, function name will be the app unique id
    :param  name    unique app name
    :param  status
    :param  message
    :param  statistics
    """

    def wrapper(*args, **kwargs):
        _snitch = func(*args, **kwargs)
        app_name = _snitch.get("name") if _snitch.get("name") else func.__name__
        status = _snitch.get("status") if _snitch.get("status") else 'OK'
        message = _snitch.get("message")
        statistics = _snitch.get("statistics")
        monitorSnith = MonitorSnith(unique_id=app_name)
        if statistics:
            monitorSnith.snith(status, message, **statistics)
        else:
            monitorSnith.snith(status, message)
    return wrapper

#==================================================================================================================

host = "127.0.0.1"
user = "root"
password = "admin"
db = "smallmonitor"


def get_engine(host, user, password, db,
               autocommit=False,
               autoflush=True,
               expire_on_commit=True,
               DEFAULT_POOL_RECYCLE=3600):
    URL = "mysql+pymysql://{user}:{password}@{host}:3306/{db}?charset=utf8&use_unicode=0".format(host=host,
                                                                                                 user=user,
                                                                                                 password=password,
                                                                                                 db=db)
    conn = create_engine(URL)
    return conn


def decode_bytes(str):
    if str:
        return str.decode("utf-8")


DB_SIZE_SQL = '''
select concat(truncate(sum(data_length)/1024/1024,2)) as data_size,
concat(truncate(sum(max_data_length)/1024/1024,2)) as max_data_size,
concat(truncate(sum(data_free)/1024/1024,2)) as data_free,
concat(truncate(sum(index_length)/1024/1024,2)) as index_size
from information_schema.tables where TABLE_SCHEMA = '{0}';
'''

DB_CONNECTION_SQL = 'SHOW FULL PROCESSLIST'


@snitch_with_return
def database_watcher(conn, db_connection_table, db_size_table):
    status = MonitorSnithStatus["OK"]
    db_connection_result = conn.execute(DB_CONNECTION_SQL)
    for row in db_connection_result:
        db_connection_table.add_row([row[0], decode_bytes(row[1]),
                                    decode_bytes(row[2]), decode_bytes(row[3]),
                                    decode_bytes(row[4]), row[5],
                                    decode_bytes(row[6]),
                                    decode_bytes(row[7])])
    db_size_result = conn.execute(DB_SIZE_SQL.format(db))
    for row in db_size_result:
        db_use_size = float(decode_bytes(row[2]))
        db_size_table.add_row([decode_bytes(row[0]),
                               decode_bytes(row[1]),
                               decode_bytes(row[2]),
                               decode_bytes(row[3])])
    if int(len(db_connection_table._rows)) > 100 or db_use_size > 2000:
        status = MonitorSnithStatus["CRITICAL"]
    elif len(db_connection_table._rows) > 50 or db_use_size > 1000:
        status = MonitorSnithStatus["WARN"]
    message = str(db_connection_table) + str(db_size_table)
    return {"name": "check_databace_source",
            "message": message,
            "status": status,
            "statistics": {"db_connection_number": int(len(db_connection_table._rows)),
                           "db_use_size": db_use_size}}


def run():
    conn = get_engine(host=host, user=user, password=password, db=db)
    db_connection_table = PrettyTable(['Id', 'User', 'Host', 'db', 'Command', "Time", "State", "Info"])
    db_size_table = PrettyTable(['Data Size(MB)', 'Max Data Size(MB)', 'Data Free(MB)', 'Index Size(MB)'])
    database_watcher(conn, db_connection_table, db_size_table)

run()
