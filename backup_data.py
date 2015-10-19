
import json
import logging
import datetime
import requests
import traceback
import os
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
home_path = os.path.expanduser("~") + "/"
mysqldump_output_file_name = home_path + "mysql_backup/"


@snitch("mysqldump", "OK")
def mysqldump(host, user, password, database_name, port=3306, skipdata=False):
    try:
        skipdata_string = ''
        if skipdata:
            skipdata_string = '-d'
        output_file = mysqldump_output_file_name + database_name + str(datetime.datetime.now().strftime("%Y-%m-%d")) + '.sql'
        mysqldump_string = "mysqldump --lock-tables=false -C -P{0}  -h{1} -u{2} -p{3} {4} {5} > {6}".format(port,
                                                                                                              host,
                                                                                                              user,
                                                                                                              password,
                                                                                                              skipdata_string,
                                                                                                              database_name,
                                                                                                              output_file
                                                                                                              )
        os.system(mysqldump_string)
    except:
        traceback.print_exc()


def backup_xxdata():
    shell_str = 'find /home/jie/xxdata -name "*.zip" -mtime -700 | xargs -I {} scp {} /home/jie/xxdata_backup/'
    os.system(shell_str)


def run():
    backup_xxdata()
    mysqldump("127.0.0.1", "root", "admin", "monitoring")

if __name__ == '__main__':
    run()
