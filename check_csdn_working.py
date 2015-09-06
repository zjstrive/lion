'''
Created on Sep 6, 2015

@author: jie
'''
import requests
import sys

WEB_URL = 'http://www.csdn.net/'
LOGIN_USER = ""
PASSWORD = ""


def run():
    try:
        page = requests.get(WEB_URL)
        if page.status_code == 200:
            print("CSDN is working great :)")
        else:
            print("It looks like CSDN is having trouble, some one please take a look at it")
            sys.exit(-1)
    except:
        print("It looks like CSDN is having trouble, some one please take a look at it")
        sys.exit(-1)


def login_web():
    # urllib2   requests
    session = requests.session()
    login_data = dict(username=LOGIN_USER,
                      password=PASSWORD
                      )


if __name__ == '__main__':
    run()
