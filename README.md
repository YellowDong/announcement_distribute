# Announcement-spider

A distributed spider fetch the announcement from sh and sz
before use the spider you should install some moduel

pip install -r requirements.txt


please config some para in settings file

REDIS_URL = 'redis://root:password@host:6379'
password is your redis password(if no password set None)
host is your server host

PDFPATH is save pdf path(default current path)

mysql or mongodb para you should write in settings file

run:
python run.py