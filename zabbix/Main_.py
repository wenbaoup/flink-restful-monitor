import sys
from zabbix import SaveJobMessage
from zabbix import QueryJobMessage



if __name__ == '__main__':
    if len(sys.argv) == 2:
        yarnIds = sys.argv[1]
        for yarnId in  yarnIds.split(","):
            SaveJobMessage.run(yarnId)
    elif len(sys.argv) == 3:
        QueryJobMessage.run(sys.argv[1], sys.argv[2])
    else:
        print("---------the arguments you have inputed was wrong, please checks the arguments' numbers!-----------")
        sys.exit(1)
