#!C:\Users\Owner\PycharmProjects\RtsConversion\venv\Scripts\python.exe
# EASY-INSTALL-ENTRY-SCRIPT: 'ftfy==5.6','console_scripts','ftfy'
__requires__ = 'ftfy==5.6'
import re
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(
        load_entry_point('ftfy==5.6', 'console_scripts', 'ftfy')()
    )
