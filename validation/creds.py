# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import os
import json


def set_credential(**kwargs):
    google_creds = kwargs.get('google_key', '')

    home_dir = os.environ.get('HOME')
    configfile = os.path.join(home_dir, '.csoconfig.json')
    if not os.path.exists(configfile):
        with open(configfile, 'w') as f:
            f.write(json.dumps({'google_key': google_creds}))

def get_credential(key):
    home_dir = os.environ.get('HOME')
    configfile = os.path.join(home_dir, '.csoconfig.json')
    if not os.path.exists(configfile):
        return None
    else:
        data=open(configfile).read()
        json_data = json.loads(data)
        return json_data.get('google_key')
