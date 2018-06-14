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
            f.write(json.dumps({'google_creds': google_creds}))

