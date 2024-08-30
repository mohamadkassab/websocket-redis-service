# import C methods to our module
from ._core import *

# add path for MT5APIManager64.dll
import os
module_dir = os.path.dirname(__file__)
script_dir = os.getcwd()

InitializeManagerAPIPath(module_path=module_dir,work_path=script_dir)
