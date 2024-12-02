import sys
import os
from pytest import fixture

# Add the parent directory to sys.path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if parent_dir not in sys.path:
  sys.path.insert(0, parent_dir)

# Import the module
from components import CustomPageIncrement

@fixture
def custom_page_increment():
  page_size = 1
  config = {}
  parameters = {}
  return CustomPageIncrement(config, page_size, parameters)
