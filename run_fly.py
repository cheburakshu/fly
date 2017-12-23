import fly
import sys

if len(sys.argv) < 2:
    print("Usage: python run_fly.py config_file_name.conf")
else:
    fly.bootstrap(sys.argv[1])