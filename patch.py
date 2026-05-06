content = open('fetch_jobs.py', encoding='utf-8').read()
fix = 'import sys, io\nsys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")\nsys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")\n'
old = 'import requests, csv, json, re'
if fix not in content:
    new_content = content.replace(old, fix + old, 1)
    open('fetch_jobs.py', 'w', encoding='utf-8').write(new_content)
    print('Patched successfully')
else:
    print('Already patched')

# Check for run_kpmg
if 'run_kpmg' not in content:
    print('WARNING: run_kpmg not found - you need the full new fetch_jobs.py')
else:
    print('run_kpmg found OK')
