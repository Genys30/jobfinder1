import requests, csv

BASE = 'https://raw.githubusercontent.com/mluggy/techmap/main/jobs/'
FNS = ['admin','business','data-science','design','devops','finance','frontend',
       'hardware','hr','legal','marketing','procurement-operations','product',
       'project-management','qa','sales','security','software','support']

total = 0
for fn in FNS:
    r = requests.get(BASE + fn + '.csv', timeout=30)
    n = len(list(csv.DictReader(r.text.splitlines())))
    print(fn, n)
    total += n

print('TOTAL:', total)