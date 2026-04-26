import json
with open('data/records.json') as f:
    r = json.load(f)
named = sum(1 for x in r if x.get('owner'))
print(f'Total:  {len(r)}')
print(f'NOF:    {sum(1 for x in r if x["type"]=="NOF")}')
print(f'TAX:    {sum(1 for x in r if x["type"]=="TAX")}')
print(f'Named:  {named}')

