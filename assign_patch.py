import itertools
from lxutils import *
from fast_bitrix24 import Bitrix

b = Bitrix("https://ctrlcrm.bitrix24.ru/rest/1/0agnq1xt4xv1cqnc/") 

with timer("Downloading deals"):
    deals = b.get_all('crm.deal.list', params = {
        'select': ['ID', 'ASSIGNED_BY_ID'],
        'filter': {
            'CLOSED': 'N'
        }
    })
    deals.sort(key = lambda d: int(d['ID']))

with timer('Getting contacts'):
    contacts_by_deal = b.get_by_ID('crm.deal.contact.items.get',
        [d['ID'] for d in deals],
        params = {'select': ['CONTACT_ID']}
    )
    contacts_by_deal.sort(key = lambda cbd: int(cbd[0]))
    
with timer('Transforming data'):
    to_unpack = [
        [
            (
                int(c['CONTACT_ID']),
                int(d['ASSIGNED_BY_ID'])
            )
            for c in contacts_by_deal[i][1]
        ]
        for i, d in enumerate(deals)]

    unpacked = list(itertools.chain(*to_unpack))
    
    patch_tasks = [
        {
            'ID': pt[0],
            'fields': {
                'ASSIGNED_BY_ID': pt[1]
            }
        } for pt in unpacked
    ]

with timer('Patching assignments'):
    r = b.call('crm.contact.update', patch_tasks)
    log(f'Done with {len(list(filter(lambda l: not l, r)))} errors')