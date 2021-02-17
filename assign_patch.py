import itertools
from lxutils.log import timer, log
from lxutils import config
from fast_bitrix24 import Bitrix

b = Bitrix(config['tokens']['webhook'])

with timer("Downloading deals"):
    deals = b.get_all('crm.deal.list', params={
        'select': ['ID', 'ASSIGNED_BY_ID'],
        'filter': {'CLOSED': 'N'}
    })

with timer('Getting contacts'):
    contacts_by_deal = b.get_by_ID('crm.deal.contact.items.get',
                                   [d['ID'] for d in deals],
                                   params={'select': ['CONTACT_ID']})

with timer('Transforming data'):
    to_unpack = [
        [
            (
                int(c['CONTACT_ID']),
                int(d['ASSIGNED_BY_ID'])
            )
            for c in contacts_by_deal[d['ID']]
        ]
        for d in deals]

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
