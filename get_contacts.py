import pandas as pd

from lxutils import config
from fast_bitrix24 import Bitrix
from itertools import chain


b = Bitrix(config['tokens']['webhook'])

deals = pd.DataFrame(b.get_all('crm.deal.list'))

# deals = deals[:-10]

timeline = b.call('crm.timeline.comment.list',
                  [{
                      'FILTER': {
                          'ENTITY_TYPE': 'deal',
                          'ENTITY_ID': deal,
                      },
                  } for deal in deals['ID']])

deals['num_comments'] = [len(deal_timeline) for deal_timeline in timeline]
deals['latest_comment'] = [
    max(deal_timeline, key=lambda x: x['CREATED'])['CREATED']
    if deal_timeline else None
    for deal_timeline in timeline]

# отобрать тех, у кого статус - проиграна, было более-менее плотное общение,
# и прошло уже 2 мес.

deals = deals.query('STAGE_ID == "LOSE" and num_comments >= 3 '
                    'and latest_comment <= "2020-11-01"')

contact_refs = b.get_by_ID('crm.deal.contact.items.get', deals['ID'])

contact_refs_chained = chain(*contact_refs.values())
IDs = [contact_ref['CONTACT_ID'] for contact_ref in contact_refs_chained]

contacts = b.get_by_ID('crm.contact.get', IDs)

contacts_stripped = [{'name': cont['NAME'], 'phone': cont['PHONE']}
                     for cont in contacts.values() if 'PHONE' in cont]

phones_unpacked = chain(*[
    [{'name': cont['name'], 'phone': phone['VALUE']}
     for phone in cont['phone']]
    for cont in contacts_stripped])

phones = pd.DataFrame(phones_unpacked)

# удаляем все символы, кроме цифр

phones['phone'] = [
    ''.join(char for char in phone if char in '0123456789')
    for phone in phones['phone']
]

# удаляем дубли

phones = phones.drop_duplicates('phone')

phones.to_csv('phones.csv', index=False)
