'''
Этот скрипт проставляет задачи на сделки в работе без незакрытых задач.
'''

import pandas as pd
import datetime
import random

from lxutils import log
from fast_bitrix24 import Bitrix

b = Bitrix("https://ctrlcrm.bitrix24.ru/rest/1/0agnq1xt4xv1cqnc/")

log("Downloading deals")
deals = b.get_all('crm.deal.list', params={
    'filter': {
        'CLOSED': 'N' # берем только сделки в работе
    },
    'select': ['ID', 'ASSIGNED_BY_ID']
})

if len(deals) > 0:
    log("Downloading activities")
    activities = b.call('crm.activity.list', [{
        'filter': {
            'OWNER_ID': d['ID'],
            'OWNER_TYPE_ID': 2, # только по сделкам
            'COMPLETED': 'N' # берем только незакрытые задачи
        }
    } for d in deals])

    # убираем сделки, в которых есть активные задачи
    deals = [d for d in deals if d['ID'] not in [a['OWNER_ID'] for a in activities]]

    if len(deals) > 0:
        log("Downloading contacts")
        deals_and_contact_ids = b.get_by_ID('crm.deal.contact.items.get',
            ID_list = [d['ID'] for d in deals])

        # добавляем в deals id основного контакта
        # сначала сортируем оба массива по id сделки
        deals.sort(key = lambda d: d['ID'])
        deals_and_contact_ids.sort(key = lambda dc: dc[0])
        for i, dc in enumerate(deals_and_contact_ids):
            # перебираем все контакты в сделке
            for c in dc[1]:
                # и если контакт - основной, то записываем в сделку его id
                if c['IS_PRIMARY'] == 'Y':
                    deals[i]['CONTACT_ID'] = c['CONTACT_ID']
                    break
            else:
                if len(dc[1]) > 0:
                    raise RuntimeError(
                        f'Сделка {dc[0]}: {len(dc[1])} контактов, но не одного основного')    
        
        # берем все данные отобранных контактов
        contacts = b.get_by_ID('crm.contact.get', 
            ID_list = [d['CONTACT_ID'] for d in deals 
                if 'CONTACT_ID' in d.keys()],
            params = {
                'select': ['ID', 'PHONE', 'NAME', 'LAST_NAME']
            })

        # причесываем контакты
        contacts_processed = {}
        for contact_id, content in contacts:
            contacts_processed.update({
                # добавляем первый телефонный номер
                int(contact_id): content['PHONE'][0]['VALUE']
            })

        # добавляем телефоны из contacts в deals
        for i, d in enumerate(deals):
            assert d['ID'] == deals[i]['ID']
            # если контакт заполнен и его id совпадает с искомым
            if ('CONTACT_ID' in d.keys()) and (d['CONTACT_ID'] in contacts_processed.keys()):
                deals[i]['PHONE'] = contacts_processed[d['CONTACT_ID']]
                
        log('Adding tasks')
        
        new_tasks = [{
            'fields': {
                "OWNER_TYPE_ID": 2, # из метода crm.enum.ownertype: 2 - тип "сделка"
                "OWNER_ID": d['ID'], # id сделки
                "TYPE_ID": 2, # звонок - из метода crm.enum.activitytype
                "SUBJECT": "Позвонить клиенту",
                "START_TIME": datetime.date.today(),
                "COMMUNICATIONS": [{
                    'VALUE': d['PHONE'],
                    'ENTITY_ID': d['CONTACT_ID'],
                    'ENTITY_TYPE': 3 # тип контакт
                }],
                "COMPLETED": "N",
                "RESPONSIBLE_ID": d['ASSIGNED_BY_ID']
            }
        } for i, d in enumerate(deals) if (
            ('PHONE' in d.keys()) and ('CONTACT_ID' in d.keys())
        )]

        if len(new_tasks) > 0:
            b.call('crm.activity.add', new_tasks)
            log(f'{len(new_tasks)} deals with no open tasks processed')
        else:
            log('No active deals without open tasks with contacts :(')
    else:
        log('No active deals without open tasks :(')
else:
    log('No active deals :(')

log('All done!')