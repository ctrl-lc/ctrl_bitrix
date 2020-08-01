'''
Этот скрипт необходим для того, чтобы обеспечить многоразовый возврат сделок из "Отложенных".

Роботы Битрикса отрабатывают по сделке только один раз, поэтому для второго и следующих
возвратов требуется этот скрипт.
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
        'STAGE_ID': 9,  # отложенные
        # дата возврата между датой, когда роботы перестали
        # создавать копию из отложенных, и сегодня
        '>UF_CRM_1582643149318': '2020-05-25',
        '<UF_CRM_1582643149318': datetime.date.today()
    },
    'select': ['ID', 'ASSIGNED_BY_ID']
})

log("Downloading employees to assign")
employees = b.get_all('user.get', params={
    'filter': {
        'UF_DEPARTMENT': 24  # КЦ
    }
})

log("Transferring deals")
assert all(b.call('crm.deal.update', [{
    'id': d['ID'],
    'fields': {
        'STAGE_ID': 'NEW',
        'UF_CRM_1582643149318': '',

        # если был назначен кто-то из КЦ, то не меняем, 
        # иначе назначаем кого-то из КЦ
        'ASSIGNED_BY_ID': random.choice(employees)['ID']
            if d['ASSIGNED_BY_ID'] not in [e['ID'] for e in employees] 
            else d['ASSIGNED_BY_ID']
    }
} for d in deals]))

log('Adding comments')
b.call('crm.timeline.comment.add', [{
    'fields': {
        'ENTITY_ID': int(d['ID']),
        'ENTITY_TYPE': 'deal',
        'COMMENT': 'Автоматически возвращено из отложенных'
    }
} for d in deals])
