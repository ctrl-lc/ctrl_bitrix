'''
Этот скрипт необходим для того, чтобы обеспечить многоразовый возврат сделок
из "Отложенных".

Роботы Битрикса отрабатывают по сделке только один раз, поэтому для второго
и следующих возвратов требуется этот скрипт.
'''

import datetime
import random

from lxutils import log, config
from fast_bitrix24 import Bitrix

b = Bitrix(config['tokens']['webhook'])

log("Downloading deals")
deals = b.get_all('crm.deal.list', params={
    'filter': {
        'STAGE_ID': 9,  # отложенные
        # дата возврата между датой, когда роботы перестали
        # создавать копию из отложенных, и сегодня
        '>UF_CRM_1582643149318': '2020-05-25',
        '<UF_CRM_1582643149318': str(datetime.date.today())
    },
    'select': ['ID', 'ASSIGNED_BY_ID', 'TITLE']
})

if len(deals) > 0:
    log("Downloading employees to assign")
    employees = b.get_all('user.get', params={
        'filter': {
            'UF_DEPARTMENT': 24  # КЦ
        }
    })

    log(f"Возвращаю {len(deals)} сделок из отложенных")
    assert all(b.call('crm.deal.update', [{
        'id': d['ID'],
        'fields': {
            'STAGE_ID': 'NEW',  # первый этап
            'UF_CRM_1582643149318': '',  # стираем дату возврата

            # добавляем в название сделки "из отложенных",
            # если такой фразы там еще нет
            'TITLE': d['TITLE'] + (
                ' - из отложенных'
                if 'из отложенных' not in d['TITLE'].lower()
                else ''
            ),

            # если был назначен кто-то из КЦ, то не меняем,
            # иначе назначаем кого-то из КЦ
            'ASSIGNED_BY_ID':
                random.choice(employees)['ID']
                if d['ASSIGNED_BY_ID'] not in [e['ID'] for e in employees]
                else d['ASSIGNED_BY_ID']

        }
    } for d in deals]))

    log('Adding comments')

    tasks = [{
        'fields': {
            'ENTITY_ID': int(d['ID']),
            'ENTITY_TYPE': 'deal',
            'COMMENT': 'Автоматически возвращено из отложенных',
        }
    } for d in deals]

    b.call('crm.timeline.comment.add', tasks)

    log(f'{len(deals)} postponed deals processed. All done!')
else:
    log('Nothing to process :(')
