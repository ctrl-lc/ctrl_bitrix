'''
Этот скрипт необходим для того, чтобы обеспечить многоразовый возврат сделок
из "Отложенных".

Роботы Битрикса отрабатывают по сделке только один раз, поэтому для второго
и следующих возвратов требуется этот скрипт.

Скрипт также проставляет поле "Прошлый продавец", окторое используется
роботами на втором этапе для назначения ответственного.
'''

import datetime
import random

from lxutils import log, config
from fast_bitrix24 import Bitrix


LAST_SALESMAN_FIELD = 'UF_CRM_1610451561'
BRING_BACK_DUE_FIELD = 'UF_CRM_1582643149318'


b = Bitrix(config['tokens']['webhook'])


def main():
    deals = download_postponed_deals()

    salespeople_IDs = download_salespeople()
    save_last_salesman(deals, salespeople_IDs)

    due = pick_due(deals)

    if len(due) > 0:
        CC_employee_IDs = get_CC_employees()
        bring_back(due, CC_employee_IDs)
        add_comments(due)

        log(f'{len(due)} postponed deals processed. All done!')
    else:
        log('Nothing to process :(')


def download_postponed_deals():
    log("Downloading postponed deals")
    return b.get_all('crm.deal.list', params={
        'filter': {
            'STAGE_ID': 9,  # отложенные
        },
        'select': ['ID', 'ASSIGNED_BY_ID', 'TITLE', BRING_BACK_DUE_FIELD]
    })


def download_salespeople():
    log("Downloading salespeople")
    return [x['ID'] for x in b.get_all('user.get', params={
        'filter': {
            'UF_DEPARTMENT': 16  # Продавцы по б/у грузовикам
        }
    })]


def save_last_salesman(deals, salespeople_IDs):
    log('Saving last salesman')

    save_list = [{
        'ID': deal['ID'],
        'fields': {
            LAST_SALESMAN_FIELD: deal['ASSIGNED_BY_ID']  # Последний продавец
        }
    } for deal in deals if deal['ASSIGNED_BY_ID'] in salespeople_IDs]

    b.call('crm.deal.update', save_list)

    log(f'{len(save_list)} salespeople saved')


def pick_due(deals):
    log('Filtering deals to bring back')
    str_today = str(datetime.date.today())
    return [deal for deal in deals
            if '2020-05-25' < deal[BRING_BACK_DUE_FIELD] <= str_today]


def get_CC_employees():
    log("Downloading СС employees to assign")
    return [x['ID'] for x in b.get_all('user.get', params={
        'filter': {
            'UF_DEPARTMENT': 24  # КЦ
        }
    })]


def bring_back(due, CC_employee_IDs):
    log(f"Возвращаю {len(due)} сделок из отложенных")

    assert all(b.call('crm.deal.update', [{
        'id': d['ID'],
        'fields': {
            'STAGE_ID': 'NEW',  # первый этап
            BRING_BACK_DUE_FIELD: '',  # стираем дату возврата

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
                random.choice(CC_employee_IDs)
                if d['ASSIGNED_BY_ID'] not in CC_employee_IDs
                else d['ASSIGNED_BY_ID']
        }
    } for d in due]))


def add_comments(due):
    log('Adding comments')

    tasks = [{
        'fields': {
            'ENTITY_ID': int(d['ID']),
            'ENTITY_TYPE': 'deal',
            'COMMENT': 'Автоматически возвращено из отложенных',
        }
    } for d in due]

    b.call('crm.timeline.comment.add', tasks)


main()
