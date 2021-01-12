'''
Проставляет задачи на сделки в работе без незакрытых задач.
'''

import datetime

from lxutils import log, config
from fast_bitrix24 import Bitrix


class TaskMonitor:

    def __init__(self):
        self.b = Bitrix(config['tokens']['webhook'])

    def main(self):
        self.download_deals()
        self.download_activities()
        if len(self.deals) > 0:
            self.keep_only_deals_without_tasks()
            if len(self.deals) > 0:
                self.download_contact_ids_from_deals()
                self.add_contact_ids_to_deals()
                self.download_contacts_details()
                self.add_phones_to_deals()
                self.compose_new_tasks()
                if len(self.new_tasks) > 0:
                    self.upload_new_tasks()
                else:
                    log('No active deals without open tasks with contacts :(')
            else:
                log('No active deals without open tasks :(')
        else:
            log('No active deals :(')
        log('All done!')

    def download_deals(self):
        log("Downloading deals")
        self.deals = self.b.get_all('crm.deal.list', params={
            'filter': {
                'CLOSED': 'N'  # берем только сделки в работе
            },
            'select': ['ID', 'ASSIGNED_BY_ID']
        })

    def download_activities(self):
        log("Downloading activities")
        self.activities = self.b.call('crm.activity.list', [{
            'filter': {
                'OWNER_ID': d['ID'],
                'OWNER_TYPE_ID': 2,  # только по сделкам
                'COMPLETED': 'N'  # берем только незакрытые задачи
            }
        } for d in self.deals])

    def keep_only_deals_without_tasks(self):
        # убираем сделки, в которых есть активные задачи
        owner_IDs = (a['OWNER_ID'] for a in self.activities)
        self.deals = [d for d in self.deals
                      if d['ID'] not in owner_IDs]

    def download_contact_ids_from_deals(self):
        log("Downloading contacts")
        self.deals_and_contact_ids = self.b.get_by_ID(
            'crm.deal.contact.items.get',
            ID_list=[d['ID'] for d in self.deals])

    def add_contact_ids_to_deals(self):
        self.deals.sort(key=lambda d: d['ID'])
        self.deals_and_contact_ids.sort(key=lambda dc: dc[0])

        for i, dc in enumerate(self.deals_and_contact_ids):
            # перебираем все контакты в сделке
            for c in dc[1]:
                # и если контакт - основной, то записываем в сделку его id
                if c['IS_PRIMARY'] == 'Y':
                    self.deals[i]['CONTACT_ID'] = c['CONTACT_ID']
                    break
            else:
                if len(dc[1]) > 0:
                    raise RuntimeError(
                        f'Сделка {dc[0]}: {len(dc[1])} контактов, '
                        'но ни одного основного')

    def download_contacts_details(self):
        # берем все данные отобранных контактов
        self.contacts = self.b.get_by_ID(
            'crm.contact.get',
            ID_list=[d['CONTACT_ID'] for d in self.deals
                     if 'CONTACT_ID' in d.keys()],
            params={
                'select': ['ID', 'PHONE', 'NAME', 'LAST_NAME']
            })

    def add_phones_to_deals(self):
        # причесываем контакты
        contacts_processed = {}
        for contact_id, content in self.contacts:
            contacts_processed.update({
                # добавляем первый телефонный номер
                int(contact_id): content['PHONE'][0]['VALUE']
            })

        # добавляем телефоны из contacts в deals
        for i, d in enumerate(self.deals):
            assert d['ID'] == self.deals[i]['ID']
            # если контакт заполнен и его id совпадает с искомым
            if ('CONTACT_ID' in d.keys() and
                    d['CONTACT_ID'] in contacts_processed):
                self.deals[i]['PHONE'] = contacts_processed[d['CONTACT_ID']]

    def compose_new_tasks(self):
        self.new_tasks = [{
            'fields': {
                # из метода crm.enum.ownertype: 2 - тип "сделка"
                "OWNER_TYPE_ID": 2,
                "OWNER_ID": d['ID'],  # id сделки
                "TYPE_ID": 2,  # звонок - из метода crm.enum.activitytype
                "SUBJECT": "Позвонить клиенту",
                "START_TIME": datetime.date.today(),
                "COMMUNICATIONS": [{
                    'VALUE': d['PHONE'],
                    'ENTITY_ID': d['CONTACT_ID'],
                    'ENTITY_TYPE': 3  # тип контакт
                }],
                "COMPLETED": "N",
                "RESPONSIBLE_ID": d['ASSIGNED_BY_ID']
            }
        } for i, d in enumerate(self.deals)
          if {'PHONE', 'CONTACT_ID'} <= d.keys()]

    def upload_new_tasks(self):
        log('Adding tasks')
        self.b.call('crm.activity.add', self.new_tasks)
        log(f'{len(self.new_tasks)} deals with no open tasks corrected')


TaskMonitor().main()
