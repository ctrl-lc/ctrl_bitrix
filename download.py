import pandas, datetime

from lxutils import log, config
from fast_bitrix24 import Bitrix

b = Bitrix(config['tokens']['webhook'])

log ("Downloading users")
users = b.get_all('user.get')

log ("Downloading stages")
stages = b.get_all('crm.dealcategory.stage.list')

log ("Downloading deals")
fields = ['ID', 'STAGE_ID', 'ASSIGNED_BY_ID', 'OPPORTUNITY', 'BEGINDATE',
    'CLOSEDATE', 'CLOSED', 'SOURCE_ID', 'SOURCE_DESCRIPTION', 'UTM_SOURCE',
    'TITLE', 'DATE_CREATE', 'UF_CRM_1579180371132', 'UF_CRM_5E20307D5B33E']
deals = b.get_all('crm.deal.list', params = {'select': fields})

# проверяем, что выгрузились все поля
if len(deals[0]) < len(fields):
    log(f'Fields not found: {fields - deals[0].keys()}')

log ("Transforming deals")
for d in deals:
    # в базе есть некоторые сделки со статусами, для которых нет соответствия в списке статусов
    try:
        d['STAGE_ID'] = next(s for s in stages if s['STATUS_ID'] == d['STAGE_ID'])['NAME']
    except StopIteration:
        pass
    d['ASSIGNED_BY_ID'] = next(u for u in users if u['ID'] == d['ASSIGNED_BY_ID'])['LAST_NAME']
    d['URL']='https://ctrlcrm.bitrix24.ru/crm/deal/details/' + d['ID'] + '/'

log ("Writing deals to file")
df = pandas.DataFrame(data = deals).drop_duplicates('ID')
df.to_csv("deals.csv", index = False)

log ("Downloading deal activities")

fields = ['START_TIME', 'DEADLINE', 'OWNER_ID', 'RESPONSIBLE_ID', 'COMPLETED', 'END_TIME', 'ID']

activities = b.get_all('crm.activity.list', params={
    'select': fields
})

log (f'{pandas.DataFrame(activities).drop_duplicates("ID").shape[0]} activities downloaded')

log ("Transforming activities")
for a in activities:
    try:
        a['RESPONSIBLE_ID'] = next(u for u in users if int(u['ID'])==int(a['RESPONSIBLE_ID']))['LAST_NAME']
    except:
        pass

log ("Writing activities to file")
df = pandas.DataFrame(activities)
df = df.drop_duplicates('ID')
df.to_csv("activities.csv", index=False, encoding='utf-8')
log (f'{df.shape[0]} activities written to file')

log ('Calculating deals status pivot')
df = pandas.DataFrame(data=deals)[['STAGE_ID', 'ASSIGNED_BY_ID', 'ID']] \
        .groupby(['STAGE_ID', 'ASSIGNED_BY_ID']).count()
df['date'] = datetime.datetime.today().strftime('%Y-%m-%d')

log ('Reading deal status history file')
hist = pandas.read_csv('deal_pivot.csv', index_col = ['STAGE_ID', 'ASSIGNED_BY_ID'])
df = df.append(hist[hist['date']!=datetime.datetime.today().strftime('%Y-%m-%d')])

log ("Writing deals status pivot to file")
df.to_csv("deal_pivot.csv", sep=',', index=True)