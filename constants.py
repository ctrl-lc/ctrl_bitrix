class Stage(object):
    CALL_CENTER = 'NEW'
    POSTPONED = 9
    LOST = 'LOSE'


class Field(object):
    # последний продавец до возврата из отложенных в КЦ
    LAST_SALESMAN = 'UF_CRM_1610451561'
    BRING_BACK_DUE = 'UF_CRM_1582643149318'  # дата возврата из отложенных
    REASON = 'UF_CRM_1579180371132'  # причина отказа
