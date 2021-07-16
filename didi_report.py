from codecs import encode
from io import StringIO

from fast_bitrix24 import Bitrix
from gspread import service_account
from lxutils import config
from lxutils.log import timer
from pandas import DataFrame

from constants import Field

bx = Bitrix(config["tokens"]["webhook"])


def main():
    df = get_didi_deals()
    df = add_contacts(df)
    df = add_stage_names(df)
    df = add_assigned(df)
    df = filter_and_rename(df)
    upload_to_gsheets(df)


def get_didi_deals():
    deals = bx.get_all(
        "crm.deal.list", {"select": ["*", "UF_*"], "filter": {"CATEGORY_ID": 6}}
    )

    return DataFrame(deals)


def add_contacts(df):
    contacts = bx.get_by_ID("crm.contact.get", df["CONTACT_ID"])
    flat = [flatten(content) for _, content in contacts.items()]
    return df.merge(DataFrame(flat), how="left", left_on="CONTACT_ID", right_on="ID")


def flatten(contact: dict):
    for field in ["PHONE", "EMAIL", "WEB"]:
        if field in contact:
            content = contact[field][0]
            new_fields = {
                    f"CONTACT_{field}": value
                    for key, value in content.items()
                    if key == "VALUE"
                }
            contact.update(new_fields)
            contact.pop(field)
    return contact


def add_stage_names(df):
    stages = bx.get_all(
        "crm.dealcategory.stage.list", {"select": ["STATUS_ID", "NAME"]}
    )
    return df.merge(
        DataFrame(stages), how="left", left_on="STAGE_ID", right_on="STATUS_ID"
    )


def add_assigned(df):
    users = bx.get_all("user.get")
    return df.merge(
        DataFrame(users), how="left", left_on="ASSIGNED_BY_ID_x", right_on="ID"
    )


def filter_and_rename(df):
    RENAME_AND_KEEP = {
        "ID_x": "ID сделки",
        "DATE_CREATE_x": "Получена",
        "UTM_SOURCE_x": "Источник",
        "NAME_x": "Имя клиента",
        "UF_CRM_60A371AD2EB58": "Название парка",
        "CONTACT_PHONE": "Телефон",
        "CONTACT_EMAIL": "Почта",
        "NAME_y": "Статус",
        "LAST_NAME_y": "Сотрудник CTRL",
        "UF_CRM_609E73EBA2F67": "ИНН",
        "UF_CRM_609E73EBC9318": "Город регистрации автопарка",
        "UF_CRM_609E73EBED9DD": "Номер договора с Агрегатором",
        "UF_CRM_609E73EC142BF": "Кол-во машин в автопарке",
        "UF_CRM_609E73EC9EB98": "Желаемый аванс",
        "UF_CRM_60A371ADDA256": "Размер сделки (количество авто)",
        "UF_CRM_60A371AE272E3": "Марка/модель",
        "UF_CRM_60A371AE78A7A": "Менеджер DIDI",
        Field.BRING_BACK_DUE: "Дата возврата из отложенных",
        "UF_CRM_1624037474589": "Кол-во выданных машин",
        "UF_CRM_1606205926614": "Дата выдачи",
        "UF_CRM_1613575670261": "Причина отказа",
        "UF_CRM_1579180371132": "Причина отказа - комментарий",
        "CLOSEDATE": "Закрыта",
    }
    return DataFrame(
        [
            {RENAME_AND_KEEP[key]: tup._asdict()[key] for key in RENAME_AND_KEEP}
            for tup in df.itertuples()
        ]
    )


def upload_to_gsheets(df):
    with timer("Loading to Google Sheets"):
        gc = service_account(filename="robotic-rampart-255014-e2f22bfae60e.json")
        with StringIO() as content:
            df.to_csv(content, index=False, encoding="utf-8")
            gc.import_csv(
                "154ic4Vq9SBKJQrLD6b_DbyHYK9E3k1QvzS1bB0tt1Kc",
                encode(content.getvalue(), "utf-8"),
            )


main()
