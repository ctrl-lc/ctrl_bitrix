from typing import ChainMap
from fast_bitrix24 import Bitrix
from lxutils import config, timer
from pandas import DataFrame, concat
from gspread import service_account
from io import StringIO
from codecs import encode


bx = Bitrix(config["tokens"]["webhook"])


def main():
    df = get_ati_deals()
    df = add_contacts(df)
    df = add_stage_names(df)
    df = add_assigned(df)
    df = filter_and_rename(df)
    upload_to_gsheets(df)


def get_ati_deals():
    # скачать сделки с правильными utm_source
    FIELDS = ["*", "UF_*"]
    ATI_SOURCES = ["ati_button", "ati_partner_link"]

    deals = bx.get_all(
        "crm.deal.list", {"select": FIELDS, "filter": {"UTM_SOURCE": ATI_SOURCES}}
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
            new_fields = dict(
                {
                    f"CONTACT_{field}": value
                    for key, value in content.items()
                    if key == "VALUE"
                }
            )
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
        "CONTACT_PHONE": "Телефон",
        "CONTACT_EMAIL": "Почта",
        "CONTACT_WEB": "Объявление",
        "NAME_y": "Статус",
        "LAST_NAME_y": "Сотрудник CTRL",
        "CLOSEDATE": "Закрыта",
    }
    return DataFrame(
        [
            {RENAME_AND_KEEP[key]: tup._asdict()[key] for key in RENAME_AND_KEEP}
            for tup in df.itertuples()
        ]
    )


def upload_to_gsheets(df):
    with timer("Loading stocks to Google Sheets"):
        gc = service_account(filename="robotic-rampart-255014-e2f22bfae60e.json")
        with StringIO() as content:
            df.to_csv(content, index=False, encoding="utf-8")
            gc.import_csv(
                "1IsJHUSLwepYjuH9FO8E9Wo2wqs9-y9LcAfF9XExJ7Wg",
                encode(content.getvalue(), "utf-8"),
            )


main()
