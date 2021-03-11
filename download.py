import datetime

import pandas as pd
from fast_bitrix24 import Bitrix
from lxutils import config
from lxutils.log import log

b = Bitrix(config["tokens"]["webhook"])


def get_users(b):
    log("Downloading users")
    users = b.get_all("user.get")
    return users


def get_stages(b):
    log("Downloading stages")
    stages = b.get_all("crm.dealcategory.stage.list")
    return stages


def user_id_to_name(*, data, user_field, users):
    try:
        user_record = next(u for u in users if u["ID"] == data[user_field])
        data[user_field] = user_record["LAST_NAME"]

    except StopIteration:
        pass

    return data


def get_deals(b, users, stages):
    log("Downloading deals")
    fields = [
        "ID",
        "STAGE_ID",
        "ASSIGNED_BY_ID",
        "OPPORTUNITY",
        "BEGINDATE",
        "CLOSEDATE",
        "CLOSED",
        "SOURCE_ID",
        "SOURCE_DESCRIPTION",
        "UTM_SOURCE",
        "TITLE",
        "DATE_CREATE",
        "UF_CRM_1579180371132",
        "UF_CRM_5E20307D5B33E",
    ]
    deals = b.get_all("crm.deal.list", params={"select": fields})

    # проверяем, что выгрузились все поля
    if len(deals[0]) < len(fields):
        log(f"Fields not found: {fields - deals[0].keys()}")

    log("Transforming deals")
    for d in deals:
        # в базе есть некоторые сделки со статусами, для которых нет соответствия в списке статусов
        try:
            d["STAGE_ID"] = next(s for s in stages if s["STATUS_ID"] == d["STAGE_ID"])[
                "NAME"
            ]
        except StopIteration:
            pass

        user_id_to_name(data=d, user_field="ASSIGNED_BY_ID", users=users)

        d["URL"] = "https://ctrlcrm.bitrix24.ru/crm/deal/details/" + d["ID"] + "/"

    log("Writing deals to file")
    df = pd.DataFrame(data=deals).drop_duplicates("ID")
    df.to_csv("deals.csv", index=False)

    return deals


def get_activities(b, users):
    log("Downloading deal activities")
    fields = [
        "START_TIME",
        "DEADLINE",
        "OWNER_ID",
        "RESPONSIBLE_ID",
        "COMPLETED",
        "END_TIME",
        "ID",
    ]
    activities = b.get_all("crm.activity.list", params={"select": fields})
    log(
        f'{pd.DataFrame(activities).drop_duplicates("ID").shape[0]} activities downloaded'
    )

    log("Transforming activities")
    for a in activities:
        user_id_to_name(data=a, user_field="RESPONSIBLE_ID", users=users)

    log("Writing activities to file")
    df = pd.DataFrame(activities)
    df = df.drop_duplicates("ID")
    df.to_csv("activities.csv", index=False, encoding="utf-8")

    log(f"{df.shape[0]} activities written to file")


def get_calls(b, users):
    log("Downloading calls")
    calls = b.get_all("voximplant.statistic.get")
    log(f"{pd.DataFrame(calls).shape[0]} calls downloaded")

    for c in calls:
        user_id_to_name(data=c, user_field="PORTAL_USER_ID", users=users)

    log("Writing calls to file")
    df = pd.DataFrame(calls)
    df.to_csv("calls.csv", index=False, encoding="utf-8")

    log(f"{df.shape[0]} calls written to file")


def get_contacts(b: Bitrix, deals):
    log("Downloading contacts")
    fields = [
        "DATE_CREATE",
        "NAME",
        "LAST_NAME",
    ]
    contacts = b.get_all("crm.contact.list", params={"select": fields})
    log(f"{len(contacts)} contacts downloaded")

    log("Downloading relations from deals to contacts")
    rels = b.get_by_ID("crm.deal.contact.items.get", [d["ID"] for d in deals])
    log(f"{len(rels)} relationships downloaded")

    log("Finding deals for contacts")
    found = 0
    for i, contact in enumerate(contacts):
        found_deal_id = None

        for deal_id, deal_rels in rels.items():
            for rel in deal_rels:
                if int(rel["CONTACT_ID"]) == int(contact["ID"]):
                    found_deal_id = deal_id
                    break

            if found_deal_id:
                break

        if found_deal_id:
            found += 1
            contacts[i]["DEAL_ID"] = found_deal_id

    log(f"{found} deals found")

    log("Writing contacts to file")
    df = pd.DataFrame(contacts)
    df.to_csv("contacts.csv", index=False, encoding="utf-8")

    log(f"{df.shape[0]} contacts written to file")


def get_deals_status_pivot(deals):
    log("Calculating deals status pivot")
    df = (
        pd.DataFrame(data=deals)[["STAGE_ID", "ASSIGNED_BY_ID", "ID"]]
        .groupby(["STAGE_ID", "ASSIGNED_BY_ID"])
        .count()
    )
    df["date"] = datetime.datetime.today().strftime("%Y-%m-%d")

    log("Reading deal status history file")
    hist = pd.read_csv("deal_pivot.csv", index_col=["STAGE_ID", "ASSIGNED_BY_ID"])
    df = df.append(hist[hist["date"] != datetime.datetime.today().strftime("%Y-%m-%d")])

    log("Writing deals status pivot to file")
    df.to_csv("deal_pivot.csv", sep=",", index=True)


users = get_users(b)
stages = get_stages(b)
deals = get_deals(b, users, stages)
get_activities(b, users)
get_calls(b, users)
get_contacts(b, deals)
get_deals_status_pivot(deals)
