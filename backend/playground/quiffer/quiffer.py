import asyncio
import os
import pprint
from pathlib import Path
from datetime import date
from decimal import Decimal
import json
import sys
from qif_reader import QifReader
from money_data import MoneyData
from quif_to_db import create_tables, insert_data_into_db, init_db

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../../src")
from core.app import App


def date_and_decimal_encoder(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def date_and_decimal_decoder(dct):
    # print(f"date_and_decimal_decoder: {dct}")
    for key, value in dct.items():
        if isinstance(value, str):
            try:
                if key in ["amount", "price"]:
                    dct[key] = Decimal(value)
                elif key == "date":
                    dct[key] = date.fromisoformat(value)
                    # print(f"date_and_decimal_decoder: {key}={dct[key]}")
            except ValueError:
                pass
    # print(f"date_and_decimal_decoder: {dct}")
    # raise
    return dct


def categories(data: MoneyData):
    # ids = {c: i + 1 for i, c in enumerate(data.categories)}
    cats = []
    for cat in data.categories.values():
        cats.append(
            (
                cat.id,
                cat.name.split(":")[-1],
                cat.description,
                cat.type,
                data.categories[cat.parent].id if cat.parent is not None else None,
                cat.source_file,
            )
        )
    return cats


def transactions(data: MoneyData, account_id: int = None):
    trans = [
        {
            "date": t.get("date"),
            "amount": t.get("amount"),
            "account": acc.get("name"),
            "contra_account": t.get("contra_account") or "",
            "category": t.get("category") or "",
            "payee": t.get("payee") or "",
            "memo": t.get("memo") or "",
            "key": f'{acc.get("name")}:{t.get("contra_account")}:{t.get("date")}:{t.get("amount")}:{t.get("category")}:{t.get("payee")}:{t.get("memo")}',
            "yek": f'{t.get("contra_account")}:{acc.get("name")}:{t.get("date")}:{-t.get("amount")}:{t.get("category")}:{t.get("payee")}:{t.get("memo")}',
            "has_contra": None,
            "splits": [s for s in t.get("splits") or []],
            "source_file": t.get("source_file") or "",
        }
        for acc in data.get("accounts", {}).values()
        if account_id is None or acc.get("id") == account_id
        for t in acc.get("transactions", [])
    ]
    trans = sorted(trans, key=lambda t: f'{t["date"]}{abs(t["amount"])}{-t["amount"]}')

    next_split_id = 1

    keys = {}
    for i, t in enumerate(trans):
        t["id"] = i + 1
        y = t["key"]
        if y in keys:
            t["duplicate"] = keys[y][0]
        else:
            keys[y] = (i, -1)
            t["duplicate"] = None
        for j, s in enumerate(t["splits"]):
            s["id"] = next_split_id
            next_split_id += 1
            s["transaction_id"] = t["id"]
            skey = f'{t["account"]}:{s.get("contra_account")}:{t["date"]}:{s.get("amount")}:{s.get("category")}:{t["payee"]}:{s.get("memo")}'
            s["key"] = skey
            s["yek"] = (
                f'{s.get("contra_account")}:{t["account"]}:{t["date"]}:{-s.get("amount")}:{s.get("category")}:{t["payee"]}:{s.get("memo")}'
            )
            if skey not in keys:
                keys[skey] = (i, j)

    for i, t in [(t["id"], t) for t in trans]:
        if t["duplicate"] is not None:
            continue
        y = t["yek"]
        if y in keys and t["amount"] < 0:
            if keys[y][1] == -1:
                t["contra_trans"] = trans[keys[y][0]]["id"]
                trans[keys[y][0]]["contra_trans"] = i
            else:
                t["contra_split"] = trans[keys[y][0]]["splits"][keys[y][1]]["id"]
                trans[keys[y][0]]["splits"][keys[y][1]]["contra_trans"] = i
        for j, s in enumerate(t["splits"]):
            y = s["yek"]
            if y in keys and s["amount"] < 0:
                if keys[y][1] == -1:
                    s["contra_trans"] = trans[keys[y][0]]["id"]
                    trans[keys[y][0]]["contra_split"] = s["id"]
                else:
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    return trans


def trans_to_db(data: MoneyData, dups=True):
    splits = []
    trans = []
    for t in transactions(data.as_dict()):
        if dups or t["duplicate"] is None:
            trans.append(
                (
                    t["id"],
                    t["duplicate"] if t["duplicate"] is not None else None,
                    t["date"],
                    # t["credit"],
                    (
                        data.accounts[t["account"]].id
                        if t["account"] in data.accounts
                        else None
                    ),
                    len(t["splits"]),
                    (
                        data.accounts[t["contra_account"]].id
                        if t["contra_account"] in data.accounts
                        else None
                    ),
                    t["amount"],
                    t["payee"],
                    # t["category"],
                    (
                        data.categories[t["category"]].id
                        if t["category"] in data.categories
                        else None
                    ),
                    t["memo"],
                    t["contra_trans"] if t.get("contra_trans") is not None else None,
                    t["contra_split"] if t.get("contra_split") is not None else None,
                    t["key"][:100],
                    t["yek"][:100],
                    t["source_file"],
                )
            )
            for s in t["splits"]:
                splits.append(
                    (
                        s["id"],
                        "DUP" if t["duplicate"] is not None else None,
                        s["transaction_id"],
                        s["amount"],
                        s.get("memo"),
                        (
                            data.categories[s["category"]].id
                            if s.get("category") in data.categories
                            else None
                        ),
                        (
                            data.accounts[s["contra_account"]].id
                            if s.get("contra_account") in data.accounts
                            else None
                        ),
                        s.get("percent"),
                        (
                            s["contra_trans"]
                            if s.get("contra_trans") is not None
                            else None
                        ),
                        (
                            s["contra_split"]
                            if s.get("contra_split") is not None
                            else None
                        ),
                        s["key"][:100],
                        s["yek"][:100],
                        s["source_file"][:100] if s.get("source_file") else None,
                    )
                )
    return trans, splits


def investments(data: MoneyData):
    investments = []
    for acc in data.accounts.values():
        for id, i in enumerate(acc.investments):
            investments.append(
                (
                    id + 1,
                    i.date,
                    i.action,
                    i.security,
                    i.quantity,
                    i.price,
                    i.amount,
                    i.memo,
                    i.cleared,
                    (
                        data.accounts[i.contra_account].id
                        if i.contra_account in data.accounts
                        else None
                    ),
                    i.commission,
                    f"L {i.contra_account}",
                )
            )
    return investments


def balances(data: MoneyData):
    balances = []
    for acc in data.accounts.values():
        for b in acc.balances:
            if b.amount is not None:
                balances.append((acc.id, b.date, b.amount, b.source_file))
    return [
        (i + 1, b[0], b[1], b[2], b[3])
        for i, b in enumerate(
            sorted(balances, key=lambda b: (str(b[1]), str(b[0]), b[3]))
        )
    ]


def accounts(data: MoneyData):
    return [
        (
            data.accounts[acc].id,
            data.accounts[acc].name,
            data.accounts[acc].acc_type.name,
            data.accounts[acc].source_file,
        )
        for acc in data.accounts
    ]


async def main():
    data = None
    init_db()
    while (answer := input("Enter command: ").lower()) != "q":
        if answer == "r":
            path = Path("C:\\Users\\heinz\\Dokumente\\projects\\qiffer\\data")
            data = QifReader(path).read().apply_category_type_heuristics()
        elif answer == "w":
            with open("data.json", "w", encoding="utf-8") as f:
                print(f"Writing to data.json")
                json.dump(
                    data.as_dict(),
                    f,
                    indent=4,
                    ensure_ascii=False,
                    default=date_and_decimal_encoder,
                )
        elif answer == "j":
            with open("data.json", "r", encoding="utf-8") as f:
                print(f"Reading from data.json")
                md = MoneyData()
                md.from_dict(
                    json.load(
                        f, parse_float=Decimal, object_hook=date_and_decimal_decoder
                    )
                )
                data = md
        elif answer == "y":
            if not data:
                print("No data loaded. Please read qif files or json first.")
                continue
            pprint.pprint(
                data.categories,
                sort_dicts=False,
                indent=4,
                width=200,
            )
            # pprint.pprint(
            #     categories(data),
            #     sort_dicts=False,
            #     indent=4,
            #     width=200,
            # )
        elif answer == "a":
            if not data:
                print("No data loaded. Please read qif files or json first.")
                continue
            for acc in data.accounts.values():
                print(f"{acc.id:>5} {acc.acc_type.name:<15} {acc.name}")
        elif answer[0] == "b":
            if not data:
                print("No data loaded. Please read qif files or json first.")
                continue
            i = int(answer[1:]) if len(answer) > 1 else None
            pprint.pprint(
                [
                    {acc.name: sorted(acc.balances, key=lambda b: b.date)}
                    for name, acc in data.accounts.items()
                    if i is None or acc.id == i
                ],
                sort_dicts=False,
                indent=4,
                width=200,
            )
        elif answer[0] == "m":
            if not data:
                print("No data loaded. Please read qif files or json first.")
                continue
            _, a1, a2 = answer.split(" ")
            i1 = int(a1)
            a1 = [a for a in data.accounts.values() if a.id == i1][0]
            i2 = int(a2)
            a2 = [a for a in data.accounts.values() if a.id == i2][0]
            print(f"Merging accounts {i1}: '{a1.name}' and {i2}: '{a2.name}'")
            print(f"  final name: '{a2.name}'")
            a2.balances.extend(a1.balances)
            a2.transactions.extend(a1.transactions)
            a2.investments.extend(a1.investments)
            for a in data.accounts.values():
                for t in a.transactions:
                    if t.contra_acc == a1.name:
                        t.contra_acc = a2.name
                    for s in t.splits:
                        if s.contra_account == a1.name:
                            s.contra_account = a2.name
            for i, a in data.accounts.items():
                if a.id == i1:
                    del data.accounts[i]
                    break
        elif answer[0] == "t":
            if not data:
                print("No data loaded. Please read qif files or json first.")
                continue
            i = int(answer[1:]) if len(answer) > 1 else None
            for t in [
                t
                for t in transactions(data.as_dict(), i)
                # if t["duplicate"] or t["contra"] or t["has_contra"]
            ]:
                print(
                    f"{t['id']:>5} "
                    f"{(t['duplicate'] if t['duplicate'] is not None else ' '):>5}"
                    f"{(t['contra'] if t['contra'] is not None else ' '):>5} "
                    f"{t['date']} "
                    f"{t['amount']:>10.2f} "
                    f"{t['account']:<30} "
                    f"{t['contra_account']:<30} "
                    f"{t['payee']:<30} "
                    f"{t['memo']:<40} "
                    f"{t['category']:<40} "
                    # f"{t['key']:<80} {t['yek']:<80}"
                )
                if t["splits"]:
                    for s in t["splits"]:
                        print(
                            " " * 36,
                            f"{s['amount']:>10.2f} ",
                            " " * 91,
                            f"{s['memo'] if s['memo'] is not None else '':<40} "
                            f"{s['category'] if s['category'] is not None else '-------':<40} "
                            f"{s['contra_account'] if s['contra_account'] is not None else '':<30} "
                            f"{s['percent'] if s['percent'] is not None else '':>4.2f}",
                            f'{s["source_file"] if s["source_file"] is not None else ""}',
                        )
        elif answer == "s":
            print(f"{list(data.accounts.values())[0].balances[0].date}")
        elif answer == "c":
            print(f"Creating tables in database")
            await create_tables()
        elif answer[0] == "i":
            if not data:
                print("No data loaded. Please read qif files or json first.")
                continue
            x = answer[1:] if len(answer) > 1 else "x"
            print(f"Inserting data into database (x={x})")
            if x in ["x", "y"]:
                print(f"Inserting categories into database")
                await insert_data_into_db("qif_categories", categories(data))
            if x in ["x", "a"]:
                print(f"Inserting accounts into database")
                await insert_data_into_db("qif_accounts", accounts(data))
            if x in ["x", "t"]:
                ts, ss = trans_to_db(data)
                print(f"Inserting {len(ts)} transactions into database")
                # pprint.pprint(ts[:10], sort_dicts=False, indent=4, width=200)
                await insert_data_into_db("qif_transactions", ts)
                print(f"Inserting {len(ss)} splits into database")
                # pprint.pprint(ss[:10], sort_dicts=False, indent=4, width=40)
                await insert_data_into_db("qif_splits", ss)
            if x in ["x", "b"]:
                bs = balances(data)
                print(f"Inserting {len(bs)} balances into database")
                # pprint.pprint(bs, sort_dicts=False, indent=4, width=200)
                await insert_data_into_db("qif_balances", bs)
            if x in ["x", "v"]:
                is_ = investments(data)
                print(f"Inserting {len(is_)} investments into database")
                # pprint.pprint(is_, sort_dicts=False, indent=4, width=200)
                await insert_data_into_db("qif_investments", is_)
        else:
            print("Commands:")
            print("  h - help")
            print("  q - quit")
            print("  r - read qif files into memory")
            print("  w - write data.json from memory")
            print("  j - read data.json and load into memory")
            print("  y - show categories")
            print("  a - show accounts")
            print("  b - show balances")
            print("  t - show transactions")
            print("  c - create tables in database")
            print("  i - insert data into database")
            print("  ix- insert data into database, x = y,a")

    # path = Path("data")
    # data = QifReader(path).read().apply_category_type_heuristics()
    # show = data.as_dict()
    # # show = [
    # #     (c.hierarchy, c.type) for c in data.categories.values() if c.type == "INCOME"
    # # ]
    # pprint.pprint(
    #     show,
    #     sort_dicts=False,
    #     indent=4,
    #     width=200,
    #     # compact=True,
    # )


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    App.initialize(__file__)
    asyncio.run(main())
