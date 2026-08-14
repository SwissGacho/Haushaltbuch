import decimal
import quiffen


class Category:
    _next_id = 1

    def __init__(
        self,
        cat: quiffen.Category,
        name=None,
        description=None,
        hierarchy=None,
        parent=None,
        source_file: str | None = None,
    ):
        self.id = Category._next_id
        Category._next_id += 1
        if cat is not None:
            name = cat.name
            description = cat.desc
            hierarchy = cat.hierarchy
            parent = cat.parent
        self.name = name
        self.description = description
        self.type = None
        self.hierarchy = hierarchy
        self.children = []
        self.parent = parent.hierarchy or parent.name if parent else None
        self.amount = decimal.Decimal(0)
        self.source_file = source_file

    def __repr__(self):
        return f"Category(id={self.id}, name={self.name}, description={self.description}, type={self.type if self.type else None}, hierarchy={self.hierarchy}, children={self.children}, parent={self.parent})"

    def as_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "type": self.type if self.type else None,
            "hierarchy": self.hierarchy,
            "children": self.children,
            "parent": self.parent,
            "source_file": self.source_file,
        }

    def add_child(self, child: "Category"):
        key = child.hierarchy or child.name
        if key not in self.children:
            self.children.append(key)


class Split:
    def __init__(
        self,
        amount,
        memo,
        category,
        contra_account,
        percent,
        source_file: str | None = None,
    ):
        self.amount = amount
        self.memo = memo
        self.category = category
        self.contra_account = contra_account
        self.percent = percent
        self.source_file = source_file

    def __repr__(self):
        return f"Split(amount={self.amount}, memo={self.memo}, category={self.category}, contra_account={self.contra_account}, percent={self.percent})"

    def as_dict(self):
        return {
            "amount": self.amount,
            "memo": self.memo,
            "category": (
                (self.category.hierarchy or self.category.name)
                if self.category
                else None
            ),
            "contra_account": self.contra_account,
            "percent": self.percent,
            "source_file": self.source_file,
        }


class Transaction:
    def __init__(
        self,
        date,
        amount,
        payee,
        category,
        contra_acc,
        memo,
        chk_num,
        splits: list[Split] | None = None,
        source_file: str | None = None,
    ):
        self.date = date
        self.amount = amount
        self.payee = payee
        self.category = category
        self.contra_acc = contra_acc
        self.memo = memo
        self.chk_num = chk_num
        self.splits: list[Split] = splits or []
        self.source_file = source_file

    def __repr__(self):
        return f"Transaction(date={self.date}, amount={self.amount}, payee={self.payee}, category={self.category})"

    def as_dict(self):
        return {
            "date": str(self.date) if self.date else None,
            "amount": self.amount,
            "payee": self.payee,
            "category": (
                (self.category.hierarchy or self.category.name)
                if self.category
                else None
            ),
            "contra_account": self.contra_acc,
            "memo": self.memo,
            "chk_num": self.chk_num,
            "splits": [s.as_dict() for s in self.splits],
            "source_file": self.source_file,
        }


class Investment:
    def __init__(
        self,
        date,
        action,
        security,
        quantity,
        price,
        amount,
        memo,
        cleared=None,
        contra_account=None,
        commission=None,
        source_file: str | None = None,
    ):
        self.date = date
        self.action = "Buy" if action == "Kauf" else action
        self.security = security
        self.quantity = quantity
        self.price = price
        self.amount = amount
        self.memo = memo
        self.cleared = cleared
        self.contra_account = contra_account
        self.commission = commission
        self.source_file = source_file

    def __repr__(self):
        return (
            f"Investment(date={self.date}, action={self.action}, security={self.security}, "
            f"quantity={self.quantity}, price={self.price}, amount={self.amount}, memo={self.memo}, "
            f"cleared={self.cleared}, contra_account={self.contra_account}, commission={self.commission})"
        )

    def as_dict(self):
        return {
            "date": self.date if self.date else None,
            "action": self.action,
            "security": self.security,
            "quantity": self.quantity,
            "price": self.price,
            "amount": self.amount,
            "memo": self.memo,
            "cleared": self.cleared,
            "contra_account": self.contra_account,
            "commission": self.commission,
        }


class Balance:
    def __init__(self, date, amount, source_file: str | None = None):
        self.date = date
        self.amount = amount
        self.source_file = source_file

    def __repr__(self):
        return f"Balance(date={self.date}, amount={str(self.amount)})"

    def as_dict(self):
        return {
            "date": self.date if self.date else None,
            "amount": self.amount,
            "source_file": self.source_file,
        }


class Account:
    _next_id = 1

    def __init__(self, name: str, acc_type, id=None, source_file: str | None = None):
        if id is not None:
            self.id = id
            if id >= Account._next_id:
                Account._next_id = id + 1
        else:
            self.id = Account._next_id
            Account._next_id += 1
        self.name = name
        self.acc_type = acc_type
        self.balances: list[Balance] = []
        self.transactions: list[Transaction] = []
        self.investments: list[Investment] = []
        self.source_file = source_file

    def __repr__(self):
        return f"Account(id={self.id}, name={self.name}, acc_type={str(self.acc_type.name)})"

    def as_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "acc_type": str(self.acc_type.name),
            "balances": [b.as_dict() for b in self.balances],
            "transactions": [t.as_dict() for t in self.transactions],
            "investments": [i.as_dict() for i in self.investments],
            "source_file": self.source_file,
        }


class MoneyData:
    def __init__(self):
        self.accounts: dict[str, Account] = {}
        self.categories: dict[str, Category] = {}

    def __repr__(self):
        return f"MoneyData(accounts={self.accounts}, categories={self.categories})"

    def as_dict(self):
        return {
            "accounts": {n: a.as_dict() for n, a in self.accounts.items()},
            "categories": {n: c.as_dict() for n, c in self.categories.items()},
        }

    def from_dict(self, data: dict):
        self.categories = {
            n: Category(
                None,
                name=c["name"],
                description=c.get("description"),
                hierarchy=c.get("hierarchy"),
                parent=None,
                source_file=c.get("source_file"),
            )
            for n, c in data.get("categories", {}).items()
        }
        self.accounts = {
            n: Account(
                name=a["name"],
                acc_type=quiffen.AccountType[a["acc_type"]],
                id=a["id"],
                source_file=a.get("source_file"),
            )
            for n, a in data.get("accounts", {}).items()
        }
        for n, a in data.get("accounts", {}).items():
            self.accounts[n].balances = [
                Balance(
                    date=b["date"], amount=b["amount"], source_file=b.get("source_file")
                )
                for b in a.get("balances", [])
            ]
            self.accounts[n].transactions = [
                Transaction(
                    date=t["date"],
                    amount=t["amount"],
                    payee=t["payee"],
                    category=self.categories.get(t["category"]),
                    contra_acc=t["contra_account"],
                    memo=t["memo"],
                    chk_num=t["chk_num"],
                    splits=[
                        Split(
                            amount=s["amount"],
                            memo=s.get("memo"),
                            category=self.categories.get(s["category"]),
                            contra_account=s.get("contra_account"),
                            percent=s.get("percent"),
                            source_file=s.get("source_file"),
                        )
                        for s in t.get("splits", [])
                    ],
                    source_file=t.get("source_file"),
                )
                for t in a.get("transactions", [])
            ]
            self.accounts[n].investments = [
                Investment(
                    date=i["date"],
                    action=i["action"],
                    security=i["security"],
                    quantity=i["quantity"],
                    price=i["price"],
                    amount=i["amount"],
                    memo=i["memo"],
                    cleared=i.get("cleared"),
                    contra_account=i.get("contra_account"),
                    commission=i.get("commission"),
                    source_file=i.get("source_file"),
                )
                for i in a.get("investments", [])
            ]
        for n, c in data.get("categories", {}).items():
            self.categories[n].type = c.get("type")
            self.categories[n].children = c.get("children", [])
            self.categories[n].parent = c.get("parent")

    def _add_category(
        self,
        cat: quiffen.Category,
        source_file: str | None = None,
    ) -> Category:
        key = cat.hierarchy or cat.name
        if not key:
            return
        if key not in self.categories:
            self.categories[key] = Category(cat, source_file=source_file)
            if cat.parent:
                parent_key = cat.parent.hierarchy or cat.parent.name
                if parent_key not in self.categories:
                    self.categories[parent_key] = Category(cat.parent)
                self.categories[parent_key].add_child(self.categories[key])
        for child in cat.children:
            c = self._add_category(child, source_file=source_file)
            self.categories[key].add_child(c)
        return self.categories[key]

    def _set_category_type_recursive(self, cat: str, category_type: str):
        if cat in self.categories:
            self.categories[cat].type = category_type
            for child in self.categories[cat].children:
                self._set_category_type_recursive(child, category_type)

    def _set_category_type(self, key: str | None, category_type: quiffen.CategoryType):
        if key is None:
            return
        if key in self.categories and self.categories[key].parent:
            key = self.categories[key].parent
        self._set_category_type_recursive(key, category_type.name)

    def _get_transactions(
        self,
        account_name: str,
        account: quiffen.Account,
        source_file: str | None = None,
    ):
        # for t in [t for t in list(account.transactions.values())[0]]:
        #     print(t)
        #     if getattr(t, "splits", []):
        #         for s in t.splits:
        #             print(
        #                 f"{s.amount=} "
        #                 f"{s.memo=} "
        #                 f"{s.category.hierarchy=} "
        #                 f"{s.to_account=} "
        #                 f"{s.percent=} "
        #             )

        for date, amount, payee, cat, contra_acc, memo, chk_num, splits in [
            (
                t.date,
                t.amount,
                t.payee,
                t.category,
                t.to_account,
                t.memo,
                t.check_number,
                t.splits,
            )
            for t in list(account.transactions.values())[0]
            if isinstance(t, quiffen.Transaction)
        ]:

            transaction = Transaction(
                date=date,
                amount=amount,
                payee=payee,
                category=cat if len(splits) == 0 else None,
                contra_acc=contra_acc if len(splits) == 0 else None,
                memo=memo,
                chk_num=chk_num,
                source_file=source_file,
            )
            self.accounts[account_name].transactions.append(transaction)

            if splits:
                for s_amount, s_memo, s_cat, s_contra, s_pct in [
                    (s.amount, s.memo, s.category, s.to_account, s.percent)
                    for s in splits
                ]:
                    split = Split(
                        amount=s_amount,
                        memo=s_memo,
                        category=s_cat,
                        contra_account=s_contra,
                        percent=s_pct,
                        source_file=source_file,
                    )
                    transaction.splits.append(split)
                    if s_cat:
                        self._add_category(s_cat, source_file=source_file)
                    s_cat_key = s_cat.hierarchy or s_cat.name if s_cat else None
                    if s_cat_key and s_cat_key in self.categories and s_amount:
                        self.categories[s_cat_key].amount += s_amount
            else:
                cat_key = cat.hierarchy or cat.name if cat else None
                if cat_key and cat_key in self.categories:
                    self.categories[cat_key].amount += amount

    def _get_investments(
        self,
        account_name: str,
        account: quiffen.Account,
        source_file: str | None = None,
    ):
        # print(f"{account_name=} {account}")
        for t in [
            t
            for t in list(account.transactions.values())[0]
            if isinstance(t, quiffen.Investment)
        ]:
            investment = Investment(
                date=t.date,
                action=t.action,
                security=t.security,
                quantity=t.quantity,
                price=t.price,
                amount=t.amount,
                memo=t.memo,
                cleared=t.cleared,
                contra_account=t.to_account,
                commission=t.commission,
                source_file=source_file,
            )
            self.accounts[account_name].investments.append(investment)

    def append_qif(self, qif: quiffen.Qif, source_file: str | None = None):
        for cat in qif.categories.values():
            self._add_category(cat)

        for account_name, account in qif.accounts.items():
            # print(f"Appending account {account_name}: {account}")
            if len(account.transactions) > 1:
                raise ValueError(
                    f"Account {account_name} has more than one transaction type, which is not supported."
                )
            # print(account)
            if account_name not in self.accounts:
                self.accounts[account_name] = Account(
                    name=account_name,
                    acc_type=account.account_type,
                    source_file=source_file,
                )
            self.accounts[account_name].balances.append(
                Balance(
                    date=account.date_at_balance,
                    amount=account.balance,
                    source_file=source_file,
                )
            )
            if account.account_type == quiffen.AccountType.INVST:
                self._get_investments(account_name, account, source_file=source_file)
            else:
                self._get_transactions(account_name, account, source_file=source_file)

    def apply_category_type_heuristics(self):
        def total_amount(category) -> decimal.Decimal:
            total = category.amount
            for child in category.children:
                total += total_amount(self.categories[child])
            return total

        for cat_key, category in (
            (k, v) for k, v in self.categories.items() if v.parent is None
        ):
            amount = total_amount(category)
            self._set_category_type(
                cat_key,
                (
                    quiffen.CategoryType.INCOME
                    if amount > 0
                    else quiffen.CategoryType.EXPENSE
                ),
            )
            # print(
            #     f"Category {cat_key} has total amount {amount}, set type to {self.categories[cat_key].type}"
            # )
        return self
