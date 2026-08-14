import re
from quiffen import Qif
from pathlib import Path

from money_data import MoneyData, Account


class QifReader:
    "Read qif data from a file"

    def __init__(self, path: Path):
        self.path = path

    def read_file(self, file_path: Path) -> Qif:
        """Read qif data from the file and return a Qif object"""
        with open(file_path, "r", encoding="cp1252") as file:
            qif_data = file.read()
        acc_name = None
        acc_type = None
        acc_balance = None
        acc_date = None
        splitlines = qif_data.splitlines()
        if len(splitlines) <= 7:
            print(f"Skipping empty file {file_path}")
            return
        print(f"Reading {file_path} with {len(splitlines)} lines")

        # print("\n".join(splitlines[:20]) + "\n...\n")
        line_no = 0
        for l in splitlines:
            if l.startswith("!Account"):
                break
            if l.startswith("!Type:"):
                acc_type = l[6:]
                if acc_type == "Invst":
                    acc_name = "Depot"
                    break
            if l[0] == "L":
                acc_name = l[2:-1]
            if l[0] == "T":
                acc_balance = l[1:]
            if l[0] == "D":
                m = re.fullmatch(r"D(\d+)/(\d+)([^\d].*)", l)
                if not m:
                    raise Exception(f"Invalid balance date line: {l}")
                acc_date = f"{m[2]}/{m[1]}{m[3]}"
                # acc_date = l[1:]
            if l[0] == "^":
                break
            line_no += 1

        if acc_name and acc_type:
            qif_data = "\n".join(
                ["!Account", f"N{acc_name}", f"T{acc_type}"]
                + (
                    [f"${acc_balance}", f"/{acc_date}"]
                    if acc_balance and acc_date
                    else []
                )
                + ["^"]
                + [splitlines[0]]
                + splitlines[line_no + 1 :]
            )

        # for l in qif_data.splitlines()[1470:1500]:
        #     print(l)
        # raise Exception("Stop")

        lines = []

        for line in qif_data.splitlines():
            if line in ("I", "Q"):
                continue
            lines.append(line)
        qif_data = "\n".join(lines)

        qif = Qif.parse_string(qif_data, day_first=True)

        return qif

    def read(self):
        "parse all files in the given path"
        data = MoneyData()
        Account._next_id = 1
        file_list = list(self.path.glob("[0-9]?/*.qif"))
        print(f"Found {len(file_list)} qif files in {self.path}")
        l = [
            ((f"{e[:5]}-{e[6:]}" if e[5] == "_" else e, e))
            for e in [str(e) for e in file_list]
        ]
        for file_path in [Path(f[1]) for f in sorted(l)]:
            source_file = file_path.parent.name + "/" + file_path.stem
            qif = self.read_file(file_path)
            if qif:
                data.append_qif(qif, source_file=source_file)
        return data
