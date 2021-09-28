from pathlib import Path


files = [_ for _ in Path(".").iterdir()]

for f in files:
    print(f"Process {f}? ")
    answer = input()
    if answer == "y":
        with open(f, encoding="utf-8") as inf:
            lines = inf.readlines()
            items = lines[0].strip().split("\t")
            print(f"{[(i, item) for i, item in enumerate(items)]}")
