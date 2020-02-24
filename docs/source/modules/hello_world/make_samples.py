import names

result = ""
names = []
for i in range(x):
    names.append(names.get_full_name())

for name in names:
    result += name + "\n"

with open("samples.csv", "w") as f:
    f.write(result)
