import re
import argparse
import subprocess
import json

REQUESTS = [
    "GET",
    "HEAD",
    "POST",
    "PUT",
    "DELETE",
    "CONNECT",
    "OPTIONS",
    "TRACE",
    "PATCH",
]

with open("result.json", "w") as writer:
    writer.write(json.dumps([]))


def parse_file(file):
    # кол-во строк в файле
    rows: str = subprocess.check_output(["wc", "-l", file], universal_newlines=True)

    # подсчет запросов каждого типа
    all_requests = {}
    for request_type in REQUESTS:
        request = subprocess.Popen(
            ("grep", f'"{request_type}', file), stdout=subprocess.PIPE
        )
        output = subprocess.check_output(
            ("wc", "-l"), stdin=request.stdout, universal_newlines=True
        )

        all_requests.update({request_type: output.split()[0]})

    # топ-3 IP по запросам
    with open(file, "r") as reader:
        all_rows = reader.readlines()
    ip_list = []
    for row in all_rows:
        ip_list.append(row.split()[0])
    unique_ip_list = set(ip_list)
    ip_count_list = {}
    for ip in unique_ip_list:
        ip_count_list.update({ip: 0})
    for row in all_rows:
        if row.split()[0] in ip_count_list:
            ip_count_list.update(
                {row.split()[0]: ip_count_list.get(row.split()[0]) + 1}
            )
    with open("unique.txt", "w") as wr:
        wr.write(str(ip_count_list))

    sorted_dict = {}
    sorted_keys = sorted(ip_count_list, key=ip_count_list.get)

    for w in sorted_keys:
        sorted_dict[w] = ip_count_list[w]

    top_3_ip = dict(list(sorted_dict.items())[-3:])

    # топ-3 по длине
    time_list = []
    for row in all_rows:
        if row.split()[9] != "-":
            time_list.append(int(row.split()[9]))
    unique_time_list = set(time_list)
    top_three_times = sorted(unique_time_list)[-3:]

    top_3_time = {}
    counter = 1
    for time in top_three_times:
        output1 = subprocess.check_output(
            ("grep", str(time), file), universal_newlines=True
        )
        top_3_time.update(
            {
                f"request_{counter}": {
                    "type": output1.split()[5][1:],
                    "url": output1.split()[6],
                    "ip": output1.split()[0],
                    "request_time": output1.split()[9],
                    "datetime": output1.split()[3] + output1.split()[4],
                }
            }
        )
        counter += 1

    with open("result.json", "r") as reader:
        data = json.load(reader)

    data.append(
        {
            "file": file,
            "count of rows": rows.split()[0],
            "requests": all_requests,
            "top-3 requests from ip": top_3_ip,
            "top-3 requests by time": top_3_time,
        }
    )

    with open("result.json", "w") as writer:
        writer.write(json.dumps(data, indent=4))


parser = argparse.ArgumentParser(description="Parsing log files")
parser.add_argument("--src", action="store")

args = parser.parse_args()

try:
    if "directory" in subprocess.check_output(
        ["file", args.src], universal_newlines=True
    ):
        output: str = subprocess.check_output(
            ["ls", "-al", args.src], universal_newlines=True
        )
        files_list = [
            re.split(":[0-9][0-9] ", i)[1]
            for i in output.splitlines()[3:]
            if i.split()[0][0] == "-"
        ]
        for file in files_list:
            if args.src[-1:] != "/":
                args.src += "/"
            parse_file(args.src + file)
    else:
        parse_file(args.src)
except TypeError:
    print("Необходимо указать --src")
