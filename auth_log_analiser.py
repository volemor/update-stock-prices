import os
import pandas as pd
'''
программа проверки лог файлов на наличие попытки подключения по ssh:
'''

def main():
    count_fail = 0
    fail_list = []
    with open('/var/log/auth.log', mode='r') as file:
        for line in file:
            if 'Failed password' in line:
                fail_list.append([*line.split()[:3], line.split()[-4]])
                # print(line[:-1])
                count_fail += 1
    df = pd.DataFrame(fail_list)
    hack_count = {}
    hack_count = {ip_hack: df[df[3] == ip_hack][3].count() for ip_hack in df[3].unique()}
    for my_item, my_values in hack_count.items():
        print(f'hackers ip [{my_item}] try [{my_values}]')
    print(f'количество попыток [{count_fail}] с уникальных адресов [{len(hack_count)}]')


if __name__ == '__main__':
    main()

