import csv
import random
import uuid
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import json

res_path = 'E:\\bigdata\\school\\csvfiles\\'
orig_path = 'E:\\bigdata\\school\\csvfiles_orig\\'
engine = create_engine(url=f"mysql+pymysql://alex3@192.168.1.75/school", echo=False)


def generate_students():
    mname_list = []
    fname_list = []
    with open(orig_path + 'russian_names.csv', 'r', encoding='utf-8') as names:
        for row in names.read().split('.biz'):
            if len(row.split(';')) > 1:
                if row.split(';')[2] == 'М':
                    mname_list.append(row.split(';')[1])
                if row.split(';')[2] == 'Ж':
                    fname_list.append(row.split(';')[1])

    msurname_list = []
    fsurname_list = []
    with open('fsurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            fsurname_list.append(row.split(' ')[1])
    with open('msurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            msurname_list.append(row.split(' ')[1])
    class_letter_list = ['A', 'Б', 'В', 'Г', 'Д']
    class_num_list = range(6, 9)
    class_list = []
    for _ in range(5):
        class_list.append(str(random.choice(class_num_list)) + random.choice(class_letter_list))

    age_list = range(2006, 2009)
    sex_list = ['М', 'Ж']
    result_list = []
    for _ in range(150):
        is_male = random.choice([True, False])
        result_list.append({'GUID': str(uuid.uuid4()),
                            'name': random.choice(mname_list) if is_male == True else random.choice(fname_list),
                            'surname': random.choice(msurname_list) if is_male == True else random.choice(fsurname_list),
                            'sex': random.choice(sex_list),
                            'age': random.choice(age_list),
                            'clss': random.choice(class_list)})

    keys = result_list[0].keys()
    with open(res_path + 'students.csv', 'w', newline='', encoding='utf-8') as output_csv:
        dict_writer = csv.DictWriter(output_csv, keys)
        dict_writer.writeheader()
        dict_writer.writerows(result_list)


def generate_teachers():
    name_list = []
    with open(orig_path + 'russian_names.csv', 'r', encoding='utf-8') as names:
        for row in names.read().split('.biz'):
            if len(row.split(';')) > 1:
                name_list.append(row.split(';')[1])

    surname_list = []
    with open(orig_path + 'russian_surnames.csv', 'r', encoding='utf-8') as surnames:
        for row in surnames.read().split('.biz'):
            if len(row.split(';')) > 1:
                surname_list.append(row.split(';')[1])

    result_list = []
    subj_list = ['русский язык', 'литература', 'математика', 'иностранный язык', 'история']
    for sub in subj_list:
        result_list.append({'GUID': str(uuid.uuid4()),
                            'name': random.choice(name_list),
                            'surname': random.choice(surname_list),
                            'subj': sub})

    keys = result_list[0].keys()
    with open(res_path + 'teachers.csv', 'w', newline='', encoding='utf-8') as output_csv:
        dict_writer = csv.DictWriter(output_csv, keys)
        dict_writer.writeheader()
        dict_writer.writerows(result_list)


def generate_subj():
    subj_list = ['русский язык', 'литература', 'математика', 'иностранный язык', 'история']
    with open(res_path + 'subjects.csv', 'w', newline='', encoding='utf-8') as output_csv:
        writer = csv.writer(output_csv, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(subj_list)


generate_students()
generate_teachers()
generate_subj()

csv_file = pd.read_csv(res_path + 'students.csv', delimiter=',', on_bad_lines='skip')
csv_file.to_sql("students", engine, if_exists='replace', index=False)
csv_file = pd.read_csv(res_path + 'teachers.csv', delimiter=',', on_bad_lines='skip')
csv_file.to_sql("teachers", engine, if_exists='replace', index=False)
csv_file = pd.read_csv(res_path + 'subjects.csv', delimiter=',', on_bad_lines='skip')
csv_file.to_sql("subjects", engine, if_exists='replace', index=False)
