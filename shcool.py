import csv
import random
import datetime
import uuid
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import json


res_path = 'E:\\bigdata\\school\\csvfiles\\'
orig_path = 'E:\\bigdata\\school\\csvfiles_orig\\'
engine = create_engine(url=f"mysql+pymysql://alex3@192.168.1.75/school", echo=False)
connection = engine.raw_connection()


def generate_students():
    mname_list = []
    fname_list = []
    with open('fsurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            fname_list.append(row.split(' ')[0])
    with open('msurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            mname_list.append(row.split(' ')[0])

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
        is_male = random.randint(0, 1)
        result_list.append({'GUID': str(uuid.uuid4()),
                            'name': random.choice(mname_list) if is_male == 1 else random.choice(fname_list),
                            'surname': random.choice(msurname_list) if is_male == 1 else random.choice(fsurname_list),
                            'sex': 'М' if is_male == 1 else 'Ж',
                            'age': random.choice(age_list),
                            'clss': random.choice(class_list)})

    keys = result_list[0].keys()
    with open(res_path + 'students.csv', 'w', newline='', encoding='utf-8') as output_csv:
        dict_writer = csv.DictWriter(output_csv, keys)
        dict_writer.writeheader()
        dict_writer.writerows(result_list)
    return result_list

def generate_teachers():
    mname_list = []
    fname_list = []
    with open('fsurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            fname_list.append(row.split(' ')[0])
    with open('msurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            mname_list.append(row.split(' ')[0])

    msurname_list = []
    fsurname_list = []
    with open('fsurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            fsurname_list.append(row.split(' ')[1])
    with open('msurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            msurname_list.append(row.split(' ')[1])
    result_list = []
    subj_list = ['русский язык', 'литература', 'математика', 'иностранный язык', 'история']
    for sub in subj_list:
        is_male = random.randint(0, 1)
        result_list.append({'GUID': str(uuid.uuid4()),
                            'name': random.choice(mname_list) if is_male == 1 else random.choice(fname_list),
                            'surname': random.choice(msurname_list) if is_male == 1 else random.choice(fsurname_list),
                            'sex': 'М' if is_male == 1 else 'Ж',
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
    return subj_list

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def generate_shedule(students, subj):
    i = 0
    result_list = []
    for date in daterange(start_date= datetime.date(2022, 9, 1), end_date=datetime.date(2023, 5, 31)):
        for student in students:
            for _ in range(random.randint(2, 3)):
                result_list.append({'ID': i,
                                    'student_id': student["GUID"],
                                    'date': date,
                                    'subj': random.choice(subj),
                                    'rate': random.randint(2, 5)
                                    })
                i += 1
    keys = result_list[0].keys()
    with open(res_path + 'shedule.csv', 'w', newline='', encoding='utf-8') as output_csv:
        dict_writer = csv.DictWriter(output_csv, keys)
        dict_writer.writeheader()
        dict_writer.writerows(result_list)

def generate_calendar():
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS calendar")
    cursor.execute("CREATE TABLE calendar"
                   "(date date, weekday smallint)")
    for single_date in daterange(start_date= datetime.date(2022, 9, 1), end_date=datetime.date(2023, 5, 31)):
        cursor.execute("INSERT INTO calendar VALUES (%s, %s)", (single_date, single_date.weekday()))
        connection.commit()
    print(cursor.fetchall())


generate_teachers()

generate_calendar()
generate_shedule(generate_students(), generate_subj())


csv_file = pd.read_csv(res_path + 'students.csv', delimiter=',', on_bad_lines='skip')
csv_file.to_sql("students", engine, if_exists='replace', index=False)
csv_file = pd.read_csv(res_path + 'teachers.csv', delimiter=',', on_bad_lines='skip')
csv_file.to_sql("teachers", engine, if_exists='replace', index=False)
csv_file = pd.read_csv(res_path + 'subjects.csv', delimiter=',', on_bad_lines='skip')
csv_file.to_sql("subjects", engine, if_exists='replace', index=False)
csv_file = pd.read_csv(res_path + 'shedule.csv', delimiter=',', on_bad_lines='skip')
csv_file.to_sql("shedule", engine, if_ex
