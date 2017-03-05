#python 2.7
import pyodbc
import csv
import sys
from os import listdir
from os.path import isfile, join, splitext
import codecs

avoid_endless_loop = 1

def get_csv_filednames(input_dir):
    data = {}
    file_list = [f for f in listdir(input_dir) if isfile(join(input_dir, f)) and f!="gitkeep"]
    for i in file_list:
        file_name_no_ext = splitext(i)[0]
        with open(input_dir+i, "rb") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter='\t')
            data[file_name_no_ext] = next(csv_reader)
    return data

def get_access_filenames(ms_acc_dsn):
    global avoid_endless_loop
    avoid_endless_loop +=1
    if avoid_endless_loop>20:
        print("please run it again")
        sys.exit(0)
    data = {}
    table_name_list = []
    dsn_name = 'DSN='+ms_acc_dsn+';PWD=""'
    cnxn = pyodbc.connect(dsn_name)
    cursor = cnxn.cursor()
    for row in cursor.tables():
        table_name_list.append(row.table_name)
    for i in table_name_list:
        data[i] = []
        try:
            for j in cursor.columns(table=i):
                column_str = j.column_name
                #print("-begin-")
                #print(type(column_str))
                #print(column_str.encode('unicode-escape').decode('utf8'))
                #print(column_str.encode('unicode-escape'))
                column_str = column_str.encode('unicode-escape').decode('utf8')
                data[i].append(column_str)
                #print("-end-")
        except:
            data = get_access_filenames(ms_acc_dsn)
    return data

def compare_with_order(list1, list2):
    #print(list2)
    for index, item in list1.items():
        print("\n---"+index+"---")
        index = index+"_txt"
        if (index in list2):
            print(index+" exist\n")
            i = 0
            for i in range(len(item)-1):
                i+=1
                try:
                    if item[i].decode('utf8').encode('unicode-escape')!=list2[index][i]:
                        print(item[i].decode('utf8')+" can not be found")
                except:
                    print(item[i].decode('utf8')+" can not be found")
                    continue
        else:
            print(index+" not exist!")
    print("---finish---")

def compare_without_order(list1, list2):
    #print(list2)
    for index, item in list1.items():
        print("\n---"+index+"---")
        index = index+"_txt"
        if (index in list2):
            print(index+" exist\n")
            i = 0
            for i in range(len(item)-1):
                i+=1
                try:
                    if item[i].decode('utf8').encode('unicode-escape') not in list2[index]:
                        print(item[i].decode('utf8')+" can not be found")
                except:
                    print(item[i].decode('utf8')+" can not be found")
                    continue
        else:
            print(index+" not exist!")
    print("---finish---")
    
csv_fieldname_list = get_csv_filednames("text/")
access_fieldname_list = get_access_filenames("new_lease")
#compare_with_order(csv_fieldname_list, access_fieldname_list)
compare_without_order(csv_fieldname_list, access_fieldname_list)
