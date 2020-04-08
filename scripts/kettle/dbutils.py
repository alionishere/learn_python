# -*- coding: utf-8 -*-
import cx_Oracle
import pymysql
import sys
import os


def get_conn(data_src):
    if data_src == 'crm' or data_src == 'mot':
        return cx_Oracle.connect('mot', 'mot', '192.250.107.198:49162/xe')
    elif data_src == 'tra5':
        return pymysql.connect("192.250.107.198", 'root', 'Admin@123', 'a5_datacenter')
    else:
        print('Get database connection error!')
