#!/usr/bin/python
# -*- coding: utf-8 -*-

from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base


# 创建对象的基类
Base = declarative_base()


# 定义表的对象
class Choice(Base):
    # table name
    __tablename__ = 'polls_choice'

    # fields
    id = Column(String(10), primary_key=True)
    choice_text = Column(String(100))
    votes = Column(String(5))
    question_id = Column(String(5))


# initialize the db connection
# from sqlalchemy import create_engine
# engine = create_engine('postgresql+psycopg2://user:password@hostname/database_name')
engine = create_engine("postgresql+psycopg2://postgres:alion@localhost/postgres")
DBSession = sessionmaker(bind=engine)
db = scoped_session(DBSession)


# execute
def exe1():
    session = DBSession()
    choice = session.query(Choice).filter(Choice.id=='2').one()
    print("choice.id: %s: %s" % (choice.id, choice.choice_text))
    print('-' * 15)
    choices = session.query(Choice)
    print(choices.all())
    for c in choices.all():
        print('%s: %s: %s: %s' %(c.id, c.choice_text, c.votes, c.question_id))


def exe2():
    choice = db.execute('select * from public.polls_choice').fetchone()
    print(choice)
    print("choice.id: %s: %s" % (choice.id, choice.choice_text))


if __name__ == '__main__':
    exe2()
    exe1()
