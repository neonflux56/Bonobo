
import bonobo
import pandas as pd
from bonobo.config import use_context_processor
import csv


def colnames1():
    l = "Abbreviation\tDescription"
    yield l

def extract1():
    f = open("data_description.txt","r")
    yield from f.readlines()

def transform1(l : str):
    l = l.strip()
    if ":" not in l and l != "":
        yield l

def transform2(l: str):
    l = l.split('\t')
    yield l


def with_opened_file(self, context):
    with open('output.csv', 'a') as f:
        yield f

@use_context_processor(with_opened_file)
def write_to_file(f, *row):
    with open('output.csv', 'a') as f:
        writer = csv.writer(f, lineterminator='\n')
        for tup in row:
            writer.writerow(tup)

if __name__ == '__main__':
    graph = bonobo.Graph()
    graph.add_chain(
        colnames1, transform2,write_to_file
    )
    graph.add_chain(
        extract1,transform1,transform2,write_to_file
    )
    bonobo.run(graph)


#########################################

#Task 2
data = pd.read_csv("train.csv")

def extract():
    yield from data.itertuples()

num = list(data.keys()).index("YearBuilt")

def discardold(*args):
    if int(args[num+1]) < 1980:
        return None

def load(*args):
    print(args)

def with_opened_file(self, context):
    with open('2output1.csv', 'a') as f:
        yield f

@use_context_processor(with_opened_file)
def write_to_file(f, *row):
    with open('2output1.csv', 'a') as f:
        writer = csv.writer(f, lineterminator='\n')
        for tup in row:
            writer.writerow(tup)

if __name__ == '__main__':
    graph = bonobo.Graph()
    graph.add_chain(
        extract, discardold, load
    )
    bonobo.run(graph)






#########################################

