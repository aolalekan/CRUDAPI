from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient
load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://aolalekan:{password}@cluster0.ludvtsm.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(connection_string)

dbs = client.list_database_names()
test_db = client.test
collections = test_db.list_collection_names()
print(collections)

def insert_test_doc():
    collection = test_db.test
    test_document = {
        "name": "Ade",
        "type": "Test"
    }
    inserted_id = collection.insert_one(test_document).inserted_id
    print(inserted_id)

production = client.production
person_collection = production.person_collection

def create_documents():
    first_names = ["Ade", "Sarah", "Jennifer", "Jose", "Brad", "Allen"]
    last_names = ["Knight","Smith", "Bart", "Cater", "Pit", "Geral"]
    ages = [21, 40, 23, 19, 34, 67]

    docs = []

    for first_name, last_name, age in zip(first_names, last_names, ages):
        doc = {"first_name": first_name, "last_name": last_name, "age": age}
        docs.append(doc)
        #person_collection.insert_one(doc)

    person_collection.insert_many(docs)

create_documents()

printer = pprint.PrettyPrinter()

def find_all_people():
    people = person_collection.find()

    for person in people:
        printer.pprint(person)

find_all_people()


def find_Ade():
    Ade = person_collection.find_one({"first_name": "Ade"})
    printer.pprint(Ade)

find_Ade()

def count_all_people():
    count = person_collection.count_documents(filter={})
    print("Number of people", count)

count_all_people()


def get_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})
    printer.pprint(person)

get_person_by_id("idn")

def update_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    all_updates = {
        "$set": {"new_field": True},
        "$inc": {"age": 1},
        "$rename": {"first_name": "first", "last_name": "last"}
    }
    person_collection.update_one({"_id: _id"}, all_updates)

update_person_by_id{"idn"}

def replace_one(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    new_doc = {
        "first_name": "new first name",
        "last_name": "new last name",
        "age": 100
    }

    person_collection.replace_one({"_id": _id}, new_doc)

replace_one{"idn"}

def delete_doc_by_id(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    person_collection.delete_one({"_id": _id})

delete_doc_by_id("idn")

