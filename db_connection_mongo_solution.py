#-------------------------------------------------------------------------
# AUTHOR: Lauren Contreras
# FILENAME: db_connection_mongo_solution.py
# SPECIFICATION: This file creates a connection to the database, creates the collections, and inlcudes functions like createDocument, deleteDocument, and updateDocument.
#                It also generates an inverted index.           
# FOR: CS 4250- Assignment #3
# TIME SPENT: total of 4 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
import pymongo
from pymongo import MongoClient
from bson import ObjectId
import datetime
def connectDataBase():

    # Create a database connection object using pymongo
    # --> add your Python code here
    DB_NAME = "CPP"
    DB_HOST = "localhost"
    DB_PORT = 27017

    try:
        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]
        createCollections(db)
        return db
    except:
        print(f"Database not connected successfully: {str(e)}")
        return None
def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    docText = convertText(docText)
    terms = docText.split(" ")
    term_count = {}
    for term in terms:
        if term in term_count:
            term_count[term] += 1
        else:
            term_count[term] = 1
    # create a list of objects to include full term objects. [{"term", count, num_char}]
    # --> add your Python code here
    term_list = []
    for term, count in term_count.items():
        term_obj = {
            "term": term,
            "count": count,
            "num_chars": len(term)

        }
        term_list.append(term_obj)
    # produce a final document as a dictionary including all the required document fields
    # --> add your Python code here
    document = {
        "id": docId,
        "text": docText,
        "title": docTitle,
        "date": docDate,
        "category": docCat,
        "terms": term_list
    }
    # insert the document
    # --> add your Python code here
    col.insert_one(document)

def deleteDocument(col, docId):

    # Delete the document from the database
    # --> add your Python code here
    col.delete_one({"id": docId})
    

def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    # Delete the document
    # --> add your Python code here
    try:
        
        col.delete_one({"id": docId})
        # Create the document with the same id
        # --> add your Python code here
        docText = convertText(docText)
        terms = docText.split()
        term_count = {}
        for term in terms:
            if term in term_count:
                term_count[term] += 1
            else:
                term_count[term] = 1
        term_list = []
        for term, count in term_count.items():
            term_obj = {
                "term": term,
                "count": count,
                "num_chars": len(term)

            }
            term_list.append(term_obj)
        new_document = { 
       
            "id": docId,
            "text": docText,
            "title": docTitle,
            "date": docDate,
            "category": docCat,
            "terms":term_list}
        col.insert_one(new_document)
        
    except Exception as e:
        print(f"Error: {str(e)}")


def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    try:

        pipeline = [
            {"$unwind": "$terms"},
            {"$group": {"_id": "$terms.term", 
                        "documents": {"$push": {"document": "$title", "count": "$terms.count"}}}},
            {"$sort": {"_id": 1}},
            {"$project": {
                "_id": 0, "term": "$_id", "documents": "$documents"}}]

        result = list(col.aggregate(pipeline))
        inverted_index = {}
        for item in result:
            term = item["term"]
            documents = item["documents"]

            docu = ", ".join(f"{doc['document']}: {doc['count']}"
                            for doc in documents)
            inverted_index[term] = docu
        
        return inverted_index
    except Exception as e:
        print(f"Error generating inverted index: {str(e)}")
        return {}

def convertText(text):
# converting to lower case and removing punctuation marks.
    text = text.lower()
    text = text.replace(",","")
    text = text.replace(".", "")
    text = text.replace("!", "")
    text = text.replace("?", "")
    return text

def createCollections(db):
    try:
        db.create_collection('documents')
        db.create_collection('terms')
    except pymongo.errors.CollectionInvalid as e:
        print(f"Collection creation failed: {str(e)}")