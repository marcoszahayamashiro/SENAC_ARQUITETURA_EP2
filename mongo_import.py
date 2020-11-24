import json 
from pymongo import MongoClient 


# Making Connection 
myclient = MongoClient("mongodb+srv://marcos:Mongo#4321@arquitetura.aafrk.mongodb.net/arquitetura?retryWrites=true&w=majority") 

# database 
db = myclient["arquitetura"] 

# Created or Switched to collection 
# names: GeeksForGeeks 
Collection = db["taxi"] 

# Loading or Opening the json file 
with open('taxi.json') as file: 
	file_data = json.load(file) 
	
# Inserting the loaded data in the Collection 
# if JSON contains data more than one entry 
# insert_many is used else inser_one is used 
if isinstance(file_data, list): 
	Collection.insert_many(file_data) 
else: 
	Collection.insert_one(file_data) 
