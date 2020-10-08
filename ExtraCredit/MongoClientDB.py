#!/usr/bin/env python3.8.3 anaconda-spyder
"""
Created on Sun Oct  4 16:05:34 2020

@author: adnan
"""

try:
    import pymongo
    from pymongo import MongoClient
    import pprint
    import pandas as pd
    import datetime
except Exception as e:
    print("We have missed some modules")
    
    
class MongoClientDB(object):
    def __init__(self, dBName=None, collectionUser=None, collectionPlaces=None, collectionOpeningHrs=None):
        self.dBName = dBName
        self.collectionUser = collectionUser
        self.collectionPlaces = collectionPlaces
        self.collectionOpeningHrs = collectionOpeningHrs
        
        self.client = MongoClient("localhost", 27017, maxPoolSize=50)
        self.DB = self.client[self.dBName]
        self.collectionUser = self.DB[self.collectionUser]
        self.collectionPlaces = self.DB[self.collectionPlaces]
        self.collectionOpeningHrs = self.DB[self.collectionOpeningHrs]
        if self.collectionUser.drop():
            print("Collection dropped")
        if self.collectionPlaces.drop():
            print("profilePlaces dropped")
        if self.collectionOpeningHrs.drop():
             print("Opening hours dropped")
        
    def InsertJsonData(self, path1=None, path2=None):
        dfUser = pd.read_json(path1)
        userData = dfUser.to_dict('records')
        
        dfPlaces = pd.read_json(path2)
        placeData = dfPlaces.to_dict('records')
        

        self.collectionUser.insert_many(userData, ordered=False)
        self.collectionPlaces.insert_many(placeData, ordered=False)
        #print("All the Data has been Exported to Mongo DB Server .... ")
        
    def InsertCSVData(self, path=None):
        df = pd.read_csv(path)
        data = df.to_dict('records')

        self.collectionOpeningHrs.insert_many(data, ordered=False)
        #print("All the Data has been Exported to Mongo DB Server .... ")
    #Mergine opening hours and placeProfiles    
    def mergeOprHrsPlcPrfs(self):
        result = self.collectionOpeningHrs.aggregate([{"$group": {"_id":{"placeID":"$placeID"},"openingHour": {"$push": {"$concat":["$days", "$hours"]}}}},

       {"$addFields":{"_id":"$_id.placeID"}},
       {"$merge":{"into": 'placeProfiles',"on": '_id',"whenMatched": 'merge',"whenNotMatched": 'discard'}}]) 
        pprint.pprint(result)
        
        
    def splitCuisinesPayment(self):
        result = self.collectionUser.aggregate([{ "$addFields": 
                { 
                    "favCuisines": { "$split": [ "$favCuisines", ", " ] } 
                }
    
                },
                { "$addFields": 
                      { 
                          "favPaymentMethod": { "$split": [ "$favPaymentMethod", ", " ] } 
                      }
        
              },      
              {
              "$merge":{
                      "into": 'userProfiles',
                      "on": '_id',
                      "whenMatched": 'merge',
                      "whenNotMatched": 'discard'
                    }
              }
              ]) 
        pprint.pprint(result)

    def popularAmbiences(self):
        print("Top 3 most popular ambiences (friends/ family/ solitary) for a single when going to a Japanese restaurant")
        result = self.collectionUser.aggregate([{
                     "$match": {"favCuisines": "Japanese" }
                  },  
                  {
                   "$group": {
                        "_id": "$preferences.ambience",
                        "totalAmount": { "$sum": 1 },
                      }
                
                  },
                  { 
                    "$project": {
                      "_id": 0,
                      "ambience": "$_id",
                      "totalAmount": 1
                   }
                },
                { "$sort" : { 'ambience' : 1,'totalAmount' : 1 } }, 
                { "$limit" : 3 }
                ])
        for res in result:
            print(res)
            
    def uniqueCuisines(self):
        print("List the names of unique cuisines in the database")
        result = self.collectionPlaces.aggregate([{

                     "$unwind": "$cuisines"
                }, 
                {
                     "$group": {
                             "_id": {
                                     "_id": "$_id"
                             },
                             "distinctCuisines": {
                                     "$addToSet": "$cuisines"
                             }
                     }
                }, 
                     {
                     "$unwind": "$distinctCuisines"
                     }, 
                     {
                       "$group": {
                               "_id": {
                                       "distinctCuisines": "$distinctCuisines"
                               } 
                       }
                     },
                     { 
                     "$project": {  
                        "_id": 0,
                        "cuisinesName": "$_id.distinctCuisines"
                     }
                    },
                    {"$match":{"cuisinesName":{"$ne": "" }}}])
        for res in result:
            print(res)

    def mexicanCuisinesRes(self):
        print("List of all restaurant includes mexican cuisines")
        result = self.collectionPlaces.aggregate([{
                     "$project":
                       {
                         "_id": 0, 
                         "placeName": 1,
                         "includeMexicanCuisines":
                           {
                             "$cond": { "if": { "$in": [ "Mexican", "$cuisines" ] }, "then": "serves mexican food", "else": "doesn’t serves mexican food" }
                           }
                       }
                  },
                    {"$sort":{"_id":1}}
            ])
        for res in result:
            print(res)

    def extraQuesOne(self):
        print("All restaurant's names in the city San Luis Potosi and their parking arrangement public and order by place name")
        result = self.collectionPlaces.aggregate([{
                      "$match": {"address.city":"San Luis Potosi","parkingArragements":"public"} 
                  },
                  {
                     "$project":
                       {
                         "_id": 0, 
                         "placeName": 1
                       }
                  },
                    {"$sort":{"placeName":1}}])
        for res in result:
            print(res)

    def extraQuesTwo(self):
        print("List total number of user for each preferences ambience")
        result = self.collectionUser.aggregate([{
                 "$project":
                   {
                     "_id": 0,
                     "preferences":1
                   }
              },
        
            {
               "$group":
               {
                "_id":"$preferences.ambience", "totalNoPersons": { "$sum": 1 } 
               }
            }, 
              {"$sort":{"totalNoPersons":1}}])
        for res in result:
            print(res)

    def extraQuesThree(self):
        print("No of restaurants group by smokingArea features:")
        result = self.collectionPlaces.aggregate([ {
                 "$group": {
                    "_id": {
                     "smokingArea":"$placeFeatures.smokingArea"
                    },
                    "totalAmount": { "$sum": 1 },
                  }
              },
              { 
                 "$project": {  
                    "_id": 0,
                    "smokingArea": "$_id.smokingArea",
                    "totalAmount": 1
                 }
                },
                {"$sort":{"totalAmount":-1}}])    
        for res in result:
            print(res)            
        
    def studentMediumBudget(self):
        result = self.collectionUser.aggregate([{ "$project" : {"_id" : 0}},
        { "$match" : {"otherDemographics.employment":"student","preferences.budget":"medium"} }
        ])
        print("All users who are students and prefer a medium budget restaurant.")
        for res in result:
             print(res)

    def restaurantsOpenSunday (self):
        print("International restaurants that are open on Sunday:")
        result = self.collectionPlaces.find({ "openingHour" : { "$elemMatch" : { "$regex": "^Sun.*" } }, "cuisines":"International"})
        for res in result:
            print(res)
            
    def averageAgeDrik(self):
        print("Average age according to each drinker level:")
        """ 
        result = self.collectionUser.aggregate([{
                  "$group": {"_id": "$personality.drinkLevel", "averageAge": {
                  "$avg": 
                
                  {
                    "$divide": [{ "$subtract": [ new Date(), new Date("$personalTraits.birthYear") ] },(365 * 24*60*60*1000)]
                  }
                  }
                
                  }}
                ])
        """   
        result = self.collectionUser.aggregate([{
                  "$group": {"_id": "$personality.drinkLevel", "averageAge": {
                  "$avg": 
                
                  {
                    "$divide": [{ "$subtract": [ datetime.datetime.now(), datetime.datetime('$personalTraits.birthYear') ] },(365 * 24*60*60*1000)]
                  }
                  }
                
                  }}
                ])
        
        
        for res in result:
            print(res)            
            
    def bakeryCuisines(self):
        self.collectionPlaces.aggregate([ { "$addFields": 
                      { 
                          "cuisines": { "$split": [ "$cuisines", ", " ] } 
                      }
        
                    },
                    { "$addFields": 
                          { 
                              "acceptedPaymentModes": { "$split": [ "$acceptedPaymentModes", ", " ] } 
                          }
            
                  },      
                  {
                  "$merge":{
                          "into": 'placeProfiles',
                          "on": '_id',
                          "whenMatched": 'merge',
                          "whenNotMatched": 'discard'
                        }
                  }
              ])
        print("all users who like Bakery cuisines and combine your output with all places having Bakery cuisines")
        userPreferBakery = self.collectionUser.aggregate([ {"$match": {"favCuisines": "Bakery" }},
               {
                  "$lookup":
                     {
                       "from": "placeProfiles",
                       "pipeline": [
                          {"$match": {"cuisines": "Bakery" }},
                          { "$project": { "_id": 0, "placeName": 1 } }
                       ],
                       "as": "bakeryRestaurants"
                     }
                }
            ])
        
        for preferCuisine in userPreferBakery:
            print(preferCuisine)
            
            
                      
        
        
if __name__ == "__main__":
    mongodb = MongoClientDB(dBName = 'FIT5137A1MRDB', collectionUser='userProfiles',collectionPlaces='placeProfiles',collectionOpeningHrs='openingHours')
    mongodb.InsertJsonData(path1="userProfile.json",path2="placeProfiles.json")
    mongodb.InsertCSVData(path="openingHours.csv")
    mongodb.mergeOprHrsPlcPrfs();
    
    #C2-1 Data insertion
    dataPlaceProfile = {
          "_id":70000,
          "acceptedPaymentModes": "any",
          "address": {
                "city": "San Luis Potosi",
                "country": "Mexico",
                "state": "SLP",
                "street": "Carretera Central Sn"
              },
          "cuisines": "Mexican, Burgers",
          "parkingArragements": "none",
          "placeFeatures": {
              "accessibility": "no_accessibility",
              "alcohol": "No_Alcohol_Served",
              "area": "closed",
              "dressCode": "informal",
              "franchise": "t",
              "otherServices": "Internet",
              "price": "medium",
              "smokingArea": "not permitted"
            },
            "placeName": "Taco Jacks",
            "openingHour": ["Mon;Tue;Wed;Thu;Fri;09:00-20:00;","Sat;12:00-18:00;","Sun;12:00-18:00;"]}
    insert_result = mongodb.collectionPlaces.insert_one(dataPlaceProfile)
    pprint.pprint(insert_result.acknowledged) 
    pprint.pprint(insert_result.inserted_id)
    
    #C2-2
    mongodb.splitCuisinesPayment();
    #Remove favCuisines Fast_Food and payment cash
    
    mongodb.collectionUser.update_one({ "_id": 1108 },
        { 
          "$pull": { "favCuisines": "Fast_Food","favPaymentMethod": "cash" }
        })
    mongodb.collectionUser.update_one({ "_id": 1108 },
        { 
          "$push": { "favPaymentMethod": "debit_cards" }
        })
    
    #C2-3
    
    deletedRow = mongodb.collectionUser.delete_one({"_id":1063});
    print(deletedRow.deleted_count, " documents deleted.")
    
    
    #C3 - 1
    numberOfUsers = mongodb.collectionUser.estimated_document_count()
    print("Number of users "+str(numberOfUsers))
    
    #C3 - 2
    numberOfPlaces = mongodb.collectionPlaces.estimated_document_count()     
    print("Number of places "+str(numberOfPlaces))
    
    #C3 - 7
    mongodb.studentMediumBudget()
    
    #C3 - 8
    mongodb.bakeryCuisines()
    
    #C3-9
    mongodb.restaurantsOpenSunday()
    
    #C3-11
    #mongodb.averageAgeDrik()
    
    #C3-13
    mongodb.popularAmbiences()
    
    #C3-14
    mongodb.uniqueCuisines()
    
    #C3-15
    mongodb.mexicanCuisinesRes()
    
    #Extra C3-16 (1)
    mongodb.extraQuesOne()
    
    #Extra C3-17 (2)
    mongodb.extraQuesTwo()  
    
    #Extra C3-18 (3)
    mongodb.extraQuesThree();
    
    
    
    
    

    
    