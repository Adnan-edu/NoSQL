# -*- coding: utf-8 -*-
"""
Created on Tue Oct  6 10:13:35 2020

@author: adnan
"""

try:
    from cassandra.cluster import Cluster
except Exception as e:
    print("We have missed some modules")
    
    

cluster = Cluster(['localhost'])
session = cluster.connect()

session.execute("DROP KEYSPACE IF EXISTS FIT5137A1_MRDB")

session.execute("CREATE KEYSPACE FIT5137A1_MRDB WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

session.set_keyspace('FIT5137A1_MRDB')

session.execute(""" 
    CREATE TYPE PersonalTraits( 
          birth_year INT, 
          weight INT, 
          height DOUBLE, 
          marital_status TEXT
    )
    """ )
session.execute(""" 
     CREATE TYPE UserPersonality( 
          interest TEXT, 
          type_of_worker TEXT, 
          fav_color TEXT, 
          drink_level TEXT
    )""" )
session.execute(""" 
    CREATE TYPE UserPreferences( 
          budget TEXT, 
          smoker BOOLEAN, 
          dress_preference TEXT,
          ambience TEXT, 
          transport TEXT,
    )""" )
    
session.execute("""     
    
        CREATE TYPE OtherDemographics( 
              religion TEXT, 
              employment TEXT,
        )""" )
session.execute("""        
        CREATE TABLE user_ratings (
        rating_id INT,
        user_id INT,
        place_id INT,
        rating_place INT,
        rating_food INT,
        rating_service INT,
        user_personal_traits FROZEN<PersonalTraits>,
        user_personality FROZEN<UserPersonality>, 
        user_preferences FROZEN<UserPreferences>,
        user_other_demographics FROZEN<OtherDemographics>,
        user_fav_cuisines set<text>, 
        user_fav_payment_method set<text>,
        PRIMARY KEY(user_id, rating_id)
        )
                 
                """)
                

session.execute(""" 
        CREATE TYPE PlaceAddress( 
              street TEXT, 
              city TEXT, 
              state TEXT,
              country TEXT
        )
        """)
session.execute(""" 
        CREATE TYPE PlaceFeatures( 
              alcohol TEXT, 
              smoking_area TEXT, 
              dress_code TEXT,
              accessibility TEXT,
              price TEXT,
              franchise TEXT,
              area TEXT,
              other_services TEXT
        )
        """)
session.execute("""         
        CREATE TABLE place_ratings (
        rating_id INT,
        user_id INT,
        place_id INT,
        rating_place INT,
        rating_food INT,
        rating_service INT,
        place_name TEXT,
        place_address FROZEN<PlaceAddress>, 
        place_features FROZEN<PlaceFeatures>,
        parking_arrangements TEXT,
        accepted_payment_modes set<text>,
        cuisines set<text>,
        PRIMARY KEY(place_id, user_id, rating_id)
        )
                """)
                

userRatings = session.prepare("""
        INSERT INTO user_ratings (rating_id,user_id,place_id,rating_place,rating_food,rating_service,user_personal_traits,user_personality, user_preferences, user_other_demographics, user_fav_cuisines,user_fav_payment_method)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?)
        """)

placeRatings = session.prepare("""
        INSERT INTO place_ratings (rating_id, user_id, place_id, rating_place, rating_food, rating_service, place_name, place_address, place_features, parking_arrangements, accepted_payment_modes, cuisines)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?)
        """)
count = 0
with open("place_ratings.csv", "r") as placeRating:
    for fare in placeRating:
        columns=fare.split(",")
        if count == 0:
            count = 1
            continue
        rating_id=int(columns[0])
        user_id=int(columns[1])
        place_id=int(columns[2])
        rating_place=int(columns[3])
        rating_food=int(columns[4])
        rating_service=int(columns[5])
        place_name=columns[6]
        place_address=columns[7]
        place_features=columns[8]
        parking_arrangements=columns[9]
        accepted_payment_modes=set(columns[10])
        cuisines=set(columns[11])
        
        session.execute(placeRatings, [rating_id, user_id, place_id, rating_place, rating_food, rating_service, place_name, place_address, place_features, parking_arrangements, accepted_payment_modes, cuisines])
        
#closing the file
placeRating.close()
                

#closing Cassandra connection
session.shutdown()                



