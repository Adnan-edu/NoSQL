
//C.1. Creating the Databases and Importing Data.

//5. Create a keyspace called FIT5137A1_MRDB

CREATE KEYSPACE FIT5137A1_MRDB WITH replication = {'class':'SimpleStrategy',
'replication_factor':1};

//6. Create the following column families - user_ratings

CREATE TYPE PersonalTraits( 
      birth_year INT, 
      weight INT, 
      height DOUBLE, 
      marital_status TEXT
);
 CREATE TYPE UserPersonality( 
      interest TEXT, 
      type_of_worker TEXT, 
      fav_color TEXT, 
      drink_level TEXT
);
CREATE TYPE UserPreferences( 
      budget TEXT, 
      smoker BOOLEAN, 
      dress_preference TEXT,
      ambience TEXT, 
      transport TEXT,
);
CREATE TYPE OtherDemographics( 
      religion TEXT, 
      employment TEXT,
);
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
);


//6. Create the following column families - place_ratings


CREATE TYPE PlaceAddress( 
      street TEXT, 
      city TEXT, 
      state TEXT,
      country TEXT
);

CREATE TYPE PlaceFeatures( 
      alcohol TEXT, 
      smoking_area TEXT, 
      dress_code TEXT,
      accessibility TEXT,
      price TEXT,
      franchise TEXT,
      area TEXT,
      other_services TEXT
);


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
);

//7.a Using the cassandra COPY command import the following data into the tables - user_ratings

copy user_ratings (rating_id,user_id,place_id,rating_place,rating_food,rating_service,user_personal_traits,user_personality, user_preferences, user_other_demographics, user_fav_cuisines,user_fav_payment_method) from 'user_ratings.csv' with HEADER = TRUE;

//7.b Using the cassandra COPY command import the following data into the tables - place_ratings 

copy place_ratings(rating_id, user_id, place_id, rating_place, rating_food, rating_service, place_name, place_address, place_features, parking_arrangements, accepted_payment_modes, cuisines) from 'place_ratings.csv' with HEADER = TRUE;


//C.2. Modifying the Databases.

//4. Update user_ratings - IF EXISTS

UPDATE user_ratings SET user_fav_cuisines = user_fav_cuisines - {'Fast_Food'}, user_fav_payment_method = user_fav_payment_method - {'cash'}, user_fav_payment_method = user_fav_payment_method + {'debit_cards'} WHERE user_id= 1108 and rating_id IN(65, 66, 67, 68, 69, 70, 71, 72, 73, 74);

//4. Update user_id = 1063


DELETE FROM user_ratings WHERE user_id = 1063 AND rating_id = 137 IF EXISTS;
DELETE FROM user_ratings WHERE user_id = 1063 AND rating_id = 138 IF EXISTS;
DELETE FROM user_ratings WHERE user_id = 1063 AND rating_id = 139 IF EXISTS;
DELETE FROM user_ratings WHERE user_id = 1063 AND rating_id = 140 IF EXISTS;
DELETE FROM user_ratings WHERE user_id = 1063 AND rating_id = 141 IF EXISTS;


//5.

INSERT INTO user_ratings (rating_id,user_id,place_id,rating_place,rating_food,rating_service,user_personal_traits,user_personality, user_preferences, user_other_demographics, user_fav_cuisines,user_fav_payment_method) VALUES(7777, 1060, 70000, 2, 1, 2, {birth_year: 1991, weight: 82, height: 1.84, marital_status: 'single'}, {interest: 'technology', type_of_worker: 'thrifty-protector', fav_color: 'blue', drink_level: 'casual drinker'}, {budget: 'medium', smoker: FALSE, dress_preference: 'formal', ambience: 'family', transport: 'public'}, {religion: 'Catholic', employment: 'student'}, {'Burgers', 'Cafeteria', 'Pizzeria', 'Juice', 'American', 'Tex-Mex', 'Spanish', 'Mexican', 'Fast_Food', 'Cafe-Coffee_Shop', 'Soup', 'Hot_Dogs', 'Italian'}, {'cash'});


//C3 Querying the Management.

//3. How many reviews were made in the database?

SELECT COUNT(*) FROM place_ratings;

//4. How many reviews were created by places having public parking arrangements?

CREATE INDEX ON place_ratings (parking_arrangements);

SELECT COUNT (*) FROM place_ratings WHERE parking_arrangements = 'public';

//5 Display the user id, rating of places and the user’s personality for all users who are
//interested ...

CREATE INDEX ON user_ratings (user_personality); 

SELECT user_id, rating_place, user_personality FROM user_ratings where user_personality={interest: 'technology', type_of_worker: 'thrifty-protector', fav_color: 'blue', drink_level: 'casual drinker'};

//6 What are the place ids and ratings for food for places serving only Pizzeria cuisine?

CREATE INDEX ON place_ratings (cuisines); 

SELECT place_id, rating_food, cuisines FROM place_ratings where cuisines CONTAINS 'Pizzeria';


//10. Display the average place rating, average food rating, and average service rating for
//puesto de tacos. Show the average values in decimal points.

CREATE INDEX ON place_ratings (place_name);

SELECT AVG(CAST(rating_place as decimal)) AS rating_place, AVG(CAST(rating_food as decimal)) AS rating_food, AVG(CAST(rating_service as decimal)) AS rating_service, place_name FROM place_ratings WHERE place_name='puesto de tacos' GROUP BY place_id;

//12. For each user whose favourite cuisine is Family, display the place ID, the place rating, the //food rating and the user’s budget.

CREATE INDEX ON user_ratings (user_fav_cuisines);

SELECT place_id, rating_place, rating_food, user_preferences.budget FROM user_ratings WHERE user_fav_cuisines CONTAINS 'Family';

//5 Extra query of C3
//4. List user_id, rating_place and user_personality of all the users other_demographics - religion: //'Catholic', employment: 'professional'


CREATE INDEX ON user_ratings (user_other_demographics);


SELECT user_id, rating_place, user_personality FROM user_ratings where user_other_demographics={religion: 'Catholic', employment: 'professional'};
