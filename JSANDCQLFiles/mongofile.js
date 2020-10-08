//C.1. Creating the Databases and Importing Data.

//1. create a database called: FIT5137A1MRDB

use FIT5137A1MRDB

//2
//Mongo Shell command(s), create the following 2 collections userProfiles and placeProfiles

db.createCollection("userProfiles")

db.createCollection("placeProfiles")

db.createCollection("openingHours")

//3. After importing data using mongo compass
//embed the corresponding opening hours in the placeProfiles

 db.openingHours.aggregate([  
  {
   
    $group: {

      _id:{placeID:"$placeID"},
      openingHour: {$push: {$concat:["$days", "$hours"]}}

      }

  },
  {
      $addFields:{
      "_id":"$_id.placeID"
      }
  },
  {
    $merge:{
      into: 'placeProfiles',
      on: '_id',
      whenMatched: 'merge',
      whenNotMatched: 'discard'
    }
  }

]).pretty()


//C2 - 1

db.placeProfiles.insertOne({
  _id:70000,
  acceptedPaymentModes: "any",
  address: {
        city: "San Luis Potosi",
        country: "Mexico",
        state: "SLP",
        street: "Carretera Central Sn"
      },
  cuisines: "Mexican, Burgers",
  parkingArragements: "none",
  placeFeatures: {
      accessibility: "no_accessibility",
      alcohol: "No_Alcohol_Served",
      area: "closed",
      dressCode: "informal",
      franchise: "TRUE",
      otherServices: "Internet",
      price: "medium",
      smokingArea: "not permitted"
    },
    placeName: "Taco Jacks",
    openingHour: ["Mon;Tue;Wed;Thu;Fri;09:00-20:00;","Sat;12:00-18:00;","Sun;12:00-18:00;"]})


//C2-2

db.userProfiles.aggregate(
    [
        { "$addFields": 
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
      $merge:{
              into: 'userProfiles',
              on: '_id',
              whenMatched: 'merge',
              whenNotMatched: 'discard'
            }
      }
    ]
)

db.userProfiles.update(
    { _id: 1108 },
    { 
      $pull: { favCuisines: "Fast_Food",favPaymentMethod: "cash" }
    }
)

Then pushing “debit_cards” into favPaymentMethod array

db.userProfiles.update(
    { _id: 1108 },
    { 
      $push: { favPaymentMethod: "debit_cards" }
    }
)

//C2.3

db.userProfiles.remove({_id:1063});


//C3

//1.

db.userProfiles.find().count()

//2.

db.placeProfiles.find().count()

//7.

db.userProfiles.aggregate([ 
{ $project : {_id : 0}},
{ $match : {"otherDemographics.employment":"student","preferences.budget":"medium"} }
]).pretty()

//8.

db.placeProfiles.aggregate(
    [
        { "$addFields": 
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
      $merge:{
              into: 'placeProfiles',
              on: '_id',
              whenMatched: 'merge',
              whenNotMatched: 'discard'
            }
      }
    ]
)


db.userProfiles.aggregate([
   
   {$match: {favCuisines:"Bakery"}},
   {
      $lookup:
         {
           from: "placeProfiles",
           pipeline: [
              {$match: {cuisines: "Bakery"}},
              { $project: { _id: 0, placeName: 1 } }
           ],
           as: "bakeryRestaurants"
         }
    }
]).pretty()


//9.

db.placeProfiles.find(
  { openingHour : { $elemMatch : { "$regex": "^Sun.*" } }, "cuisines":"International"}).pretty()


//11.


db.userProfiles.aggregate([
  {
  $group: {
    _id: {
     "drinkLevel":"$personality.drinkLevel"
    }, 

  averageAge: {
    $avg: 

      {
        $divide: [{ $subtract: [ new Date(), new Date("$personalTraits.birthYear") ] },(365 * 24*60*60*1000)]
      }
  }

  }},
  { 
     $project: {  
        _id: 0,
        drinkLevel: "$_id.drinkLevel",
        averageAge: 1
     }
    },
    {$sort:{"drinkLevel":1}}
]).pretty()


//13.

db.userProfiles.aggregate([
  {
     $match: {"favCuisines": "Japanese" }
  },  
  {
   $group: {
        _id: "$preferences.ambience",
        totalAmount: { $sum: 1 },
      }

  },
  { 
    $project: {
      _id: 0,
      ambience: "$_id",
      totalAmount: 1
   }
},
{ $sort : { 'ambience' : 1,'totalAmount' : 1 } }, 
{ $limit : 3 }
]).pretty()


//14.

db.placeProfiles.aggregate([
{

     $unwind: "$cuisines"
}, 
{
     $group: {
             _id: {
                     _id: "$_id"
             },
             distinctCuisines: {
                     $addToSet: "$cuisines"
             }
     }
}, 
     {
     "$unwind": "$distinctCuisines"
     }, 
     {
       "$group": {
               _id: {
                       "distinctCuisines": "$distinctCuisines"
               } 
       }
     },
     { 
     $project: {  
        _id: 0,
        cuisinesName: "$_id.distinctCuisines"
     }
    },
    {$match:{"cuisinesName":{$ne: "" }}}

]).pretty()


//15.

db.placeProfiles.aggregate(
   [
      {
         $project:
           {
             _id: 0, 
             placeName: 1,
             includeMexicanCuisines:
               {
                 $cond: { if: { $in: [ "Mexican", "$cuisines" ] }, then: "serves mexican food", else: "doesn’t serves mexican food" }
               }
           }
      },
        {$sort:{"_id":1}}
   ]
)



//C3-Extra five questions

// 1. Display all of the restaurant's names in the city "San Luis Potosi" and their parking arrangement “public” 
//and order by place name. 

db.placeProfiles.aggregate(
   [
      {
         $match : {"address.city":"San Luis Potosi","parkingArragements":"public"} 
      },
      {
         $project:
           {
             _id: 0, 
             placeName: 1,
             parkingArragements: 1
           }
      },
        {$sort:{"placeName":1}}
   ]
).pretty()


//2.List total number of user for each preferences ambience

db.userProfiles.aggregate(
   [
      {
         $project:
           {
             _id: 0,
             preferences:1
           }
      },

    {
       $group:
       {
        _id:{
        "ambience":"$preferences.ambience"
        }, 

        totalNoPersons: { $sum: 1 } 
       }
    },
    { 
     $project: {  
        _id: 0,
        ambience: "$_id.ambience",
        totalNoPersons: 1
     }
    }, 
      {$sort:{"totalNoPersons":1}}
   ]
).pretty()



//3.No of restaurants group by smokingArea features

db.placeProfiles.aggregate([  
  {
     $group: {
        _id: {
         "smokingArea":"$placeFeatures.smokingArea"
        },
        totalAmount: { $sum: 1 },
      }
  },
  { 
     $project: {  
        _id: 0,
        smokingArea: "$_id.smokingArea",
        totalAmount: 1
     }
    },
    {$sort:{"totalAmount":-1}}
]).pretty()


 //5.Display all users who like to smoke and combine your output with all places not having smoking areas.

db.userProfiles.aggregate([
   
   {$match: {"preferences.smoker":true}},
   {
      $lookup:
         {
           from: "placeProfiles",
           pipeline: [
              {$match: {"placeFeatures.smokingArea": "not permitted"}},
              { $project: { _id: 0, placeName: 1 } }
           ],
           as: "smokingNotPermitted"
         }
    }
]).pretty()
