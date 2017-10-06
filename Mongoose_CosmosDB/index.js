var mongoose = require('mongoose');
var env = require('dotenv').load();

mongoose.connect(process.env.COSMOSDB_CONNSTR+process.env.COSMOSDB_DBNAME+"?ssl=true&replicaSet=globaldb"); //Creates a new DB, if it doesn't already exist

var db = mongoose.connection;
db.on('error', console.error.bind(console, 'connection error:'));
db.once('open', function() {
  // we're connected!
  console.log("Connected to DB");
});


/** Create Schema **/
//Use this way, if you'd like to store one type of data per collection. 
//This method could be expensive since CosmosDB charges per collection. 
//In this case, 2 collections will be created. One for Family, the other for VacationDestinations
const Family = mongoose.model('Family', new mongoose.Schema({
    lastName: String,
    parents: [{
        familyName: String,
        firstName: String,
        gender: String
    }],
    children: [{
        familyName: String,
        firstName: String,
        gender: String,
        grade: Number
    }],
    pets:[{
        givenName: String
    }],
    address: {
        country: String,
        state: String,
        city: String
    }
}));

const family = new Family({
    lastName: "Volum",
        parents: [
            { firstName: "Thomas" },
            { firstName: "Mary Kay" }
        ],
        children: [
            { firstName: "Ryan", gender: "male", grade: 8 },
            { firstName: "Patrick", gender: "male", grade: 7 }
        ],
        pets: [
            { givenName: "Blackie" }
        ],
        address: { country: "USA", state: "WA", city: "Seattle" }
});

family.save((err, saveFamily) => {
    console.log(JSON.stringify(saveFamily));
});

const VacationDestinations = mongoose.model('VacationDestinations', new mongoose.Schema({
    name: String,
    country: String
}));

const vacaySpot = new VacationDestinations({
    name: "Honolulu",
    country: "USA"
});

vacaySpot.save((err, saveVacay) => {
    console.log(JSON.stringify(saveVacay));
});


/** Using Discriminators **/
//This will create only 1 collection in CosmosDB, thus optimizing costs.
const baseConfig = {
    discriminatorKey: "_type", //If you've got a lot of different data types, you could also consider setting up a secondary index here.
    collection: "alldata"
}

const commonModel = mongoose.model('Common', new mongoose.Schema({}, baseConfig));

const Family_common = commonModel.discriminator('FamilyType', new mongoose.Schema({
    lastName: String,
    parents: [{
        familyName: String,
        firstName: String,
        gender: String
    }],
    children: [{
        familyName: String,
        firstName: String,
        gender: String,
        grade: Number
    }],
    pets:[{
        givenName: String
    }],
    address: {
        country: String,
        state: String,
        city: String
    }
}, baseConfig));

const Vacation_common = commonModel.discriminator('VacationDestinationsType', new mongoose.Schema({
    name: String,
    country: String
}, baseConfig));

const family_common = new Family_common({
    lastName: "Volum",
    parents: [
        { firstName: "Thomas" },
        { firstName: "Mary Kay" }
    ],
    children: [
        { firstName: "Ryan", gender: "male", grade: 8 },
        { firstName: "Patrick", gender: "male", grade: 7 }
    ],
    pets: [
        { givenName: "Blackie" }
    ],
    address: { country: "USA", state: "WA", city: "Seattle" }
});

family_common.save((err, saveFamily) => {
    console.log("Saved: " + JSON.stringify(saveFamily));
});

const vacay_common = new Vacation_common({
    name: "Honolulu",
    country: "USA"
});

vacay_common.save((err, saveVacay) => {
    console.log("Saved: " + JSON.stringify(saveVacay));
});


/** Reading data from CosmosDB - without discriminator **/
Family.find({ 'children.gender' : "male"}, function(err, foundFamily){
    foundFamily.forEach(fam => console.log("Found Family: " + JSON.stringify(fam)));
});

/** Reading data from CosmosDB - with discriminator **/
Family_common.find({ 'children.gender' : "male"}, function(err, foundFamily){
    foundFamily.forEach(fam => console.log("Found Family (using discriminator): " + JSON.stringify(fam)));
});