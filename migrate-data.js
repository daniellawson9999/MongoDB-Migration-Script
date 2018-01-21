const mongodb = require('mongodb');
const async = require('async');
const fs = require('fs');
const path = require('path');

//get command line argument, connect to client, const for doc size
const ObjectsPerQuery = process.argv[2];
const MongoClient = mongodb.MongoClient;
const DocumentSize = 1000;

//initialize paths
let customerAddressDataPath = path.join(__dirname,'data','m3-customer-address-data.json');
let customerDataPath = path.join(__dirname,'data','m3-customer-data.json');
let url = 'mongodb://localhost:27017/migration-database';

MongoClient.connect(url, (err,databases) => {
  //keep track of time
  let start = new Date();
  if(err) return process.exit(1);
  //connect to database
  let db =  databases.db('migration-database');
  let data = db.collection('customer-data');
  //use ReadData which reads and parses JSON data
  ReadData((customerArray, addressArray) => {
    //when done, start to build the queries
    let queries = [];
    //loop equal to the number of queries
    for(let i = 0; i < DocumentSize/ObjectsPerQuery; i++){
      //builds contents of a query, with a doc size equal to command line argument
      let contents = [];
      let j = 0;
      //add the right amount of docs to content
      while(j < ObjectsPerQuery && j < DocumentSize){
        //figure out the current document place, and then combine and add to contents
        let current = i*ObjectsPerQuery + j;
        let entry = {};
        for(key in customerArray[j]) entry[key] = customerArray[j][key];
        for(key in addressArray[j]) entry[key] = addressArray[j][key];
        contents.push(entry);
        j++;
      }
      //create a query function to insert each content, and add query function to the queries array
      let query = (done) => {
        data
        .insertMany(contents, (error,results) => {
          done(error,results);
        });
      }
      queries.push(query);
    }
    //run all queries in paralell, print status and elapsed time when done
    async.parallel(queries, (error,results) => {
      if(error) console.error(error);
      console.log(results.length + ' queries, with ' + ObjectsPerQuery + ' objects per query' + ', completed in ' + (new Date()-start)/1000 + ' seconds');
      databases.close();
    });
  });

});

function ReadData(callback){
  fs.readFile(customerDataPath,{encoding: 'utf-8'},(err,customerData) => {
    if(err) return console.error(err);
    fs.readFile(customerAddressDataPath,{encoding: 'utf-8'}, (err,addressData) => {
      if(err) return console.erro(err);
      let customerArray = JSON.parse(customerData);
      let addressArray = JSON.parse(addressData);
      callback(customerArray, addressArray);
      //console.log(`customer data length ${customerArray.length}, address length ${addressArray.length}\n`);
    });
  });
}
