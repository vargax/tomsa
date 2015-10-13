// IMPORTS -------------------------------------------------------------------------------------------------------------
import GeotabulaDB from 'geotabuladb'
import * as geoHelper from 'geotabuladb'

// CONSTANTS -----------------------------------------------------------------------------------------------------------
const _NEIGHBORS_TABLE_SUFFIX = '_neighbor';

// CONFIG VARIABLES ----------------------------------------------------------------------------------------------------
let radius = 1000;
let timeout = 100;

let shape_table = 'manzanas';
let shape_column_sptObjId = 'gid';
let shape_column_sptObjGeom = 'geom';

let out_table = 'schelling';
let neighbors_table = out_table+_NEIGHBORS_TABLE_SUFFIX;

let neighbors_table_columns = ['gid','neighbor_gid','lineal_distance'];

// QUEUE VARIABLES -----------------------------------------------------------------------------------------------------
let queue = [
    clean,
    createTables,
    calculateNeighbors
];
let currentTask = null;
let remainingSteps = 0;

// SCRIPT --------------------------------------------------------------------------------------------------------------
let geo = new GeotabulaDB();
geo.setCredentials({
    user: 'tomsa',
    password: 'tomsa',
    database: 'tomsa'
});

// Sync-required part ----------------------------------------------------------
let rl = require('readline-sync');
let input;

console.log('\n|-----------------------------------------------------');
console.log("| TOMSA :: iter1 :: Basic radius-based Schelling Model");
console.log('|----------------------------------------------------- \n');

input = rl.question("Output table name ('"+out_table+"'): ");
if (input.length != 0) {
    out_table = input;
    neighbors_table = out_table+_NEIGHBORS_TABLE_SUFFIX;
}
console.log("|--> Output table name set to '"+out_table+"'");

input = rl.question("Radius ("+radius+' meters): ');
if (input.length != 0) radius = Number.parseInt(input);
console.log('|--> Radius set to '+radius+' meters');

input = rl.question("Timeout ("+timeout+' milisecs): ');
if (input.length != 0) timeout = Number.parseInt(input);
console.log('|--> Timeout set to '+timeout+' milisecs');

// Async-required part ---------------------------------------------------------
rl = require('readline').createInterface({
    input: process.stdin,
    output: process.stdout
});
rl.question("Press ENTER to start or Ctrl+c to cancel... ", function() {
    processQueue();
});

// QUEUE ---------------------------------------------------------------------------------------------------------------
function processQueue() {
    if (!currentTask) {
        currentTask = queue.shift();
        try {
            currentTask();
        } catch (e) {
	        console.dir(e);
            console.log('DONE!');
        }
    } else {
        remainingSteps--;
        if (remainingSteps == 0) {
            currentTask = null;
            processQueue();
        }
    }
}

/**
 * Registers a new step for the current task.
 */
function registerSteps(numSteps) {
    if (numSteps == undefined) {
        remainingSteps++;
    } else {
        remainingSteps += numSteps;
    }
}

/**
 * Adds a new task to the beginning of the queue --> this task would be executed on the completion of the current task.
 * @param nextTask :: Reference to the function to be executed.
 */
function addTask(nextTask) {
    queue.unshift(nextTask);
}
/**
 * Adds a new task to the end of the queue.
 * @param lastTask :: Reference to the function to be executed.
 */
function pushTask(lastTask) {
    queue.push(lastTask);
}

// FUNCTIONS -----------------------------------------------------------------------------------------------------------
function clean() {
    console.log('clean()');
    registerSteps(2);

    geo.query(geoHelper.QueryBuilder.dropTable(out_table), processQueue);
    geo.query(geoHelper.QueryBuilder.dropTable(out_table+_NEIGHBORS_TABLE_SUFFIX), processQueue);
}

function createTables() {
    console.log('createTables()');
    registerSteps(2);

    // Main table for schelling model...
    let queryParams = {
        tableName: shape_table,
        properties: [shape_column_sptObjId, shape_column_sptObjGeom]
    };
    let query = geoHelper.QueryBuilder.copyTable(out_table, queryParams);

    let columns = [
        ['t', geoHelper.INT],
        ['currentPop', geoHelper.INT]
    ];
    query += geoHelper.QueryBuilder.addColumns(out_table, columns);
    query += geoHelper.QueryBuilder.update({
        tableName: out_table,
        values: [
            ['t',0],
            ['currentPop',-1]
        ],
        where: 't is null'
    });
    query += 'ALTER TABLE '+out_table+' ADD CONSTRAINT '+out_table+'_pk PRIMARY KEY(t,'+shape_column_sptObjId+');';
    geo.query(query, processQueue);

    // Table for neighbors calculations...
    columns = [
        [neighbors_table_columns[0], geoHelper.INT], [neighbors_table_columns[1], geoHelper.INT],
        [neighbors_table_columns[2], geoHelper.FLOAT]
    ];
    query = geoHelper.QueryBuilder.createTable(neighbors_table, columns);
    query += 'ALTER TABLE '+neighbors_table+' ADD CONSTRAINT '+neighbors_table+'_pk PRIMARY KEY('+neighbors_table_columns[0]+','+neighbors_table_columns[1]+');';
    geo.query(query, processQueue);

    addTask(function() {
        console.log('|-> VACUUM');
        registerSteps();
        geo.query('VACUUM', processQueue);
    });
}

function calculateNeighbors() {
    console.log('calculateNeighbors()');
    getAllBlocks();
    addTask(function() {
        console.log('|-> VACUUM');
        registerSteps();
        geo.query('VACUUM', processQueue);
    });

    // SUPPORT FUNCTIONS -----------------------------------------------------------------------------------------------
    function getAllBlocks() {
        console.log('|->getAllBlocks()');
        let queryParams = {
            tableName: shape_table,
            properties: [shape_column_sptObjId]
            //where: 'pob > 0'
            //limit: 100
        };

        registerSteps();
        geo.query(queryParams, lookForCloseBlocks);
    }

    let totalBlocks;
    let doneBlocks=0;
    let queries = [];
    function lookForCloseBlocks(allBlocks) {
        console.log('|->lookForCloseBlocks()');
        totalBlocks = allBlocks.length;
        console.log('|-->Looking for blocks at ' + radius + ' meters of a total of ' + totalBlocks + ' blocks...');

        for (let block of allBlocks) {

            let currentBlockId = block[shape_column_sptObjId];
            let query;

            query = 'INSERT INTO ' + neighbors_table;
            query += ' SELECT ' + shape_table + '.' + shape_column_sptObjId + ',neighbor.' + shape_column_sptObjId
                + ',ST_Distance(neighbor.' + shape_column_sptObjGeom + ',' + shape_table + '.' + shape_column_sptObjGeom + ')';
            query += ' FROM ' + shape_table + ',' + shape_table + ' neighbor';
            query += ' WHERE ' + shape_table + '.' + shape_column_sptObjId + '=' + currentBlockId
                + 'AND ST_DWithin(neighbor.' + shape_column_sptObjGeom + ',' + shape_table + '.' + shape_column_sptObjGeom + ',' + radius + ')';
            //query+= ' LIMIT 1000';

            queries.push(query);
        }
        recursiveSetTimeOut();
        processQueue();
    }

    function recursiveSetTimeOut() {
        let query = queries.shift();
        if (query != undefined) {
            registerSteps();
            setTimeout(function() {
                geo.query(query, blockDone);
                recursiveSetTimeOut();
            }, timeout);
        }
    }

    function blockDone() {
        doneBlocks++;
        console.log('|---> '+(doneBlocks/totalBlocks).toFixed(4)+' complete ('+doneBlocks+' of '+totalBlocks+' queries done)');
        processQueue();
    }
}

function schelling() {
    console.log('schelling()');


    function genInitialPop() {
        console.log('|->genInitialPop()');

    }
}
