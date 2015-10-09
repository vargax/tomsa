// IMPORTS -------------------------------------------------------------------------------------------------------------
import GeotabulaDB from 'geotabuladb'
import * as geoHelper from 'geotabuladb'

// CONSTANTS -----------------------------------------------------------------------------------------------------------
const _RADIUS_TABLE = '_rad';

// CONFIG VARIABLES ----------------------------------------------------------------------------------------------------
let radius = 1000;
let timeout = 100;

let inShapeTable = 'manzanas';
let inShapeColumns = ['gid','geom']; // --> inShapeColumns[0] = spatialObject id
                                     // --> inShapeColumns[1] = spatialObject geometry

let outTable = 'schelling';

// QUEUE VARIABLES -----------------------------------------------------------------------------------------------------
let queue = [
    clean,
    createTables
    //calculateNeighbors
];
let currentTask = null;
let remainingSteps = 0;

// SCRIPT --------------------------------------------------------------------------------------------------------------
let rowsCount = 0;
let geo = new GeotabulaDB();
geo.setCredentials({
    user: 'tomsa',
    password: 'tomsa',
    database: 'tomsa'
});

// Sync-required part ----------------------------------------------------------
let rl = require('readline-sync');
let input;

console.log("TOMSA :: iter1 :: Basic radius-based Schelling Model");
input = rl.question("Output table name ('"+outTable+"'): ");
if (input.length != 0) ODtableName = input;
console.log("|--> Output table name set to '"+outTable+"'");

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
            console.log('DONE!')
        }
    } else {
        remainingSteps--;
        if (remainingSteps == 0) {
            currentTask = null;
            processQueue();
        }
    }
}

function registerStep() {
    remainingSteps++;
}

function addTask(nextTask) {
    queue.unshift(nextTask);
}

function pushTask(lastTask) {
    queue.push(lastTask);
}

// FUNCTIONS -----------------------------------------------------------------------------------------------------------
function clean() {
    console.log('clean()');
    registerStep();
    geo.query(geoHelper.QueryBuilder.dropTable(outTable), processQueue);

    registerStep();
    geo.query(geoHelper.QueryBuilder.dropTable(outTable+_RADIUS_TABLE), processQueue);
}

function createTables() {
    console.log('createTables()');

    // Main table for schelling model...
    let queryParams = {
        tableName: inShapeTable,
        properties: inShapeColumns
    };
    let query = geoHelper.QueryBuilder.copyTable(outTable, queryParams);

    let columns = [
        ['t', geoHelper.INT],
        ['currentPop', geoHelper.INT]
    ];
    query += geoHelper.QueryBuilder.addColumns(outTable, columns);
    query += geoHelper.QueryBuilder.update({
        tableName: outTable,
        values: [
            ['t',0],
            ['currentPop',-1]
        ],
        where: 't is null'
    });
    query += 'ALTER TABLE '+outTable+' ADD CONSTRAINT pk PRIMARY KEY(t,'+inShapeColumns[0]+');';

    registerStep();
    geo.query(query, processQueue);

    // Table for neighbors calculations...
    columns = [
        ['id', geoHelper.PK],
        ['gid', geoHelper.INT], ['neighbor_gid', geoHelper.INT],
        ['lineal_distance', geoHelper.FLOAT]
    ];
    query = geoHelper.QueryBuilder.createTable(outTable+_RADIUS_TABLE, columns);

    registerStep();
    geo.query(query, processQueue);

    function vacuum() {
        console.log('VACUUM');
        registerStep();
        geo.query('VACUUM', processQueue);
    }
    addTask(vacuum);
}

function calculateNeighbors() {

}