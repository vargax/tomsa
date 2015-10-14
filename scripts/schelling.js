// IMPORTS -------------------------------------------------------------------------------------------------------------
import GeotabulaDB from 'geotabuladb'
import * as geoHelper from 'geotabuladb'

// CONSTANTS -----------------------------------------------------------------------------------------------------------
const _NEIGHBORS_TABLE_SUFFIX = '_neighbor';
const WORKERS = 10;

// CONFIG VARIABLES ----------------------------------------------------------------------------------------------------
let radius = 1000;
let populations = 3;
let iterations = 10;

let shape_table = 'manzanas';
let shape_table_columns = ['gid','geom'];
let gid = shape_table_columns[0];
let geom = shape_table_columns[1];

let out_table = 'schelling';
let out_table_colums = ['t','currentPop'];
let time = out_table_colums[0];
let currentPop = out_table_colums[1];

let neighbors_table = out_table+_NEIGHBORS_TABLE_SUFFIX;
let neighbor_gid = gid+_NEIGHBORS_TABLE_SUFFIX;
let neighbor_distance = 'lineal_distance';

// QUEUE VARIABLES -----------------------------------------------------------------------------------------------------
let queue = [
    genInitialPopulation,
    schelling
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
console.log('|-----------------------------------------------------\n');

input = rl.question("Output table name ('"+out_table+"'): ");
if (input.length != 0) {
    out_table = input;
    neighbors_table = out_table+_NEIGHBORS_TABLE_SUFFIX;
}
console.log("|-> Output table name set to '"+out_table+"'");

input = rl.question("Calculate neighbors -> would take some time! (no): yes/no ");
if (input.length != 0 && input === 'yes') {
    input = rl.question("|-> Radius ("+radius+' meters): ');
    if (input.length != 0) radius = Number.parseInt(input);
    console.log('|--> Radius set to '+radius+' meters');
    addTask(calculateNeighbors);
}

input = rl.question("Populations ("+populations+' different populations): ');
if (input.length != 0) populations = Number.parseInt(input);
console.log('|-> Populations set to '+populations+' different populations');

input = rl.question("Iterations ("+iterations+' schelling iterations): ');
if (input.length != 0) iterations = Number.parseInt(input);
console.log('|-> Iterations set to '+iterations+'');

// Async-required part ---------------------------------------------------------
rl = require('readline').createInterface({
    input: process.stdin,
    output: process.stdout
});
rl.question("\nPress ENTER to start or Ctrl+c to cancel...", function() {
    processQueue();
});

// QUEUE ---------------------------------------------------------------------------------------------------------------
function processQueue() {
    if (!currentTask) {
        currentTask = queue.shift();
        try {
            currentTask();
        } catch (e) {
            console.log('\nDONE!');
            console.dir(e);
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
function registerSteps(numSteps = 1) {
    remainingSteps += numSteps;
}

/**
 * Adds a new task to the beginning of the queue --> this task would be executed on the completion of the current task.
 * @param nextTask :: Reference to the function to be executed.
 */
function addTask(nextTask) {
    if (Array.isArray(nextTask)) {
        let task = nextTask.pop();
        while (task != undefined) {
            queue.unshift(task);
            task = nextTask.pop();
        }
    } else {
        queue.unshift(nextTask);
    }
}
/**
 * Adds a new task to the end of the queue.
 * @param lastTask :: Reference to the function to be executed.
 */
function pushTask(lastTask) {
    queue.push(lastTask);
}

// MAIN FUNCTIONS ------------------------------------------------------------------------------------------------------
function calculateNeighbors() {
    console.log('\ncalculateNeighbors()');
    registerSteps();
    addTask([clean, createTables, runQuery]);
    processQueue();

    function clean() {
        console.log('|-> clean()');
        registerSteps(2);

        geo.query(geoHelper.QueryBuilder.dropTable(out_table), processQueue);
        geo.query(geoHelper.QueryBuilder.dropTable(neighbors_table), processQueue);
    }

    function createTables() {
        console.log('|-> createTables()');
        registerSteps(2);

        // Main table for schelling model...
        let queryParams = {
            tableName: shape_table,
            properties: [gid, geom]
        };
        let query = geoHelper.QueryBuilder.copyTable(out_table, queryParams);

        let columns = [
            [time, geoHelper.INT],
            [currentPop, geoHelper.INT]
        ];
        query += geoHelper.QueryBuilder.addColumns(out_table, columns);
        query += geoHelper.QueryBuilder.update({
            tableName: out_table,
            values: [
                [time,0],
                [currentPop,-1]
            ],
            where: time+' is null'
        });
        query += 'ALTER TABLE '+out_table+' ADD CONSTRAINT '+out_table+'_pk PRIMARY KEY('+time+','+gid+');';
        geo.query(query, processQueue);

        // Table for neighbors calculations...
        columns = [
            [gid, geoHelper.INT], [neighbor_gid, geoHelper.INT],
            [neighbor_distance, geoHelper.FLOAT]
        ];
        query = geoHelper.QueryBuilder.createTable(neighbors_table, columns);
        query += 'ALTER TABLE '+neighbors_table+' ADD CONSTRAINT '+neighbors_table+'_pk PRIMARY KEY('+gid+','+neighbor_gid+');';
        geo.query(query, processQueue);

        vacuum();
    }

    function runQuery() {
        console.log('|-> runQuery()');

        let query;

        query = 'INSERT INTO '+neighbors_table;
        query+= ' SELECT '+shape_table+'.'+gid+',neighbor.'+gid;
        query+= ' ,ST_Distance(neighbor.'+geom+','+shape_table+'.'+geom+')';
        query+= ' FROM '+shape_table+','+shape_table+' neighbor';
        query+= ' WHERE ST_DWithin(neighbor.'+geom+','+shape_table+'.'+geom+','+radius+')';
        query+= ' LIMIT 10';

        registerSteps();
        geo.query(query, function() {
            console.log('|--> DONE neighbors calculation!');
            processQueue();
        });

        vacuum();
    }
}

function genInitialPopulation() {
    console.log('\ngenInitialPopulation()');
    clean();

    function clean() {
        console.log('|-> clean()');
        let query = 'DELETE FROM '+out_table+' WHERE '+time+'<> 0;';
        query += 'UPDATE '+out_table+' SET '+currentPop+'=-1;';
        registerSteps();
        geo.query(query, countBlocks);
    }

    function countBlocks() {
        console.log('|-> countBlocks()');
        let query = 'SELECT count(*) FROM '+out_table+';';
        registerSteps();
        geo.query(query, setPopulation);
        processQueue();
    }

    function setPopulation(numBlocks) {
        console.log('|-> setPopulation()');

        numBlocks = Number.parseInt(numBlocks[0]['count']);
        let limit = Math.round(numBlocks/(populations+1));

        let query='';
        let population = populations;
        while (population != 0) {
            query+= 'UPDATE '+out_table+' SET '+currentPop+'='+population;
            query+= ' FROM ('
                    +' SELECT '+gid+' FROM '+out_table
                    +' WHERE '+currentPop+'=-1 AND '+gid+'>=random()*'
                    +' (SELECT count(*) FROM '+out_table+' WHERE '+currentPop+'=-1)'
                    +' LIMIT '+limit
                    +')AS target';
            query+= ' WHERE '+out_table+'.'+gid+'=target.'+gid+';';
            population--;
        }
        query+='UPDATE '+out_table+' SET '+currentPop+'=0 WHERE '+currentPop+'= -1;';

        registerSteps();
        geo.query(query, function() {
            console.log('|--> DONE Initial population generation!');
            processQueue();
        });
        vacuum();
        processQueue();
    }
}

let currentIteration = 1;
function schelling() {
    console.log('\nschelling()');
    loadNeighbors();

    let neighbors = new Map();
    function loadNeighbors() {
        console.log('|-> loadNeighbors()');
        queryBlocks();

        function queryBlocks() {
            console.log('|--> queryBlocks()');
            let query = 'SELECT '+gid+' FROM '+out_table+';';
            registerSteps();
            geo.query(query, processBlocks);
        }

        let blocks = [];
        let totalBlocks;
        function processBlocks(allBlocks) {
            console.log('|--> processBlocks()');
            for (let block of allBlocks) {
                blocks.push(block[gid]);
            }

            console.log('|--> registerNeighbors()');
            totalBlocks = blocks.length;
            process.stdout.write('Loading neighbors to RAM: ');

            for (let worker = 0; worker < WORKERS; worker++) {}
                registerNeighbors();

            processQueue();
        }

        let doneBlocks = 0;
        let nextReport = 0;
        let hash2block = new Map();
        function registerNeighbors(blockNeighbors, hash) {
            if (hash == undefined) { // --> Recursion base condition
                registerSteps();
            } else {
                let block = hash2block.get(hash);
                hash2block.delete(hash);

                let myNeighbors = [];
                for (let neighbor of blockNeighbors) {
                    myNeighbors.push(neighbor[gid]);
                }

                neighbors.set(block, myNeighbors);
                doneBlocks++;

                let progress = doneBlocks/totalBlocks;

                if(progress > nextReport) {
                    let report = progress*100;
                    process.stdout.write(' '+report.toFixed(0)+'%');
                    nextReport += 0.05;
                }
            }

            let nextBlock = blocks.shift();
            if (nextBlock != undefined) { // --> Recursion termination condition
                let query = 'SELECT '+neighbor_gid+' FROM '+neighbors_table+' WHERE '+gid+'='+nextBlock+';';
                registerSteps();
                hash2block.set(geo.query(query, registerNeighbors),nextBlock);
            } else {
                console.log('|---> DONE neighbors load! :: '+neighbors.size+' bocks registered');
            }

            processQueue();
        }

    }

    function getLastIteration() {
        let query = 'SELECT '+gid+','+currentPop+' FROM '+out_table+' WHERE '+time+'='+(currentIteration-1);
        registerSteps();
        geo.query(query, searchCandidates);
    }

    function searchCandidates(lastIteration) {

        processQueue();
    }

}

// SUPPORT FUNCTIONS ---------------------------------------------------------------------------------------------------
function vacuum() {
    addTask(function() {
        console.log('|-> VACUUM');
        registerSteps();
        geo.query('VACUUM', processQueue);
    });
}