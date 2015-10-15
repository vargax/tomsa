// IMPORTS -------------------------------------------------------------------------------------------------------------
import GeotabulaDB from 'geotabuladb'
import * as geoHelper from 'geotabuladb'

// CONSTANTS -----------------------------------------------------------------------------------------------------------
const _NEIGHBORS_TABLE_SUFFIX = '_neighbor';
const WORKERS = 10;

// CONFIG VARIABLES ----------------------------------------------------------------------------------------------------
let radius = 1000;
let populations = 3;
let tolerance = 0.3;
let iterations = 10;

let shape_table = 'manzanas';
let shape_table_columns = ['gid','geom'];
let gid = shape_table_columns[0];
let geom = shape_table_columns[1];

let out_table = 'schelling';
let out_table_columns = ['t','currentPop'];
let time = out_table_columns[0];
let currentPop = out_table_columns[1];

let neighbors_table = out_table+_NEIGHBORS_TABLE_SUFFIX;
let neighbor_gid = gid+_NEIGHBORS_TABLE_SUFFIX;
let neighbor_distance = 'lineal_distance';

// QUEUE ADMINISTRATION ------------------------------------------------------------------------------------------------
let currentTask = null;
let remainingSteps = 0;
let queue = [
    //genInitialPopulation,
    schelling
];
let timeStamp = Date.now();

function processQueue() {
    if (remainingSteps < 0) {
        console.error('There is something wrong with your queue! remainingSteps='+remainingSteps);
        console.dir(queue);
    }

    if (!currentTask) {
        let currentTime = Date.now();
        console.log(':<-- Last task execution time: '+((currentTime - timeStamp)/1000)+' seconds');
        timeStamp = currentTime;
        console.dir(queue);

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

// SHARED OBJECTS ------------------------------------------------------------------------------------------------------
let blocks = null;
let numBlocks = -1;
function queryBlocks() {
    console.log(':+> queryBlocks()');
    blocks = [];
    getBlocks();

    function getBlocks() {
        console.log(':++> getBlocks()');
        let query = 'SELECT '+gid+' FROM '+out_table+';';
        registerSteps();
        geo.query(query, loadBlocks);
    }

    function loadBlocks(allBlocks) {
        console.log(':++> loadBlocks()');
        for (let block of allBlocks) {
            blocks.push(block[gid]);
        }
        numBlocks = blocks.length;
        processQueue();
    }
}

let neighbors = null;
let numNeighbors = 0;
function queryNeighbors() {
    console.log(':+> queryNeighbors()');
    if (!blocks) {              // --> If the blocks had not been retrieved yet
        queryBlocks();          // |-> Retrieve blocks..
        addTask(queryNeighbors);// |-> and call me again when done...
        return
    }
    getNeighbors();

    function getNeighbors() {
        console.log(':++> getNeighbors()');
        let query = 'SELECT '+gid+','+neighbor_gid+' FROM '+neighbors_table+';';
        registerSteps();
        geo.query(query, loadNeighbors);
    }

    function loadNeighbors(allNeighbors) {
        console.log(':++> loadNeighbors()');
        neighbors = new Map();
        for (let tuple of allNeighbors) {
            let myGid = tuple[gid];
            let neighborGid = tuple[neighbor_gid];
            try {
                neighbors.get(myGid).push(neighborGid);
            } catch (e) {
                neighbors.set(myGid,[neighborGid]);
            }
        }
        processQueue();
    }
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
        //query+= ' LIMIT 10';

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
        geo.query(query, genQueries);
    }

    let queries = [];
    let totalQueries;
    function genQueries() {
        console.log('|-> genQueries()');

        if (!blocks) {          // --> If the blocks had not been retrieved yet
            queryBlocks();      // |-> Retrieve blocks..
            addTask(genQueries);// |-> and call me again when done...
            return
        }

        for (let block of blocks) {
            let query = '';
            query+= 'UPDATE '+out_table+' SET '+currentPop+'='+getRandomPopulation();
            query+= ' WHERE '+gid+'='+block;

            queries.push(query);
        }
        totalQueries = queries.length;

        console.log('|-> setPopulation()');
        let maxWorkers = numBlocks < WORKERS ? numBlocks : WORKERS;
        for (let worker = 0; worker < maxWorkers; worker++) {
            registerSteps();
            setPopulation();
        }
        vacuum();
        processQueue();

        function getRandomPopulation() {
            return Math.floor(Math.random() * (populations + 1));
        }
    }

    function setPopulation() {
        let nextQuery = queries.shift();
        if (nextQuery != undefined) { // --> Recursion termination condition
            registerSteps();
            geo.query(nextQuery, setPopulation);
            process.stdout.write('Progress: '+(1-(queries.length/totalQueries)).toFixed(3)+'\r');
        }
        processQueue();
    }
}

function schelling() {
    console.log('\nschelling()');
    queryInitialState();

    function queryInitialState() {
        console.log('|--> queryInitialState()');
        if (!neighbors) {               // --> If the neighbors had not been retrieved yet
            queryNeighbors();           // |-> Retrieve neighbors...
            addTask(queryInitialState); // |-> and call me again when done...
            return
        }

        let query = 'SELECT '+gid+','+currentPop+' FROM '+out_table+' WHERE '+time+'=0';

        registerSteps();
        geo.query(query, loadInitialState);
    }

    let schellingSim = null;
    let currentIteration = 0;
    function loadInitialState(initialState) {
        console.log('|-> loadInitialState()');
        let currentState = new Map();
        for (let block of initialState) {
            let blockId = block[gid];
            let currentPop = Number.parseInt(block[currentPop]);
            currentState.set(blockId, currentPop);
        }
        schellingSim = new Map();
        schellingSim.set(0,currentState);

        processQueue();
        simulate();
    }

    function simulate() {
        console.log('|-> simulate() :: '+currentIteration);
        let currentState = schellingSim.get(currentIteration);
        let newState = new Map();

        let emptyBlocks = [];
        let movingPopulations = [];

        let blocks = blocks.slice();
        console.log('|--> processBlock()');
        let maxWorkers = numBlocks < WORKERS ? numBlocks : WORKERS;
        for (let worker = 0; worker < maxWorkers; worker++) {
            registerSteps();
            processBlock();
        }

        currentIteration++;
        schellingSim.set(currentIteration, newState);

        currentIteration <= iterations ? addTask(simulate) : addTask(saveResults);

        addTask(movingPop2emptyBlocks);
        processQueue();

        function processBlock() {
            let currentBlock = blocks.shift();
            if (currentBlock == undefined) {        // --> Recursion termination condition
                processQueue();
                return
            }

            let myPopulation = currentState.get(currentBlock);
            if (myPopulation == 0) {                // --> If this is an empty block...
                emptyBlocks.push(currentBlock);     // |-> Add block to available blocks...
            } else {                                // |-> Else
                let myNeighbors = neighbors.get(currentBlock);
                if (amIMoving(myPopulation, myNeighbors)) {
                    movingPopulations.push(myPopulation);
                    emptyBlocks.push(currentBlock);
                } else {
                    newState.set(currentBlock, currentPop);
                }
            }
            process.stdout.write('Progress: '+(1-(blocks.length/numBlocks)).toFixed(3)+'\r');
            processBlock();
        }

        function movingPop2emptyBlocks() {
            registerSteps();
            console.log('|--> movingPop2emptyBlocks()');

            let population = movingPopulations.shift();
            while (population != undefined) {

                let randomBlock = Math.floor(Math.random() * emptyBlocks.length);
                let myNewBlock = emptyBlocks.splice(randomBlock, 1);
                newState.set(myNewBlock, population);

                population = movingPopulations.shift();
            }

            let emptyBlock = emptyBlocks.shift();
            while (emptyBlock != undefined) {
                newState.set(emptyBlock,0);
            }

            processQueue();
        }

        function amIMoving(myPopulation, myNeighbors) {
            let likeMe = 0;
            for (let neighbor of myNeighbors) {
                let neighborPop = currentState.get(neighbor);
                if (myPopulation == neighborPop) likeMe++;
            }

            return (likeMe/myNeighbors.length) <= tolerance;
        }
    }

    function saveResults() {
        console.log('|-> saveResults()');
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

input = rl.question("Tolerance ("+tolerance+' tolerance factor): ');
if (input.length != 0) tolerance = Number.parseFloat(input);
console.log('|-> Tolerance factor set to '+tolerance+'');

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
