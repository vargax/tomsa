// IMPORTS -------------------------------------------------------------------------------------------------------------
import GeotabulaDB from 'geotabuladb'
import * as geoHelper from 'geotabuladb'

// CONSTANTS -----------------------------------------------------------------------------------------------------------
const _NEIGHBORS_TABLE_SUFFIX = '_neighbors';
const DATABASE_WORKERS = 10;

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
let out_table_columns = ['t','currentpop'];     // --> column names ALLAYS in lowercase!
let time = out_table_columns[0];
let currentPop = out_table_columns[1];

let neighbors_table = out_table+_NEIGHBORS_TABLE_SUFFIX;
let neighbor_gid = gid+_NEIGHBORS_TABLE_SUFFIX;
let neighbor_distance = 'lineal_distance';

// QUEUE ADMINISTRATION ------------------------------------------------------------------------------------------------
let currentTask = null;
let remainingSteps = 0;
let queue = [
    genInitialPopulation,
    schelling,
    driver
];
let timeStamp = Date.now();

function processQueue() {
    //console.log(': <-- processQueue()');
    if (remainingSteps < 0) {
        console.error('! <-- There is something wrong with your queue! remainingSteps='+remainingSteps);
        console.dir(queue);
    }

    if (!currentTask) {
        let currentTime = Date.now();
        console.log(': <-- Last task execution time: '+((currentTime - timeStamp)/1000)+' seconds');
        timeStamp = currentTime;
        //console.dir(queue);

        currentTask = queue.shift();
        try {
            currentTask();
        } catch (e) {
            console.log(remainingSteps+' remaining steps :: '+queue.length+' queue => DONE!');
            if (remainingSteps != 0 || queue.length != 0) {
                console.dir(queue);
                console.dir(e);
            }
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
    console.log(': <-- AddTask');
    if (Array.isArray(nextTask)) {
        let task = nextTask.pop();
        while (task != undefined) {
            queue.unshift(task);
            task = nextTask.pop();
        }
    } else {
        queue.unshift(nextTask);
    }
    //console.dir(queue);
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
    console.log(':-> queryBlocks()');
    blocks = [];
    getBlocks();

    function getBlocks() {
        console.log(':--> getBlocks()');
        let query = 'SELECT '+gid+' FROM '+out_table+';';
        registerSteps();
        geo.query(query, loadBlocks);
    }

    function loadBlocks(allBlocks) {
        console.log(':--> loadBlocks()');

        for (let block of allBlocks) blocks.push(block[gid]);

        numBlocks = blocks.length;

        console.log('  => '+numBlocks+' blocks loaded!');
        processQueue();
    }
}

let neighbors = null;
function queryNeighbors() {
    console.log(':-> queryNeighbors()');
    if (!blocks) {              // --> If the blocks had not been retrieved yet
        addTask(queryNeighbors);// |-> Call me again when you
        queryBlocks();          // |-> retrieve blocks..
        return
    }

    getNeighbors();

    function getNeighbors() {
        console.log(':--> getNeighbors()');
        let query = 'SELECT '+gid+','+neighbor_gid+' FROM '+neighbors_table+';';
        registerSteps();
        geo.query(query, loadNeighbors);
    }

    function loadNeighbors(allNeighbors) {
        console.log(':--> loadNeighbors()');
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
        console.log('  => '+neighbors.size+' neighbors loaded!');
        processQueue();
    }
}

// MAIN FUNCTIONS ------------------------------------------------------------------------------------------------------
function calculateNeighbors() {
    console.log('\ncalculateNeighbors()');
    registerSteps();
    addTask([clean, createTables, runQuery, done]);
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

        scheduleVACUUM();
    }

    function runQuery() {
        console.log('|-> runQuery()');

        let query;

        query = 'INSERT INTO '+neighbors_table;
        query+= ' SELECT '+shape_table+'.'+gid+',neighbor.'+gid;
        query+= ' ,ST_Distance(neighbor.'+geom+'::geography,'+shape_table+'.'+geom+'::geography)';
        query+= ' FROM '+shape_table+','+shape_table+' neighbor';
        query+= ' WHERE ST_DWithin(neighbor.'+geom+'::geography,'+shape_table+'.'+geom+'::geography,'+radius+')';
        //query+= ' LIMIT 10';

        registerSteps();
        geo.query(query, processQueue);
    }

    function done() {
        registerSteps();
        console.log('|--> DONE neighbors calculation!');
        scheduleVACUUM();
        processQueue();
    }
}

function genInitialPopulation() {
    console.log('\ngenInitialPopulation()');
    registerSteps();
    addTask([clean, setPopulations, done]);
    processQueue();

    function clean() {
        console.log('|-> clean()');
        let query = 'DELETE FROM '+out_table+' WHERE '+time+'<> 0;';
        query += 'UPDATE '+out_table+' SET '+currentPop+'=-1;';

        registerSteps();
        geo.query(query, processQueue);
    }

    function setPopulations() {
        console.log('|-> setPopulations()');

        if (!blocks) {                  // --> If the blocks had not been retrieved yet
            addTask(setPopulations);    // |-> Call me again when you
            queryBlocks();              // |-> retrieve blocks..
            return
        }

        let query='';
        let population = populations;
        let limit = Math.round(numBlocks/(populations+1));
        let remaining = numBlocks;
        while (population != 0) {
            query+= 'UPDATE '+out_table+' SET '+currentPop+'='+population;
            query+= ' FROM ('
                +' SELECT '+gid+' FROM '+out_table
                +' WHERE '+currentPop+'=-1 AND '+gid+'>=random()*'+remaining
                +' LIMIT '+limit
                +')AS target';
            query+= ' WHERE '+out_table+'.'+gid+'=target.'+gid+';';

            remaining -= limit;
            population--;
        }
        query+='UPDATE '+out_table+' SET '+currentPop+'=0 WHERE '+currentPop+'= -1;';

        registerSteps();
        geo.query(query, processQueue);
    }

    function done() {
        registerSteps();
        console.log('|--> DONE Initial population generation!');
        scheduleVACUUM();
        processQueue();
    }
}

function schelling() {
    console.log('\nschelling()');

    let schellingIterations = null;
    let currentIteration = 0;
    let hash2inserts = new Map();

    registerSteps();
    addTask([prepare, simulate, done]);
    processQueue();

    function prepare() {
        console.log('|-> prepare()');
        queryInitialState();

        function queryInitialState() {
            console.log('|--> queryInitialState()');
            if (!neighbors) {               // --> If the neighbors had not been retrieved yet
                addTask(queryInitialState); // |-> call me again when done the
                queryNeighbors();           // |-> neighbors retrieval
                return
            }

            let query = 'SELECT '+gid+','+currentPop+' FROM '+out_table+' WHERE '+time+'=0';

            registerSteps();
            geo.query(query, loadInitialState);
        }

        function loadInitialState(queryResult) {
            console.log('|--> loadInitialState()');
            let initialState = new Map();
            let withoutNeighbors = [];

            for (let block of queryResult) {
                let blockId = block[gid];
                let blockPop = block[currentPop];

                let blockNeighbors = neighbors.get(blockId);
                if (blockNeighbors == undefined) {
                    withoutNeighbors.push(blockId);
                    continue;
                }

                initialState.set(blockId, blockPop);
            }

            if (withoutNeighbors.length != 0) {
                console.log('  WAR :: '+withoutNeighbors.length+' blocks without neighbors!');
                console.dir(withoutNeighbors);
            }

            schellingIterations = [];
            schellingIterations.push(initialState);

            processQueue();
        }
    }

    function simulate() {
        let lastState = schellingIterations[currentIteration];

        currentIteration++;

        console.log('|-> simulate() :: '+currentIteration+' iteration ('+(iterations-currentIteration)+' remaining)');
        let nextState = new Map(); // --> The KEY is the block gid, the VALUE is the currentPop in that block.
        let emptyBlocks = [];
        let movingPopulations = [];

        if (currentIteration < iterations) addTask(simulate);
        addTask(movingPop2emptyBlocks);

        schellingIterations.push(nextState);

        registerSteps();
        processBlock();

        function processBlock() {
            console.log('|--> processBlock() '+lastState.size+' blocks');

            for (let tuple of lastState) {
                let myGid = tuple[0];
                let myPopulation = tuple[1];

                if (myPopulation == 0) {
                    emptyBlocks.push(myGid);
                    continue;
                }

                let myNeighbors = neighbors.get(myGid);
                if (amIMoving(myPopulation, myNeighbors)) {
                    movingPopulations.push(myPopulation);
                    emptyBlocks.push(myGid);
                    continue;
                }

                nextState.set(myGid, myPopulation);
            }

            console.log('  => Empty '+emptyBlocks.length+' :: Moving '+movingPopulations.length+' :: Stay '+nextState.size);
            processQueue();

            function amIMoving(myPopulation, myNeighbors) {
                let likeMe = 0;
                for (let neighbor of myNeighbors) {
                    let neighborPop = lastState.get(neighbor);
                    if (myPopulation == neighborPop) likeMe++;
                }

                return (likeMe/myNeighbors.length) <= tolerance;
            }
        }

        function movingPop2emptyBlocks() {
            registerSteps();
            console.log('|--> movingPop2emptyBlocks()');

            let population = movingPopulations.pop();
            while (population != undefined) {
                let randomBlock = Math.floor(Math.random() * emptyBlocks.length);
                let myNewBlock = (emptyBlocks.splice(randomBlock, 1))[0];
                nextState.set(myNewBlock, population);
                population = movingPopulations.pop();
            }

            let emptyBlock = emptyBlocks.pop();
            while (emptyBlock != undefined) {
                nextState.set(emptyBlock,0);
                emptyBlock = emptyBlocks.pop();
            }

            addTask(saveResults);
            processQueue();
        }

        function saveResults() {
            console.log('|--> saveResults()');

            let columns = [time,gid,currentPop];
            let values = [];
            for (let [myGid, myPopulation] of nextState)
                values.push([currentIteration, myGid, myPopulation]);

            let query = geoHelper.QueryBuilder.insertInto(out_table,columns,values);

            registerSteps();
            hash2inserts.set(geo.query(query, queryCallback), currentIteration);

            function queryCallback(noResult, hash) {
                console.log('|---> Insert for iteration '+hash2inserts.get(hash)+' DONE!');
                processQueue();
            }
        }
    }

    function done() {
        registerSteps();
        console.log('|--> DONE Schelling simulation!');
        scheduleVACUUM();
        processQueue();
    }
}

function driver() {
    console.log('|-> driver()');

    let query = '';
    query+= 'UPDATE '+out_table+' SET '+geom+' = subquery.'+geom;
    query+= ' FROM (SELECT '+gid+','+geom+' FROM '+shape_table+') as subquery';
    query+= ' WHERE '+out_table+'.'+gid+'= subquery.'+gid+';';

    registerSteps();
    geo.query(query, queryCallback);

    function queryCallback(result, hash) {
        console.log('|--> Geometry update DONE in '+out_table);
        scheduleVACUUM();
        processQueue();
    }
}

// SUPPORT FUNCTIONS ---------------------------------------------------------------------------------------------------
function scheduleVACUUM() {
    addTask(vacuum);

    function vacuum() {
        console.log('|-> VACUUM');
        registerSteps();
        geo.query('VACUUM', processQueue);
    }
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

console.log('\n|-----------------------------------------------------------');
console.log("| TOMSA :: iter1 :: Euclidean distance-based Schelling Model");
console.log('|-----------------------------------------------------------\n');

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
