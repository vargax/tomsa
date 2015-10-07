// IMPORTS -------------------------------------------------------------------------------------------------------------
import GeotabulaDB from 'geotabuladb'
import * as SQLhelper from './ODmatrix/SQLhelper.js'

// CONSTANTS -----------------------------------------------------------------------------------------------------------
const ODtableName = 'tabulatr_OD_pop_OandD';
const radius = 1000; // In meters
const timeout = 130; // In milisecs

// SCRIPT --------------------------------------------------------------------------------------------------------------
let geo = new GeotabulaDB();
geo.setCredentials({
    user: 'tomsa',
    password: 'tomsa',
    database: 'tomsa'
});
let rowsCount = 0;
dropTable();

// FUNCTIONS -----------------------------------------------------------------------------------------------------------
function dropTable() {
    console.log('dropTable()');
    geo.query(SQLhelper.QueryBuilder.dropTable(ODtableName), createTable);
}

function createTable() {
    console.log('createTable()');
    let columns = [
        ['id', SQLhelper.PK],
        ['spO_gid', SQLhelper.INT], ['spD_gid', SQLhelper.INT],
        ['time', SQLhelper.TIMESTAMP]
    ];
    let query = SQLhelper.QueryBuilder.createTable(ODtableName, columns);
    geo.query(query, getAllBlocks);
}

function getAllBlocks() {
    console.log('getAllBlocks()');
    let queryParams = {
        tableName: 'manzanas',
        properties: ['gid','geom'],
	where: 'pob > 0'
        //limit: 100
    };
    geo.query(queryParams, lookForCloseBlocks);
}

const hashToBlockId = new Map();
function lookForCloseBlocks(allBlocks) {
    const totalBlocks = allBlocks.length;
    console.log('lookForCloseBlocks()');
    console.log(' Looking for blocks at '+radius+' meters of a total of '+totalBlocks+' blocks...');

    let spObjAtRadiusQueries = [];
    for (let block of allBlocks) {
        let queryParams = {
            properties: ['gid'],
            tableName: 'manzanas',
            geometry: 'geom',
            spObj: block.geom,
            radius: radius,
            where: 'pob > 0'
        };
        spObjAtRadiusQueries.push([block, queryParams]);
    }
    recursiveSetTimeOut();


    function recursiveSetTimeOut() {
        console.log(' '+(1 - (spObjAtRadiusQueries.length/totalBlocks)).toFixed(4)+' complete ('+spObjAtRadiusQueries.length+' of '+totalBlocks+' queries remaining) EXPECTED ROWS: '+rowsCount);
        let spObjAtRadiusQuery = spObjAtRadiusQueries.pop();
        if (spObjAtRadiusQuery != undefined) {
            setTimeout(function() {
                let hash = geo.spatialObjectsAtRadius(spObjAtRadiusQuery[1], saveNearBlocks);
                hashToBlockId.set(hash, spObjAtRadiusQuery[0].gid);

                recursiveSetTimeOut();
            }, timeout);
        }
    }
}

const hashToInsertId = new Map();
function saveNearBlocks(nearBlocks, spObjAtRadiusHash) {
    let spO_gid = hashToBlockId.get(spObjAtRadiusHash);
    hashToBlockId.delete(spObjAtRadiusHash);
    //console.log('  saveNearBlocks() for block '+spO_gid);

    let columns = ['spO_gid','spD_gid'];
    let values = [];
    for (let feature of nearBlocks.features) {
        let spD_gid = feature.properties.gid;
        values.push([spO_gid, spD_gid]);
    }

    rowsCount += values.length;
    let query = SQLhelper.QueryBuilder.insertInto(ODtableName,columns,values);
    let insertHash = geo.query(query, end);
    hashToInsertId.set(insertHash, spO_gid);
}

function end(noResult, hash) {
    //console.log('  Done insert for '+hashToInsertId.get(hash));
    hashToInsertId.delete(hash);
}
