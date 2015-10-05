import GeotabulaDB from 'geotabuladb'
import * as SQLhelper from './SQLhelper.js'

let ODtableName = 'tabulatr_OD';
let _geo;
let _map;

export default class ShapeManager {
    constructor() {
        _geo = new GeotabulaDB();
        _map = new Map();
    }

    init() {
        _geo.setCredentials({
            user: 'tomsa',
            password: 'tomsa',
            database: 'tomsa'
        });

        let counter;
        let done = 0;
        let queries = [];

        function dropTable() {
            _geo.query(SQLhelper.QueryBuilder.dropTable(ODtableName), createTable);
        }

        function createTable() {
            let columns = [
                ['id', SQLhelper.PK],
                ['spO_gid', SQLhelper.INT], ['spD_gid', SQLhelper.INT],
                ['time', SQLhelper.TIMESTAMP]
            ];
            let query = SQLhelper.QueryBuilder.createTable(ODtableName, columns);
            _geo.query(query, lookForNearBlocks);
        }

        function lookForNearBlocks() {

        }

        function saveNearBlocks() {

        }

        _geo.query(SQLhelper.QueryBuilder.dropTable(ODtableName), function() {
            let columns = [
                ['id', SQLhelper.PK],
                ['spO_gid', SQLhelper.INT], ['spD_gid', SQLhelper.INT],
                ['time', SQLhelper.TIMESTAMP]
            ];

            let query = SQLhelper.QueryBuilder.createTable(ODtableName, columns);
            _geo.query(query, function() {
                let queryParams = {
                    tableName: 'manzanas',
                    properties: ['gid','geom'],
                    limit: 100
                };
                _geo.query(queryParams, function(results) {
                    counter = results.length;

                    for (let result of results) {
                        let queryParams = {
                            properties: ['gid'],
                            tableName: 'manzanas',
                            geometry: 'geom',
                            spObj: result.geom,
                            radius: 20
                        };
                        
                        let hash = _geo.spatialObjectsAtRadius(queryParams, function(results, hash) {
                            let spO_gid = _map.get(hash);
                            _map.delete(hash);
                            console.log('-- Remaining queries: '+counter--);
                            let columns = ['spO_gid','spD_gid'];
                            let values = [];
                            for (let feature of results.features) {
                                let spD_gid = feature.properties.gid;
                                values.push([spO_gid, spD_gid]);
                            }

                            let query = SQLhelper.QueryBuilder.insertInto(ODtableName,columns,values);
                            queries.push(query);
                        });
                        _map.set(hash, result.gid);
                    }
                });
            });
        });
        return queries;
    }
}

