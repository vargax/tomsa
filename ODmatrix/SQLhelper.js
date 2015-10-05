export const PK = ' SERIAL PRIMARY KEY';
export const STRING = ' TEXT';
export const INT = ' INT';
export const FLOAT = ' FLOAT';
export const TIMESTAMP = ' TIMESTAMP';

export class QueryBuilder {

    static dropTable(tableName) {
        return 'DROP TABLE IF EXISTS '+tableName+';'
    }

    static createTable(tableName, columns) {
        let query = 'CREATE TABLE '+tableName+'(';
        for (let column of columns) {
            query += column[0]+' '+column[1]+','
        }
        query = query.slice(0,-1)+');';

        //console.log(query);
        return query;
    }

    static insertInto(tableName, columns, values) {
        let query = 'INSERT INTO '+tableName+'(';
        for (let column of columns) {
            query += column+',';
        }
        query = query.slice(0,-1)+') VALUES ';

        for (let value of values) {
            query += '(';
            for (let item of value) {
                query += item+',';
            }
            query = query.slice(0,-1)+'),';
        }
        query = query.slice(0,-1)+';';

        //console.log(query);
        return query;
    }
}

export function logCallback(result, hash) {
    console.log(hash);
    console.dir(result);
}