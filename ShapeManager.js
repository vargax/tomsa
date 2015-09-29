import GeotabulaDB from 'geotabuladb'

let _geo;
export default class ShapeManager {
    constructor() {
        _geo = new GeotabulaDB();
    }

    init() {
        _geo.setCredentials({
            user: 'tomsa',
            password: 'tomsa',
            database: 'tomsa'
        });
    }
}