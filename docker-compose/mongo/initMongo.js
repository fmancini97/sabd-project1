const ASCENDING = 1
const DESCENDING = -1
let dbName = "sabd"

db = db.getSiblingDB("admin")

db.createUser({
    user: "sabd",
    pwd: "sabd",
    roles: [
        {role: "readWrite",
            db: dbName
        }
    ]
})
db = db.getSiblingDB(dbName)

db.createCollection("query1_result")
db.query1_result.createIndex({Data: ASCENDING, Regione: ASCENDING})
db.query1_result.createIndex({Regione: ASCENDING, Data: ASCENDING})


db.createCollection("query2_result")
db.query2_result.createIndex({Data: ASCENDING, "Fascia anagrafica": ASCENDING, "Regione": ASCENDING})

db.createCollection("query3_result")
db.query3_result.createIndex({Algoritmo: ASCENDING, K: ASCENDING, "Regione": ASCENDING})

db.createCollection("query3_benchmark")
db.query3_benchmark.createIndex({Algoritmo: ASCENDING, K: ASCENDING})


