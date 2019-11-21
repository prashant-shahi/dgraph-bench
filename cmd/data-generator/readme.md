## Generate the files

```bash
#CSV
sh ./createCSV10mi.sh
sh ./createCSV200mi.sh

#RDF
sh ./createRDF10mi.sh
sh ./createRDF200mi.sh
```

## Bulkload

```bash
./dgraph bulk -f ./out.rdf.gz -s ./Empty.schema
./dgraph bulk -f ./out.rdf.gz -s ./full_lv1.schema
./dgraph bulk -f ./out.rdf.gz -s ./full_lv2.schema
./dgraph bulk -f ./out.rdf.gz -s ./full_lv3.schema
```

## Bulkload Neo4J

```bash
./neo4j-admin import --nodes='/Users/${YOURUSER}/Library/Application Support/Neo4j Desktop Canary/Application/neo4jDatabases/database-00000000-0000-0000-0000-0000000000000/installation-3.5.12/import/out.csv'
```

```bash
./neo4j-admin import --nodes='path to your dataset => out.csv'
```
