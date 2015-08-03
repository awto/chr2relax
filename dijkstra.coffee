chr = require "./chr2"

Cot = require "cot"
prettyjson = require "prettyjson"
yop = require "yield-on-promise"
chalk = require "chalk"
_ = require "underscore"
Q = require "q"

dijkstraCompiled = {
  name: "dijkstra"
  constraints: [
    {name: "source", fields: {id:"int"}}
    {name: "dist", fields: {to:"int", weight: "float"}}
    {name: "edge", fields: {from:"int",to:"int",weight:"float"}}
    ]
  rules: [
    {
      name: "init"
      variable: "_"
      head:
        vars: [["C","_.id"]]
        guard: ["_.type==='source'"]
      body: [{post:
        obj:
          type:"dist"
          weight: 0
        fields:
          to: "C"
        }]
      },
    {
      name: "keep_shortest"
      variable: "_"
      STAT: true
      shared: ["V"]
      head1:
        guard: ["_.type==='dist'"]
        shared: {V: "_.to"}
        vars: [["D1","_.weight"]]
        order: "_.weight"
      head2:
        guard: ["_.type==='dist'"]
        shared: {V: "_.to"}
        vars: [["D2","_.weight"]]
        order: "_.weight"
      guard: ["D1 < D2"]
      body: [remove:"c$2"]
      },
    {
      name: "label"
      STAT: true
      variable: "_"
      #PH_IN_VIEW: true
      shared: ["V"]
      head1:
        guard: ["_.type==='dist'"]
        shared: {V:"_.to"}
        vars: [["D","_.weight"]]
      head2:
        guard: ["_.type==='edge'"]
        shared: {V:"_.from"}
        vars: [["U","_.to"],["C","_.weight"]]
      body: [{
        post:
          obj:
            type:"dist"
          fields:
            to: "U"
            weight: "C+D"
          }]
      }
    ]
  }

db = null
store = null

getDb = ->
    return db if db?
    db = (new Cot {hostname: "localhost", port: 5984}).db("graph")


getStore = ->
  return store if store
  db = getDb()
  store = chr db, dijkstraCompiled

migrateCmd = ->
  getStore().migrate()

commit_init = ->
  getStore().commit("init")
commit_keep_shortest = ->
  getStore().commit("keep_shortest")
commit_label = ->
  getStore().commit("label")

resetHard = ->
  db = getDb()
  yop db.cot.jsonRequest "DELETE", "/#{db.name}" 
  yop db.cot.jsonRequest "PUT", "/#{db.name}"
  migrateCmd()
  yop db.post {type:"edge",from:1,weight:1,to:2}
  yop db.post {type:"edge",from:1,weight:10,to:3}
  yop db.post {type:"edge",from:2,weight:1,to:4}
  yop db.post {type:"edge",from:3,weight:9,to:4}
  yop db.post {type:"edge",from:4,weight:2,to:1}
  yop db.post {type:"source",id:1}

gc = ->
  getStore().gcIter()

commands = {
  migrate: migrateCmd
  #step
  keep_shortest: commit_keep_shortest
  label: commit_label
  init: commit_init
  reset: resetHard
  gc
  }

do ->
  cmds = process.argv[2..]
  yop.frun(->
    for i in cmds
      cmd = commands[i]
      unless cmd?
        console.error chalk.red("unknown command"), i
      cmd()).done()
  return


