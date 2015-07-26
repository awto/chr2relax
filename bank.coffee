chr = require "./chr2"

Cot = require "cot"
prettyjson = require "prettyjson"
yop = require "yield-on-promise"
chalk = require "chalk"
_ = require "underscore"
Q = require "q"

bank = {
  name: "bank"
  constraints: [
    {name: "client", fields: {id:"int"}}
    {name: "account", fields: {client:"int", id: "int", balance: "float"}}
    {name: "platinum", fields: {client:"int"}}
    {name: "deposit", fields: {account:"int",amount:"float"}}
    {name: "withdraw", fields: {account:"int",amount:"float"}}
    ]
  rules: [{
    name: "calc_platinum",
    shared: ["C"]
    head:
      guard: ["type === 'client'"]
      shared: {C: "id"}
      init: "0"
    aggregate:
      guard: ["type === 'account'"]
      shared: {C:"client"}
      vars: [["B","balance"]]
      init: "B"
    result: "Sum"
    reduce: "sum(Sum)"
    guard: ["Sum > 2500"]
    body: [{
      post:
        obj:
          type: "platinum"
        fields:
          client: "C"
    }]}
  {
    name: "platinum_set"
    shared: ["C"]
    head1:
      guard: ["type === 'platinum'"]
      shared: {C: "client"}
    head2:
      guard: ["type === 'platinum'"]
      shared: {C: "client"}
    body: [remove:"c$2"]}
  ]}


db = null
store = null

getDb = ->
    return db if db?
    db = (new Cot {hostname: "localhost", port: 5984}).db("bank")

getStore = ->
  return store if store
  db = getDb()
  store = chr db, bank

initial = [
  {type:"client",id:1}
  {type:"account",id:1,client:1,balance:1000}
  {type:"account",id:2,client:1,balance:900}
  {type:"account",id:2,client:1,balance:700}
  {type:"client",id:2}
  {type:"account",id:3,client:2,balance:1000}
  ]

reset = ->
  db = getDb()
  d = yop db.allDocs({include_docs:true})
  for {doc} in d.rows
    if doc.type?
      try
        yop db.delete(doc._id,doc._rev)
  for i in initial
    yop db.post i
  return

migrate = ->
  getStore().migrate()

gc = -> getStore().gc()

calc_platinum = ->
  getStore().commit("calc_platinum")
platinum_set = ->
  getStore().commit("platinum_set")

commands = {
  migrate
  reset
  calc_platinum
  platinum_set
  gc }

do ->
  cmds = process.argv[2..]
  yop.frun(->
    for i in cmds
      cmd = commands[i]
      unless cmd?
        console.error chalk.red("unknown command"), cmd  
      cmd()).done()
  return

