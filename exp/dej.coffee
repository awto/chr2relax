designId = "dijkstra"
Cot = require "cot"
prettyjson = require "prettyjson"
yop = require "yield-on-promise"
chalk = require "chalk"

db = (new Cot {hostname: "localhost", port: 5984}).db("graph")

pretty = (d) ->
  console.log(prettyjson.render d)

migrate = ->
  _id = "_design/#{designId}"
  design = try
    yop db.get _id
  catch
    {_id}
  design.views = vs = {}
  design.language = "javascript"
  for i, v of views
    vs[i] = o = {}
    o[j] = f.toString() for j, f of v
  design.lists = ls = {}
  for i, v of lists
    ls[i] = v.toString()
  yop db.post(design)
  console.log "migration", chalk.green("DONE!"), "at", design._rev

head = (name) ->
    yop db.view designId, name, include_docs: true

#TODO: should be some other file to avoid capturing something from context
views =   
  head_keep_alive:
    map: (doc) ->
      {type} = doc
      if type is "dist" # TODO: ad hoc condition may be set here (order)
        emit [doc.to,1], doc._id
        emit [doc.to,2], doc._id
          # V:doc.to
  head_init:
    map: (doc) ->
      if doc.type is "source" and not doc.$ph$init
        #TODO: all additional conditions are handled here
        emit [doc.value], doc._id
  head_label:
    map: (doc) ->
      {type} = doc
      if type is "dist"
        emit [doc.to,1], doc._id
      else if type is "edge"
        emit [doc.from,2], doc._id
  all:
    map: (doc) ->
      return unless doc.type?
      emit doc.type, doc


lists =
  cur: (head,req) ->
    types =
      dist: ["to","weight"]
      source: ["value"]
      edge: ["from","to","weight"]
    start
      headers:
        'Content-Type': 'text/html'
    send "<html><body><ul>"
    while (row = getRow())
      type = row.key
      continue unless type?
      t = types[type]
      continue unless t?
      v = row.value
      args = for i in t
        f = v[i]
        if f? then f.toString() else "null"
      send("<li>#{type}(#{args.join()})</li>")
    send "</ul></body></html>"
    

commit_init = ->
  console.log(chalk.blue "init...")
  t = head "head_init"
  bulk = []
  pretty t
  for {key,id} in t.rows
    try
      doc = yop db.get id
      console.log "try",doc._id,doc.$ph$init
      continue if doc.$ph$init
      doc.$ph$init = true
      yop db.post doc
    catch e
      console.error chalk.red("commit_init"), e
      continue
    do ->
      V = doc.value
      bulk.push
        type: "dist"
        to: V
        weight: 0
  yop db.bulk bulk
  console.log chalk.green("commit_init"), bulk.length
  return bulk.length is 0

undef = {}

commit_keepshortest = ->
  console.log(chalk.blue "keepshortest...")
  t = head "head_keep_alive"
  cur = undef
  hist = {}
  first = []
  {rows} = t
  bulk = []
  effect = 0
  for i in rows when not i._skip
    [v,t] = i.key
    if cur isnt v
      cur = v
      first.length = 0
    switch t
      when 1 then first.push i.id
      when 2
        s = i.id
        for f in first
          # for same constraint type
          continue if s is f
          try
            cf = yop db.get f
            cs = yop db.get s
          catch e
            #console.error chalk.red("commit_keepshortest"), e
            continue
          do ->
            V = cs.to
            D1 = cf.weight
            D2 = cs.weight
            # TODO: optimize this as order in view
            return unless D1 <= D2
            try
              #TODO: still doesn't work, something may delete cf and cs
              yop db.delete(s,cs._rev)
              effect--
            catch e
              console.error chalk.red("commit_keepshortest"), e
  console.log chalk.green("commit_keepshortest"),
    if effect is 0 then "0" else chalk.red(effect)
  return effect is 0

commit_label = ->
  console.log(chalk.blue "label...")
  t = head "head_label"
  cur = undef
  hist = {}
  first = []
  {rows} = t
  bulk = []
  for i in rows
    {key,id} = i
    v = key[...-1].join()
    t = key[key.length-1]
    if cur isnt v
      cur = v
      hist = {}
      first.length = 0
    switch t
      when 1 then first.push id
      when 2
        s = id
        for f in first
          histkey = "label:#{f}~#{s}"
          try
            hist = yop db.post({_id: histkey, type: "$ph", rule: "label"})
          catch e
            # console.error "hist token", e
            continue
          try
            cf = yop db.get f
            cs = yop db.get s
          catch e
            console.error chalk.red("commit_label"), e
            try
              yop db.delete histkey, hist._rev
            continue
          do ->
            V = cf.to
            D = cf.weight
            C = cs.weight
            U = cs.to
            bulk.push
              type: "dist"
              to: U
              weight: D+C
    # TODO: batches mean bulkin earlier? if it is atomic of course
  yop db.bulk bulk
  console.log chalk.green("commit_label"), bulk.length
  return bulk.length is 0

reset = ->
  d = yop db.allDocs({include_docs:true})
  #pretty d
  for {doc} in d.rows
    if doc.type is "dist" or doc.type is "$ph" 
      yop db.delete(doc._id,doc._rev)
    else
      if doc.$ph$init
        delete doc.$ph$init
        yop db.post(doc)
        console.log "reset$ph", doc._id
  console.log "reset", chalk.green("DONE!")

stepHlp = ->
  init_done = commit_init()
  keepshortest_done = commit_keepshortest()
  label_done =commit_label()
  done = init_done and keepshortest_done and label_done
  if done
    console.log chalk.green("DONE")
  else  
    console.log chalk.blue("partial")
  return done

step = ->
  migrate()
  stepHlp()

solve = ->
  migrate()
  loop
    break if stepHlp()

commands =
  migrate: ->
    yop.frun(migrate).done()
  step: ->
    yop.frun(step).done()
  keep_shortest: ->
    yop.frun(commit_keepshortest).done()
  label: ->
    yop.frun(commit_label).done()
  init: ->
    yop.frun(commit_init).done()
  reset: ->
    yop.frun(reset).done()
  main: ->
    yop.frun(main).done()
  solve: ->
    yop.frun(solve).done()

do ->
  cmds = process.argv[2..]
  for i in cmds
    cmd = commands[i]
    unless cmd?
      console.error chalk.red("unknown command"), cmdName  
    cmd()
  return
