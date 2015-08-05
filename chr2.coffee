Cot = require "cot"
prettyjson = require "prettyjson"
yop = require "yield-on-promise"
chalk = require "chalk"
_ = require "underscore"
crypto = require "crypto"
querystring = require "querystring"
Q = require "q"

GLOBAL_STAT = false

actions = []

pretty = (d) ->
  console.log(prettyjson.render d)
  d

cotChanges = (db,query) ->
  db.cot.jsonRequest("GET",
    "/#{db.name}/_changes?#{querystring.stringify(query)}")
  .then((response) ->
    if (response.statusCode isnt 200)
      throw new Error("error reading _changes: #{response.unparsedBody}")
    else response.body)

# we may use compile type macrosses by passing the text through uglify

# _.templateSettings.variable = "data"

singleHeadMapTemplate = _.template("""
<% var i, head = rule.head, vars = head.vars;
%>function(doc) {
    var $$ = $1 = doc;
    <% if (rule.variable) { %>
      var <%=rule.variable%> = doc;
    <% } %>
    if (<%= head.guard.join(" && ") %>) {
      <%= rule.mkWith(head) %>
      emit([<% for(i = 0;i < vars.length;++i) {
          %><%=vars[i][1]%><% }%>],doc._id);
    }
}
""", {variable:"rule"})

singleHeadFilterTemplate = _.template("""
<% var i, j, head = rule.head, vars = head.vars;
%>function(doc) {
    var $$ = $1 = doc;
    <% if (rule.variable) { %>
      var <%=rule.variable%> = doc;
    <% } %>
    if (<%= head.guard.join(" && ") %>) {
      return true;
    }
    return false;
}
""", {variable:"rule"})

doubleHeadMapTemplate = _.template("""
  <% var i, j, k, head, shared = rule.shared;
  %>function(doc) {
    var $$ = $1 = doc, $key;
    <% if (rule.variable) { %>
      var <%=rule.variable%> = doc;
    <% } %>
    <% if (rule.PH_IN_VIEW && rule.prpg) { %>
      if (doc.type === "chr$ph" && doc.rule === "<%=rule.name%>") {
        emit(doc.key.concat([0]),null);
        return;
      }
    <% } %>
    <% for(i = 1; i<=2; i++) {
        head = rule["head"+i];
        if (!head)
          throw new Error("no head " + i + "defined");
        %>
          if (<%=head.guard.join(" && ")%>) {
            <%= rule.mkWith(head) %>
             $key = [];
             <% for(j = 0; j < shared.length; ++j) { %>
                $key.push(<%=head.shared[shared[j]]%>);
             <% } %>
             $key.push(<%=head.order%>, <%=i%>);
             <% for(j = 0; j < head.vars.length; ++j) { %>
                $key.push(<%=head.vars[j][1]%>);
             <% } %>
             emit($key,null);
        }
    <% } %>
  }
  """,{variable:"rule"})

doubleHeadFilterTemplate = _.template("""
  <% var i, j, head, shared = rule.shared;
  %>function(doc) {
    var $$ = $1 = doc;
    <% if (rule.variable) { %>
      var <%=rule.variable%> = doc;
    <% } %>
    <% for(i = 1; i<=2; i++) { %>
        <% head = rule["head"+i]; %>
        if (<%=head.guard.join(" && ")%>)
            return true;
    <% }  %>
    return false;
  }
  """,{variable:"rule"})

aggregateMapTemplate = _.template("""
  <% var i, head = rule.head, shared = rule.shared,
         aggregate = rule.aggregate, s;
  %>function(doc) {
    var $$ = $1 = doc, $key;
    <% if (rule.variable) { %>
      var <%=rule.variable%> = doc;
    <% } %>
    if (<%=head.guard.join(" && ")%>) {
      <%= rule.mkWith(head) %>
      $key = [];
      <% for(i = 0; i < shared.length; ++i) { %>
        $key.push(<%=head.shared[shared[i]]%>);
      <% } %>
      $key.push(1);
      $key.push($$._id);
      <% for(i = 0; i < head.vars.length; ++i) { %>
        $key.push(head.vars[i][1]);
      <% } %>
      emit($key,<%=head.init%>);
    } else if (<%=aggregate.guard.join(" && ")%>) {
      $key = []
      <%= rule.mkWith(aggregate) %>
      <% for(i = 0; i < shared.length; ++i) { %>
        <% s = shared[i]; %>
        var <%=s%> = <%= aggregate.shared[s] %>;
        $key.push(<%=s%>);
      <% } %>
      $key.push(2);
      <% for(i = 0; i < aggregate.vars.length; ++i) { %>
        var <%=aggregate.vars[i][0]%> = <%= aggregate.vars[i][1] %>;
      <% } %>
      emit($key,<%=aggregate.init%>);
    }
  }
  """, {variable:"rule"})

aggregateReduceTemplate = _.template("""
  <% var i, head = rule.head, shared = rule.shared,
         aggregate = rule.aggregate;
  %>function(key,<%= rule.result %>) {
    var i;
    <% for(i = 0; i < shared.length; ++i) { %>
      var <%= shared[i] %> = key[<%=i%>];
      return <%= rule.reduce %>;
    <% } %>
  }
  """, {variable:"rule"})

aggregateFilterTemplate = _.template("""
  <% var i, j;
  %>function(doc) {
    var $$ = $1 = doc;
    <% if (rule.variable) { %>
      var <%=rule.variable%> = doc;
    <% } %>
    if (<%=rule.head.guard.join(" && ")%>) return true;
    if (<%=rule.aggregate.guard.join(" && ")%>) return true;
    return false;
  }
  """,{variable:"rule"})

nameId = 0

class Rule
  constructor: (opts) ->
    for i, v of opts
      @[i] = v
    name = @name ?= "rule$#{++nameId}"
    @headName = "head_#{name}"
    @useWith = not @variable?
    @body ?= []
    @computePrpgRule()
  compile: ->
    @compileVars()
    @compileBody()
    @compileGuard()
    @

Rule::mkWith = (head) ->
  return "" unless @useWith and head.type?
  flds = @solver.fields[head.type]
  throw new Error("no such field #{name}") unless flds?
  ("     var #{i} = $$.#{i};" for i of flds).join("\n")

class SingleHead extends Rule
  constructor: (@solver, opts) ->
    super(opts)
    throw new Error("no head defined in #{@name}") unless @head
  compile: ->
    super()
    compileHead @head
    @
  genFilter: ->
    singleHeadFilterTemplate @
  genMap: ->
    singleHeadMapTemplate @
    

class DoubleHead extends Rule
  PH_IN_VIEW: false
  constructor: (@solver, opts) ->
    super(opts)
    throw new Error("no head 1 defined in #{@name}") unless @head1
    throw new Error("no head 2 defined in #{@name}") unless @head2
  compile: ->
    super()
    {head1, head2} = @
    compileHead head1
    compileHead head2
    @
  genFilter: ->
    doubleHeadFilterTemplate @
  genMap: ->
    doubleHeadMapTemplate @

compileHead = (head) ->
    g = head.guard ?= []
    head.vars ?= []
    head.order ?= "0"
    g.push("$$.type==='#{head.type}'") if head.type?

class Aggregate extends Rule
  constructor: (@solver, opts) ->
    super(opts)
    throw new Error("no head defined in #{@name}") unless @head
    throw new Error("no aggregate head defined in #{@name}") unless @aggregate
  compile: ->
    super()
    compileHead @head
    compileHead @aggregate
  genMap: ->
    aggregateMapTemplate @
  genReduce: ->
    aggregateReduceTemplate @
  genFilter: ->
    aggregateFilterTemplate @

prepareRule = (solver,rule) ->
  if rule.head1?
    if checkPrpgRule rule
      return new DoubleHeadPrpg(solver,rule)
    return new DoubleHead(solver,rule)
  else if rule.aggregate? then return new Aggregate(solver,rule)
  else return new SingleHead(solver,rule)

compileRules = (solver) ->
  byName = solver.byName = {}
  {rules} = solver
  for i,x in rules
   r = prepareRule(solver,i)
   r.compile()
   rules[x] = cur = r
   byName[cur.name] = cur
  solver

gcMap = (doc) ->
  emit [doc._id,0] #if doc.chr$track
  if doc.chr$ref?
    emit [i, 1] for i in doc.chr$ref
  return

messagesMap = (doc) ->
  emit doc.id, doc if doc.type is "chr$msg"

compileViews = (solver) -> 
  views = solver._views =
    all: 
      map: """function (doc) { if (doc.type) emit(doc.type,doc); }"""
    gc:
      map: gcMap.toString()
  filters = solver._filters = {}
  for r in solver.rules
    nm = "head_#{r.name}"
    v = views[nm] =
      map: r.genMap()
    v.reduce = r.genReduce() if r.genReduce?      
    filters[nm] = r.genFilter()
  solver

compileConstraints = (solver) ->
  res = solver.keys = {}
  flds = solver.fields = {}
  for {name,keys,fields} in solver.constraints
    if keys?
      keys = (i for i of fields) if keys is "all"
      res[name] = keys
    flds[name] = fields
  return

compile = (solver) ->
  if solver._compiled
    return
  solver._compiled = true
  solver.prefix ?= "chr$ph$#{solver.name}"
  solver.needsCleanup = []
  compileConstraints solver
  compileRules solver
  compileLists solver
  compileViews solver
  return

class Region
  constructor: (@store,@key) ->
    @locked = []
    @objects = {}
    @bulk = []
  acquire: (id) ->
    doc = @objects[id]
    if doc?
      return null if doc._deleted
      return doc
    {db} = @store
    try
      doc = yop db.get id
    catch e
      return null
    return null if doc.chr$lock? and doc.chr$lock isnt @key
    doc.chr$lock = @key
    try
      res = yop db.post doc
      doc._id = res.id
      doc._rev = res.rev
    catch e
      return null
    @locked.push doc
    @objects[id] = doc
    if doc.chr$ref
      for i in doc.chr$ref
        return null unless @acquire(i)
    return doc
  unlock: (d) ->
    return if d._deleted
    {db} = @store
    return unless d.chr$lock
    try
      doc = yop db.get d._id
      delete d.chr$lock
      delete doc.chr$lock
      yop db.post doc
    catch e
      console.log chalk.red("couldn't unlock"), doc, e
  exit: ->
    {bulk,STAT} = @
    console.time "#{STAT} exit" if STAT?
    for i in @locked when not i._deleted
      delete i.chr$lock
      bulk.push i
    res = yop @store.db.bulk bulk
    @locked.length = 0
    bulk.length = true
    console.timeEnd "#{STAT} exit" if STAT?
    return  
  post: (doc) ->
    @bulk.push doc
  remove: (doc) ->
    doc._deleted = true
    @bulk.push doc


class Store
  constructor: (@db, @solver) ->
    compile(solver)
    @seq = {}
  view: (name, opts) ->
    p = {update_seq: true}
    if opts?
      p[i] = v for i, v of opts
    res = yop @db.view @solver.name, name, p
    @seq[name] = res.update_seq
    res
  inRegion: (key, f) ->
    reg = new Region(@,key)
    try
      return f(reg)
    finally
      reg.exit()
  # waits for view from its last query
  waitView: (name) ->
    last = @seq[name]
    loop 
      r = yop cotChanges(@db,{
        limit:1
        since:@seq[name]
        filter:"#{@solver.name}/#{name}"
        feed:"longpoll"})
      if r.results.length
        return
  migrate: -> migrate @db, @solver
  getRule: (id) ->
    res = if id.substr?
      @solver.byName[id]
    else
      @solver.rules[id]
    unless res?
      console.log chalk.red("no such rule"), chalk.yellow(id)
      console.log "Available:"
      for i of @solver.byName
        console.log " * #{i}"
      throw new Error("no such rule #{id}") 
    res
  loopRule: (id) ->
    {headName} = r = @getRule id
    loop
      r.commit @
      @waitView headName
    return
  loopRuleQ: (id) -> yop.frun => @loopRule(id)
  commit: (id) ->
    @getRule(id).commit(@)
  post: (obj) ->
    unless obj._id
      keys = @solver.keys[obj.type]
      if keys?
        id = mkHash(JSON.stringify(obj[i] for i in keys))
        try
          nobj = yop @db.get id
          neq = false
          for i of @solver.fields[obj.type] when not keys[i]?
            unless obj[i] is nobj[i]
              neq = true
              break
          unless neq
            obj._rev = nobj._rev
            return false
          nobj[i] = v for i, v of obj
          res = yop @db.post nobj
          obj._rev = res.rev
          return true
        catch
          obj._id = id
    res = yop @db.post obj
    obj._rev = res.rev
    return true
  remove: (doc) ->
    delete doc.chr$lock
    return if doc._deleted
    doc._deleted = true
    try
      yop @db.delete doc._id, doc._rev
    catch e
      console.log chalk.red("couldn't delete"), doc, e
    @
  gcIter: ->
    t = @view "gc", {include_docs:true}
    {db} = @
    bulk = []
    for {key:[ref,code],id,doc} in t.rows
      switch code
        when 0 then skip = ref
        when 1
          try
            unless ref is skip
              doc._deleted = true
              bulk.push doc
    r = yop db.bulk bulk
    effect = bulk.length
    console.log "gc", chalk.green("DONE!"), effect
    return
  gcLoop: ->
    yop.frun(=>@gcIter()).then(-> Q.delay(1000)).then(=>@gcLoop())
  gcLoopQ: -> yop.frun => @gcLoop()
  run: ->
    {update_seq} = yop @db.info()
    @startSeq = update_seq 
    t = yop @db.post {type:"chr$proc",pid: process.pid,name:@name}
    id = @_id = t.id
    return unless id?
    loop
      change = yop cotChanges(@db,{
          since:update_seq
          feed: "longpoll"
          doc_ids: JSON.stringify [id]})
      update_seq = change.last_seq
      return for i in change.results when i.deleted
  runQ: -> yop.frun => @run()

compileLists = (solver) ->
  list = solver._list = {}
  for {name,fields} in solver.constraints
    cur = list[name] = (j for j of fields)
  return

curListTempl = _.template("""
function(head, res) {
  var i, v, args, row, types = <%=JSON.stringify(solver._list)%>;
  start({headers: {'Content-Type': 'text/html'}});
  send("<html><body><ul>");
  while (row = getRow()) {
    type = row.key;
    if (!type)
      continue;
    t = types[type];
    if (!t)
      continue;
    v = row.value;
    args = [];
    for(i = 0; i < t.length; ++i) {
      f = v[t[i]];
      args.push(f == null ? "null" : f);
    }
    send("<li>" + type + "(" + args.join() + ")</li>");
   }
   send("</ul></body></html>");
}""",{variable:"solver"})

migrate = (db, solver) ->
  _id = "_design/#{solver.name}"
  design = try
    yop db.get _id
  catch
    {_id}
  design.language = "javascript"
  design.views = solver._views
  design.filters = solver._filters
  design.lists =
    compact: curListTempl(solver)
  yop db.post(design)
  console.log "migration", chalk.green("DONE!"), "at", design._rev ? "initial"

checkPrpgRule = (rule) ->
  val = rule.prpg
  unless val?
    val = true
    for i in rule.body when i.remove?
      val = false
      break
  return rule.prpg = val

Rule::computePrpgRule = ->

SingleHead::computePrpgRule = ->
    val = @prpg
    unless val?
      val = true
      for i in @body when i.remove?
        val = false
        break
    @prpg = val 
    if val
      @setIsPrpgRule()

SingleHead::setIsPrpgRule = ->
  g = @head.guard ?= []
  @prpgkey = key = "#{@solver.prefix}$#{@name}"
  g.push "!$$.#{key}"

DoubleHead::setIsPrpgRule = ->
  @prgpkey = key = "#{@solver.prefix}$#{@name}"
  for i in [1..2]
    g = @["head#{i}"].guard ?= []
  return

Aggregate::computePrpgRule = ->
  
locked = (doc,tid) ->
  return false if doc.chr$lock is tid
  return false unless doc.chr$locl

SingleHead::commit = (store) ->
    t = store.view @headName, {include_docs: true}
    tid = @getTid()
    {prpgkey} = @
    {db} = store
    effect = 0
    store.inRegion tid, (reg) =>
      rows = reg.lockAll t.rows
      for {key,id} in rows
        doc = reg.acquire id
        continue unless doc?
        try
          if prpgkey?
            doc[prpgkey] = true
            r = yop db.post doc
            doc._rev = r.rev
        catch e
          console.error chalk.red("commit #{@name}"), e
          continue
        args = [doc].concat(key)
        for i in @actions
          ++effect if i.apply(reg,args)
      console.log "commit #{@name}", chalk.green("DONE!"), effect
    return effect is 0

mkHash = (val) ->
  crypto.createHash("sha1").update(JSON.stringify(val)).digest("hex")

getHistId = (args...) ->
    res = (i._id for i in args)
    mkHash(res.join '-')

threadCnt = 0

Rule::getTid = ->
  mkHash "#{process.pid}.#{@name}.#{++threadCnt}"

Region::lockAll = (rows) ->
  locked = {}
  res = []
  bulk = []
  {key,locked,objects} = @
  for i in rows
    {id} = i
    doc = objects[id]
    if doc?
      i.doc = doc
      res.push i
      continue
    doc = i.doc
    if doc.chr$lock?
      if doc.chr$lock is key
        objects[id] = doc
        res.push i
      else
        doc.chr$skip = true
      continue
    objects[id] = doc
    doc.chr$lock = key
    res.push i
    bulk.push doc
  console.log chalk.yellow("STAT:"), @STAT, "lock bulk", bulk.length if @STAT?
  r = yop @store.db.bulk bulk
  for i, x in r
    doc = bulk[x]
    if i.rev?
      doc._rev = i.rev
      locked.push doc
    else
      doc.chr$skip = true
  return res

DoubleHead::commit = (store) ->
  STAT = @STAT ? GLOBAL_STAT
  {shared,actions,_guard,name} = @
  if STAT
    stat = (tag, val) =>
      console.log chalk.yellow("STAT:"), "#{name}: #{tag}:", val   
  console.time "#{name} request" if STAT
  console.time "#{name}" if STAT
  t = store.view @headName, {include_docs: true}
  console.timeEnd "#{name} request" if STAT
  tid = @getTid() 
  first = []
  {db} = store
  effect = 0
  sharedLen = shared.length
  ph = {}
  store.inRegion tid, (reg) =>
    reg.STAT = name if STAT
    console.time "#{name} lock"
    rows = reg.lockAll(t.rows)
    console.timeEnd "#{name} lock"
    statNotMatched = 0
    statMatched = 0
    for i in rows
      doc = i.doc
      continue if doc.chr$skip or doc._deleted
      vars = i.key
      sharedVars = vars[...sharedLen]
      pos = vars[sharedLen+1]
      others = vars[sharedLen+2..]
      if sharedVars > cur
        cur = sharedVars
        first.length = 0
        ph = {}
      cur = sharedVars
      switch pos
        when 1 then first.push [i.id,others,doc]
        when 2
          id2 = i.id
          for [id1,args1,doc1] in first
            continue if id1 is id2 or doc1._deleted
            doc2 = doc
            args = [doc1,doc2].concat(sharedVars,args1,others)
            if _guard? and not _guard.apply(store,args)
              ++statNotMatched if STAT
              continue
            ++statMatched if STAT
            for j in actions
              ++effect if j.apply(reg,args)
    if STAT
      stat "loaded", t.rows.length
      stat "locked rows", rows.length
      stat "bulk", reg.bulk.length
      stat "not matched", statNotMatched
      stat "matched", statMatched
      stat "locked objs", reg.locked.length
      stat "effect", effect 
  console.timeEnd "#{name}" if STAT
  console.log "commit #{name}:", chalk.green("DONE!"), effect 
  return effect is 0

class DoubleHeadPrpg extends DoubleHead
  constructor: (store, rule) -> super(store, rule)
  prpg: true

DoubleHeadPrpg::commit = (store) ->
  STAT = @STAT ? GLOBAL_STAT
  {shared,actions,_guard,name} = @
  if STAT
    stat = (tag, val...) =>
      console.log chalk.yellow("STAT:"), "#{name}: #{tag}:", val...   
  console.time "#{name}" if STAT
  console.time "#{name} request" if STAT
  t = store.view @headName, {include_docs: true}
  console.timeEnd "#{name} request" if STAT
  tid = @getTid() 
  first = []
  {db} = store
  effect = 0
  sharedLen = shared.length
  ph = {}
  store.inRegion tid, (reg) =>
    reg.STAT = name if STAT
    console.time "#{name} lock" if STAT
    rows = reg.lockAll(t.rows)
    console.timeEnd "#{name} lock" if STAT
    prods = []
    statNotMatched = 0
    statMatched = 0
    statPhKeys = 0
    statPhMatched = 0
    console.time "#{name} prod" if STAT
    for i in rows
      doc = i.doc
      continue if doc.chr$skip or doc._deleted
      vars = i.key
      sharedVars = vars[...sharedLen]
      order = vars[sharedLen]
      pos = vars[sharedLen+1]
      others = vars[sharedLen+2..]
      if sharedVars > cur
        cur = sharedVars
        first.length = 0
        ph = {}
      cur = sharedVars
      switch pos
        when 0
          ++statPhKeys if STAT
          ph[i.id] = true
        when 1 then first.push [i.id,others,doc,order]
        when 2
          id2 = i.id
          for [id1,args1,doc1,order] in first
            continue if id1 is id2 or doc1._deleted
            doc2 = doc
            histKey = getHistId doc1, doc2
            if ph[histKey]
              ++statPhMatched if STAT
              continue
            args = [doc1,doc2].concat(sharedVars,args1,others)
            if _guard? and not _guard.apply(store,args)
              ++statNotMatched
              continue
            ++statMatched
            prods.push [args, histKey, sharedVars.concat([order])]
    console.timeEnd "#{name} prod" if STAT
    phbulk = []
    console.time("#{name} ph") if STAT
    for [i,histKey,key] in prods
      [doc1, doc2] = i
      phbulk.push {
        type:"chr$ph"
        _id: histKey
        rule: name
        key
        chr$ref: [doc1._id, doc2._id]}
    r = yop db.bulk phbulk
    console.timeEnd("#{name} ph") if STAT
    console.time("#{name} body") if STAT
    statPH = 0
    for i,x in r when i.rev?
      ++statPH if STAT
      [args] = prods[x]
      for j in actions
        ++effect if j.apply(reg,args)
    console.timeEnd("#{name} body") if STAT
    if STAT
      stat "loaded", t.rows.length
      stat "locked rows", rows.length
      stat "prods", prods.length
      stat "bulk", reg.bulk.length
      stat "not matched", statNotMatched
      stat "matched", statMatched
      stat "locked objs", reg.locked.length
      stat "PH: matched:", statPhMatched
      stat "PH: keys:", statPhKeys
      stat "PH: tot:", r.length
      stat "PH: ok:", statPH
      stat "PH: conflict:", r.length - statPH
      stat "effect", effect 
  console.timeEnd("#{name}") if STAT
  console.log "commit #{name}:", chalk.green("DONE!"), effect 
  return effect is 0
  
Aggregate::commit = (store) ->
  t = store.view @headName, {group:true}
  tid = @getTid() 
  first = []
  {shared,actions,_guard} = @
  {db} = store
  effect = 0
  sharedLen = shared.length
  for i in t.rows
    vars = i.key
    val = i.value
    sharedVars = vars[...sharedLen]
    pos = vars[sharedLen]
    if sharedVars > cur
      cur = sharedVars
      first.length = 0
    switch pos
      when 1
        others = vars[sharedLen+1..]
        first.push [others]
      when 2
        for [args1] in first
          args = [val].concat(sharedVars,args1)
          if _guard? and not _guard.apply(store,args)
            continue
          for i in actions
            ++effect if i.apply(store,args)
  console.log "commit #{@name}:", chalk.green("DONE!"), effect 
  return effect is 0

SingleHead::compileVars = ->
  res = @vars = ["c$1"]
  vars = @head.vars ?= []
  for [i] in @head.vars
    res.push i
  @

DoubleHead::compileVars = ->
  res = @vars = ["c$1","c$2"]
  res.push(@shared...)
  vars1 = @head1.vars ?= []
  vars2 = @head2.vars ?= []
  for [i] in vars1
    res.push i
  for [i] in vars2
    res.push i
  @

Aggregate::compileVars = ->
  res = @vars = [@result]
  res.push(@shared...)
  res.push "c$1"
  vars = @head.vars ?= []
  for [i] in vars
    res.push i
  @

Rule::compileGuard = ->
  {guard} = @
  return unless guard?
  g = guard.join "&&"
  args = @vars.concat ["return #{guard};"]
  @_guard = new Function(args...)

Rule::compileBody = ->
  res = @actions = []
  for i in @body
    for j,v of i
      act = actions[j]
      throw new Error("unknown action #{j}") unless act?
      res.push(act @, v)
  @

actions.remove = (rule, v) ->
  args = rule.vars.concat ["this.remove(#{v}); return true;"]
  new Function(args...)

actions.post = (rule, opts) ->
  body = ["var obj = #{JSON.stringify(opts.obj)};"]
  for n,v of opts.fields
    body.push "obj.#{n} = #{v};"
  body.push "return this.post(obj);"
  args = rule.vars.concat [body.join "\n"]
  new Function(args...)

Rule::commitQ = (n) -> yop.frun => @commit n

module.exports = (db, solver) ->
  new Store(db, solver)


