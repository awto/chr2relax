chr2relax
=========

This system is a next step from chr2sql system towards bigger data handling. It 
is more a proof of concept implementation in progress. It uses couchdb because 
of its simple materialized views, which correspond to alpha and beta memories 
in RETE algorithm. At the moment it doesn’t conform to abstract operational 
semantics unless solver services are run in some specific order. Now it works 
only with single head and double head rules. And it is better if in double head 
rules the heads have at least 1 shared variable. It also supports aggregation 
from CHRv2.


