# Now

- Allow prop for all and iter
- Instead of get_named_values, return an iterator over columns

- Support sqlite "in" cluase with json_each
- Support iter:clone()/recompile for

- Throw error when iter invoked twice on same prepareted statement, or consider
  calling clone

- Abstract database interface to work with luasql dbs (refer to the
  santoku.web.sqlite compatibility layer)

- Helper for automatically computing column names for inserts and updates
