
# tresse

A poorly thought out and stupid each+map+reduce contraption.


## use

```
require 'tresse'

r =
  Tresse::Group.new('test0')
    .append { 'b' }
    .append { 'a' }
    .collect { |e| e * 2 }

r
  # => %[ aa bb ]
  # or
  # => %[ bb aa ]
```

```ruby
require 'tresse'

r =
  Tresse::Group.new('test0')
    .append { [ 'a' ] }
    .append { [ 'c' ] }
    .append { [ 'b' ] }
    .each { |e| e[0] = e[0] * 2 }
    .inject([]) { |a, e| a << e.first; a.sort }

r
  # => %w[ aa bb cc ]
```

## license

MIT, see [LICENSE.txt](LICENSE.txt)

