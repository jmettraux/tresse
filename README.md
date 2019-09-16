
# tresse

A poorly thought out and stupid source+map+reduce contraption.

You source one or more pieces of data, map them a couple of times then reduce them.

By default, the whole of Tresse uses 8 work threads, it can be changed:
```ruby
Tresse.max_work_thread_count # => 8

Tresse.max_work_thread_count = 10

Tresse.max_work_thread_count # => 10
```

## use

Two sources flattened together
```ruby
r =
  Tresse::Group.new('test0')
    .source { (0..3).to_a }
    .source { (4..9).to_a }
    .values # or .flatten
    .sort

r #=> (0..9).to_a
```

Combining two sources again
```ruby
r =
  Tresse::Group.new('test0')
    .source { (0..3).to_a }
    .source { ('a'..'c').to_a }
    .map { |e| e.collect { |e| e * 2 } }
    .values # or .flatten

r
  # => [ 0, 2, 4, 6, 'aa', 'bb', 'cc' ]
  #    or
  # => [ 'aa', 'bb', 'cc', 0, 2, 4, 6 ]
```

Each can be used, the outcome of its block is discarded
```ruby
t = []
  # collecting on the side

r =
  Tresse::Group.new('test0')
    .source { (0..3).to_a }
    .source { ('a'..'c').to_a }
    .each { |e| t << e.collect { |e| e * 2 } }
    .values

r
  # => [ 0, 1, 2, 3, 'a', 'b', 'c' ]
  #    or
  # => [ 'a', 'b', 'c', 0, 1, 2, 3 ]

t
  # => [ [ 0, 2, 4, 6 ], [ 'aa', 'bb', 'cc' ] ]
  #    or
  # => [ [ 'aa', 'bb', 'cc' ], [ 0, 2, 4, 6 ] ]
```


## license

MIT, see [LICENSE.txt](LICENSE.txt)

