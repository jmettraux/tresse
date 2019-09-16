
require 'thread'


module Tresse

  VERSION = '1.0.0'

  class << self

    attr_accessor :max_work_thread_count

    def init

      @max_work_thread_count = 7
      @work_queue = Queue.new

      @on_error =
        lambda do |where, err|
          puts "-" * 80
          p where
          p err
          puts err.backtrace
          puts "-" * 80
        end

      @work_threads =
        @max_work_thread_count.times
          .collect { |i| make_work_thread(i) }
    end

    def enqueue(batch)

      @work_queue << batch

      batch.group
    end

    def on_error(&block)

      @on_error = block
    end

    protected

    def make_work_thread(i)

      Thread.new do

        Thread.current[:tresse] = true
        Thread.current[:i] = i

        loop do
          begin

            batch = @work_queue.pop

            batch.process

          rescue => err

            @on_error.call(:in_worker_thread, err)
          end
        end
      end
    end
  end
    #
  self.init


  class Batch

    attr_reader :group
    attr_reader :map_index
    attr_accessor :value

    def initialize(group, block_or_group)

      @group = group
      @bog = block_or_group

      @map_index = -1
      @value = nil
    end

    def process

      @map_index += 1
      @group.send(:receive, self)
    end

    def source

      args = [ group ] + [ nil ] * 7
      args = args[0, @bog.method(:call).arity]

      @value = @bog.call(*args)
    end
  end

  class Group

    attr_accessor :name
    attr_reader :batches

    def initialize(name=nil)

      @name = name

      @batches = []
      @maps = [ nil ]

      @reduce = nil
      @reduce_batches = []
      @reduction_queue = Queue.new
    end

    #
    # sourcing methods

    def source(o=nil, &block)

      batch = Tresse::Batch.new(self, o ? o : block)

      @batches << batch
      Tresse.enqueue(batch)
    end

    #
    # mapping

    def each(&block)

      @maps << [ :each, block ]

      self
    end

    def map(&block)

      @maps << [ :map, block ]

      self
    end

    #
    # reducing

    def reduce(target, &block)

      @reduce = [ target, block ]

      @reduction_queue.pop
    end
    alias inject reduce

    def flatten

      @reduce = [ [], lambda { |a, e| a.concat(e) } ]

      @reduction_queue.pop
    end
    alias values flatten

    protected

    def receive(batch)

      if batch.map_index == 0
        batch.source
        Tresse.enqueue(batch)
      elsif m = @maps[batch.map_index]
        do_map(batch, *m)
        Tresse.enqueue(batch)
      else
        queue_for_reduction(batch)
      end
    end

    def do_map(batch, type, block)

      args = [ batch.value, batch ]
      args = args[0, block.method(:call).arity.abs]
      r = block.call(*args)

      batch.value = r if type == :map
    end

    def queue_for_reduction(batch)

      @reduce_batches << batch

      return if @reduce_batches.size < @batches.size
      return unless @reduce

      es = @batches.collect(&:value)
      target, block = @reduce

      @reduction_queue << es.inject(target, &block)
    end
  end
end

