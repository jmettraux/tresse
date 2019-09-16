
require 'thread'


module Tresse

  VERSION = '1.1.0'

  class << self

    def init

      @work_queue = Queue.new
      @work_threads = 8.times.collect { |i| make_work_thread }

      @on_error =
        lambda do |where, err|
          puts "-" * 80
          p where
          p err
          puts err.backtrace
          puts "-" * 80
        end
    end

    def enqueue(batch)

      @work_queue << batch

      batch.group
    end

    def on_error(&block)

      @on_error = block
    end

    def max_work_thread_count

      @work_threads.size
    end

    def max_work_thread_count=(i)

      i0 = @work_threads.size

      @work_threads << make_work_thread while @work_threads.size < i
      @work_threads.pop while @work_threads.size > i

      i
    end

    protected

    def make_work_thread

      Thread.new do

        t = Thread.current
        t[:tresse] = true

        loop do
          begin

            batch = @work_queue.pop

            unless @work_threads.include?(t)
              @work_queue << batch
              break
            end

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

    def map(type, block)

      args = [ @value, self ]
      args = args[0, block.method(:call).arity.abs]

      r = block.call(*args)

      @value = r if type == :map
    end
  end

  class Group

    attr_accessor :name
    #attr_reader :batches

    def initialize(name=nil)

      @name = name

      @batches = []
      @launched = false
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

      self
    end

    #
    # mapping

    def each(&block)

      do_map(:each, block)
    end

    def map(&block)

      do_map(:map, block)
    end

    #
    # reducing

    def reduce(target, &block)

      do_reduce(target, block)
    end
    alias inject reduce

    def flatten

      do_reduce([], lambda { |a, e| a.concat(e) })
    end
    alias values flatten

    protected

    def do_map(type, block)

      @maps << [ type, block ]

      launch

      self
    end

    def do_reduce(target, block)

      @reduce = [ target, block ]

      launch

      @reduction_queue.pop
    end

    def launch

      return if @launched == true
      @launched = true

      @batches.each { |b| Tresse.enqueue(b) }
    end

    def receive(batch)

      if batch.map_index == 0
        batch.source
        Tresse.enqueue(batch)
      elsif m = @maps[batch.map_index]
        batch.map(*m)
        Tresse.enqueue(batch)
      else
        queue_for_reduction(batch)
      end
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

