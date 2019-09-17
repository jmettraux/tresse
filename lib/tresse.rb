
require 'thread'


module Tresse

  VERSION = '1.1.4'

  class << self

    def init

      @work_queue = Queue.new
      @work_threads = 8.times.collect { |i| make_work_thread }
    end

    def enqueue(batch)

      @work_queue << batch

      batch.group
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

            batch.error = err
          end
        end
      end
    end
  end
    #
  self.init


  def self.call_block(block, args)

    block.call(*args[0, block.arity.abs])
  end


  class Batch

    attr_reader :group
    attr_reader :map_index
    attr_reader :completed
    attr_accessor :value
    attr_reader :error

    def initialize(group, block_or_group)

      @group = group
      @bog = block_or_group

      @map_index = -1
      @value = nil
      @completed = false
    end

    def process

      @map_index += 1
      @group.send(:receive, self)
    end

    def source

      @value = Tresse.call_block(@bog, [ group ] + [ nil ] * 7)
    end

    def map(type, block)

      r = Tresse.call_block(block, [ @value, self ])

      @value = r if type == :map
    end

    def complete

      @completed = true
    end

    def error=(err)

      @error = err
      @group.send(:receive, self)
    end
  end

  class Group

    attr_accessor :name

    def initialize(name=nil)

      @name = name

      @batches = []
      @launched = false
      @maps = [ nil ]

      @reduce = nil
      @reduce_mutex = Mutex.new
      @reduction_queue = Queue.new
    end

    #
    # sourcing methods

    def source(&block)

      @batches << Tresse::Batch.new(self, block)

      self
    end

    def source_each(collection, &block)

      if collection.is_a?(Hash)
        collection.each { |k, v|
          source { Tresse.call_block(block, [ k, v ]) } }
      else
        collection.each_with_index { |e, i|
          source { Tresse.call_block(block, [ e, i ]) } }
      end

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

      do_reduce(
        [],
        lambda { |a, e|
          if e.respond_to?(:to_a) && ! e.is_a?(Hash)
            a.concat(e.to_a)
          else
            a.push(e)
          end })
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

      r = @reduction_queue.pop

      raise r.error if r.is_a?(Tresse::Batch)

      r
    end

    def launch

      return if @launched == true
      @launched = true

      @batches.each { |b| Tresse.enqueue(b) }
    end

    def receive(batch)

      if batch.error
        @reduction_queue << batch
      elsif batch.map_index == 0
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

      @reduce_mutex.synchronize do

        batch.complete

        return unless @reduce
        return if @batches.find { |b| ! b.completed }

        es = @batches.collect(&:value)
        target, block = @reduce

        @reduction_queue << es.inject(target, &block)
      end
    end
  end
end

