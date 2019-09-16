
require 'thread'


module Tresse

  VERSION = '1.0.0'

  class << self

    attr_accessor :max_work_threads

    def init

      @max_work_threads = 7
      @work_queue = Queue.new
      @thread_queue = Queue.new

      @on_error = lambda do |where, err|
        puts "-" * 80
        p where
        p err
        puts err.backtrace
        puts "-" * 80
      end

      run
    end

    def enqueue(batch)

      @work_queue << batch

      batch.group
    end

    def on_error(&block)

      @on_error = block
    end

    protected

    def run

      @max_work_threads.times { |i| @thread_queue << i }

      Thread.new do
        loop do
          begin

            i = @thread_queue.pop
            batch = @work_queue.pop

            hand_to_worker_thread(i, batch)

          rescue => err

            @on_error.call(:in_loop, err)
          end
        end
      end
    end

    def hand_to_worker_thread(i, batch)

      Thread.new do
        begin

          Thread.current[:tress] = true
          Thread.current[:i] = i

          batch.process

          @thread_queue << i unless i >= @max_work_threads

        rescue => err

          @on_error.call(:in_worker_thread, err)
        end
      end
    end
  end
    #
  self.init


  class Batch

    attr_reader :group
    attr_reader :each_index, :value

    def initialize(group, block_or_group)

      @group = group
      @bog = block_or_group

      @each_index = -1
      @value = nil
    end

    def process

      @each_index += 1
      @group.send(:hand, self)
    end

    protected

    def generate

      args = [ group ] + [ nil ] * 7
      args = args[0, @bog.method(:call).arity]

      @value = @bog.call(*args)
    end
  end

  class Group

    attr_accessor :name
    attr_reader :batches

    def initialize(name)

      @name = name

      @batches = []
      @eaches = [ nil ]

      @final = nil
      @final_batches = []
      @final_queue = Queue.new
    end

    #
    # appending methods

    def append(o=nil, &block)

      batch = Tresse::Batch.new(self, o ? o : block)

      @batches << batch
      Tresse.enqueue(batch)
    end

    #
    # step methods

    def each(&block)

      @eaches << block

      self
    end

    #
    # final methods

    def inject(target, &block)

      @final = [ target, block ]

      @final_queue.pop
    end
    alias reduce inject

    def collect(&block)

      @final = block

      @final_queue.pop
    end
    alias map collect

    protected

    def hand(batch)

      if batch.each_index == 0
        batch.send(:generate)
        Tresse.enqueue(batch)
      elsif e = @eaches[batch.each_index]
        args = [ batch.value, batch ]
        args = args[0, e.method(:call).arity.abs]
        e.call(*args)
        Tresse.enqueue(batch)
      else
        queue_for_final(batch)
      end
    end

    def queue_for_final(batch)

      @final_batches << batch

      return if @final_batches.size < @batches.size

      es = @batches.collect(&:value)

      @final_queue <<
        if @final.is_a?(Array)
          es.inject(@final[0], &@final[1])
        else
          es.collect(&@final)
        end
    end
  end
end

