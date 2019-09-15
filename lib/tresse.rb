
require 'thread'


module Tresse

  VERSION = '0.1.0'

  class << self

    attr_accessor :max_worker_threads

    def init

      @max_worker_threads = 7
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

    def stop

      return if @status == :stopped

      @status = :stopped
      @thread = nil
      @work_queue << :stop
    end

    def start

      return if @status == :running

      run
    end

    protected

    def run

      @max_work_threads.times { |i| @thread_queue << i }

      @status =
        :running

      @thread =
        Thread.new do
            loop do
              begin

                batch = @work_queue.pop

                if @status == :stopped
                  @work_queue << batch unless batch == :stop
                  break
                end

                i = @thread_queue.pop

                if @status == :stopped
                  @work_queue << batch
                  break
                end

                hand_to_worker_thread(i, batch)

              rescue => err
                @on_error.call(:in_loop, err)
              end
            end
          end
        end
    end

    def hand_to_worker_thread(i, batch)

      Thread.new do |t|
        begin
          t[:tress] = true
          t[:i] = i
          batch.process(i)
          @thread_queue << i unless i >= @max_work_threads
        rescue => err
          @on_error.call(:in_worker_thread, err)
        end
      end
    end
  end
  self.init


  class Batch

    def initialize(group, block_or_group)

      @group = group
      @bog = block_or_group

      @each_index = 0
    end

    def process(i)

      args = [ group, i, nil, nil, nil, nil ][0, @bog.method(:call).arity]

      @bog.call(*args)
    end
  end

  class Group

    attr_accessor :name

    def initialize(name)

      @name = name

      @queue = Queue.new # queueing batches
      @eaches = [ nil ]
    end

    #
    # appending methods

    def append(o=nil, &block)

      Tresse.enqueue(
        Tresse::Batch.new(self, o ? o : block))
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
    end
    alias reduce inject

    def collect(target, &block)
    end
    alias map collect
  end
end

