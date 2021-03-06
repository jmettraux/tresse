
#
# spec'ing tresse
#
# Sun Sep 15 16:57:55 JST 2019
#

require 'spec_helper'


describe Tresse::Group do

  describe '#source' do

    it 'queues its result' do

      r =
        Tresse::Group.new('test0')
          .source { (0..3).to_a }
          .source { (4..9).to_a }
          .values
          .sort

      expect(r).to eq((0..9).to_a)
    end
  end

  describe '#flatten' do

    it 'returns the values in a single array' do

      r =
        Tresse::Group.new('test0')
          .source { (0..4).to_a }
          .source { ('a'..'c').to_a }
          .flatten

      expect(r.size).to eq(8)
      [ 0, 1, 2, 3, 4, 'a', 'b', 'c' ].each { |e| expect(r).to include(e) }
      i = r.index(3); expect(r[i + 1]).to eq(4)
      i = r.index('a'); expect(r[i + 1]).to eq('b')
    end

    it 'does not mind non-arrays' do

      r =
        Tresse::Group.new('test0')
          .source { (0..4).to_a }
          .source { { a: 0, b: 1 } }
          .source { :nada }
          .source { (0..3).each_with_index }
          .flatten

      expect(r.size).to eq(11)

      [ *(0..4), { a: 0, b: 1 }, :nada, *(0..3).each_with_index ]
        .each { |e| expect(r).to include(e) }
    end

    it 'fails if there are no sources' do

      expect {
        Tresse::Group.new
          .flatten
      }.to raise_error(RuntimeError, 'no sources defined')
    end
  end

  describe '#map' do

    it 'replaces each batch value with its result' do

      r =
        Tresse::Group.new('test0')
          .source { (0..3).to_a }
          .source { ('a'..'c').to_a }
          .map { |e| e.collect { |e| e * 2 } }
          .values

      expect(r.size).to eq(7)
      [ 0, 2, 4, 6, 'aa', 'bb', 'cc' ].each { |e| expect(r).to include(e) }
      i = r.index(0); expect(r[i + 1]).to eq(2)
      i = r.index('aa'); expect(r[i + 1]).to eq('bb')
    end

    it 'fails if there are no sources' do

      expect {
        Tresse::Group.new
          .map { |e| e }
      }.to raise_error(RuntimeError, 'no sources defined')
    end
  end

  describe '#each' do

    it 'processes each batch but does not replace their values' do

      r =
        Tresse::Group.new('test0')
          .source { (0..3).to_a }
          .source { ('a'..'c').to_a }
          .each { |e| e.collect { |e| e * 2 } }
          .values

      [ 0, 1, 2, 3, 'a', 'b', 'c' ].each { |e| expect(r).to include(e) }
      i = r.index(0); expect(r[i + 1]).to eq(1)
      i = r.index('a'); expect(r[i + 1]).to eq('b')
    end

    it 'fails if there are no sources' do

      expect {
        Tresse::Group.new
          .each { |e| }
      }.to raise_error(RuntimeError, 'no sources defined')
    end
  end

  describe '#inject' do

    it 'injects' do

      r =
        Tresse::Group.new('test0')
          .source { [ 'a' ] }
          .source { [ 'c' ] }
          .source { [ 'b' ] }
          .each { |e| e[0] = e[0] * 2; 'X' }
          .inject([]) { |a, e| a << e.first; a.sort }

      expect(r).to eq(%w[ aa bb cc ])
    end

    it 'fails if there are no sources' do

      expect {
        Tresse::Group.new
          .inject([]) { |a, e| a }
      }.to raise_error(RuntimeError, 'no sources defined')
    end
  end

  describe '.max_work_thread_count' do

    it 'returns 8 by default' do

      expect(Tresse.max_work_thread_count).to eq(8)
    end
  end

  describe '.max_work_thread_count=' do

    it 'sets the max' do

      Tresse.max_work_thread_count = 6

      expect(Tresse.max_work_thread_count).to eq(6)
    end

    it 'is respected' do

      Tresse.max_work_thread_count = 1

      expect(Tresse.max_work_thread_count).to eq(1)
      expect(Tresse.class_eval { @work_threads.size }).to eq(1)

      g = lambda {
        t = []
        Tresse::Group.new
          .source { (0..3).to_a }
          .source { (3..6).to_a }
          .each { |e| t << :a0; sleep 0.001; t << :z0 }
          .each { |e| t << :a1; sleep 0.001; t << :z1 }
          .flatten
        t }

      1_000.times do
        t = g.call.collect(&:to_s)
        expect(t).to eq(%w[ a0 z0 a0 z0 a1 z1 a1 z1 ])
      end

      Tresse.max_work_thread_count = 8
    end
  end

  describe '#source_each' do

    it 'queues its result (Array)' do

      r =
        Tresse::Group.new('test0')
          .source_each([ 2, 4 ]) { |i| (0..i).to_a }
          .values
          .sort

      expect(r).to eq([ 0, 0, 1, 1, 2, 2, 3, 4 ])
    end

    it 'queues its result (Enumerator)' do

      ids = (0..99).to_a

      r =
        Tresse::Group.new('test0')
          .source_each(ids.each_slice(25)) { |is| [ is[0], is[-1] ]  }
          .values
          #.sort

      expect(r).to eq([ 0, 24, 25, 49, 50, 74, 75, 99 ])
    end

    it 'queues its result (Hash)' do

      h = { a: %w[ a b c ], d: %w[ d e f ] }

      r =
        Tresse::Group.new('test0')
          .source_each(h) { |_, v| v }
          .values
          #.sort

      expect(r).to eq(%w[ a b c d e f ])
    end

    it 'lets mapping fail if there are no sources' do

      expect {
        Tresse::Group.new
          .source_each([]) { |e| e }
          .each { |e| }
      }.to raise_error(RuntimeError, 'no sources defined')
    end

    it 'lets reduction fail if there are no sources' do

      expect {
        Tresse::Group.new
          .source_each([]) { |e| e }
          .values
      }.to raise_error(RuntimeError, 'no sources defined')
    end
  end

  context 'errors' do

    they 'are catched and reraised at reduction level' do

      expect {
        Tresse::Group.new('test0')
          .source { [ 1, 2 ] }
          .source { [ 3, 4 ] }
          .map { |a| fail 'too bad!' }
          .values
      }.to raise_error(
        RuntimeError, 'too bad!'
      )
    end
  end
end

