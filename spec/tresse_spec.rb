
#
# spec'ing tresse
#
# Sun Sep 15 16:57:55 JST 2019
#

require 'spec_helper'


describe Tresse::Group do

  it 'works' do

    trace = []

    Tresse::Group.new('test0')
      .append { (0..3).to_a }
      .append { ('a'..'c').to_a }
      .each { |e| trace << e }

    sleep 0.350

    expect(trace).to eq([ [ 0, 1, 2, 3 ], %w[ a b c ] ])
  end
end

