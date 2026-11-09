# Wildlife Observations. Run: trmnlp test (from plugin/)
RSpec.describe 'Wildlife Observations' do
  let(:mocks) { JSON.parse(File.read(File.join(__dir__, 'mocks.json'))).transform_values { |v| v.transform_keys(&:to_sym) } }
  let(:custom_fields) { {} }
  let(:views) { %w[full half_horizontal half_vertical quadrant] }

  # one observation in the backend's response format
  def raw(overrides = {})
    { id: 1, observed_on: '2014-02-12', location_name: 'Hilvarenbeek, NL-NB, NL',
      taxon_common_name: 'Eurasian Eagle-Owl', taxon_name: 'Bubo bubo', iconic_taxon_name: 'Aves',
      photo_url: 'https://inaturalist-open-data.s3.amazonaws.com/photos/1840610/large.jpg',
      photo_license: 'cc-by-nc', observer_name: 'Paul Cools' }.merge(overrides)
  end

  # items: what the (mocked) backend answers; nil keeps a single default observation
  def draw(view, items: nil, answer: nil, device: 'og_png', fields: {})
    answer ||= { json: { items: items || [raw], total_count: 1, error: nil } }
    trmnl.render(view:, device:, custom_fields: fields,
                 mocks: { 'https://trmnl.bettens.dev/inaturalist/observation' => answer })
  end

  it_behaves_like 'a publishable recipe'

  %w[og_png v2].each do |device|
    it "draws every view on #{device} with the species as title, 'Seen' and no overflow" do
      views.each do |view|
        screen = draw(view, device:)
        expect(screen).to have_css('.title_bar .title', exact_text: 'Eurasian Eagle-Owl')  # exactly the name, nothing appended
        expect(screen).to have_css('.title_bar .instance', text: 'Seen')
        expect(screen).to have_css('img.image--cover')
        expect(screen).to have_no_overflow
      end
    end
  end

  it 'shows the place beside the date, except on the narrow views (date only)' do
    expect(draw('full')).to have_css('.title_bar .instance', text: 'Hilvarenbeek, NL-NB')
    %w[half_vertical quadrant].each do |view|
      expect(draw(view)).to have_no_css('.title_bar .instance', text: 'Hilvarenbeek')
    end
  end

  it 'shows the credit label for a CC BY-NC photo' do
    expect(draw('full')).to have_css('.inat-credit-box:not(.hidden)', text: 'Paul Cools · CC BY-NC')
  end

  it 'hides the credit label for CC0' do
    screen = draw('full', items: [raw(photo_license: 'cc0')])
    expect(screen).to have_no_css('.inat-credit-box:not(.hidden)')
  end

  it 'shows the QR code only when the option is on' do
    expect(draw('full', fields: { show_qr: 'true' })).to have_css('.inat-qr:not(.hidden) .qr-code, .inat-qr:not(.hidden) svg')
    expect(draw('full')).to have_no_css('.inat-qr')
  end

  it 'draws no custom style block' do
    screen = draw('full')
    expect(screen).to have_no_css('style', text: 'inat-', visible: :all)
  end

  it 'shows the empty state without a photo' do
    ['full', 'quadrant'].each do |view|
      screen = draw(view, items: [])
      expect(screen).to have_text('Nothing seen yet')
      expect(screen).to have_no_overflow
    end
  end

  it 'shows the backend error text in the empty state' do
    expect(draw('full', items: [], answer: { json: { items: [], total_count: 0, error: 'Server down' } })).to have_text('Server down')
  end

  it 'drops the date beside a long species name on the narrow views, keeps it on the wide ones' do
    long = [raw(taxon_common_name: 'Western European Herring Gull')]
    %w[half_vertical quadrant].each do |view|
      screen = draw(view, items: long)
      expect(screen).to have_css('.title_bar .title', text: 'Western European Herring Gull')
      expect(screen).to have_no_css('.title_bar .instance', text: 'Seen')
    end
    expect(draw('full', items: long)).to have_css('.title_bar .instance', text: 'Seen')
  end
end
