function transform(data) {
  var raw = data.items || [];

  var items = raw.slice(0, 4).map(function(obs) {
    return {
      photo_url:   obs.photo_url || null,
      species:     obs.taxon_common_name || obs.taxon_name || null,
      scientific:  obs.taxon_name || null,
      iconic:      obs.iconic_taxon_name || null,
      location:    obs.location_name || obs.place_guess || null,
      observed_on: obs.observed_on || null,
      observer:    obs.observer_name || obs.observer_login || null,
      license:     _license(obs.photo_license),
      inat_url:    obs.id ? 'https://www.inaturalist.org/observations/' + obs.id : null,
    };
  });

  return {
    items: items,
    total_count: data.total_count || 0,
    error: data.error || null,
  };
}

function _license(code) {
  if (!code) return null;
  var map = {
    'cc-by':       'CC BY',
    'cc-by-nc':    'CC BY-NC',
    'cc-by-sa':    'CC BY-SA',
    'cc-by-nd':    'CC BY-ND',
    'cc-by-nc-sa': 'CC BY-NC-SA',
    'cc-by-nc-nd': 'CC BY-NC-ND',
    'cc0':         'CC0',
  };
  return map[code] || code.toUpperCase();
}
