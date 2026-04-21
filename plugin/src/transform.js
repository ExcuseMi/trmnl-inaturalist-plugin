function transform(data) {
  const obs = data.observation || {};

  return {
    obs: {
      photo_url:   obs.photo_url || null,
      species:     obs.taxon_common_name || obs.taxon_name || null,
      scientific:  obs.taxon_name || null,
      iconic:      obs.iconic_taxon_name || null,
      location:    obs.location_name || obs.place_guess || null,
      observed_on: obs.observed_on || null,
      observer:    obs.observer_name || obs.observer_login || null,
      license:     _license(obs.photo_license),
      attribution: obs.photo_attribution || null,
    },
    total_count: data.total_count || 0,
    error: data.error || null,
  };
}

function _license(code) {
  if (!code) return null;
  const map = {
    'cc-by':        'CC BY',
    'cc-by-nc':     'CC BY-NC',
    'cc-by-sa':     'CC BY-SA',
    'cc-by-nd':     'CC BY-ND',
    'cc-by-nc-sa':  'CC BY-NC-SA',
    'cc-by-nc-nd':  'CC BY-NC-ND',
    'cc0':          'CC0',
  };
  return map[code] || code.toUpperCase();
}
