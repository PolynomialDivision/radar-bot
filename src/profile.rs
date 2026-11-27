//! The reference point and what follows from it (city, district, postal code, the NINA region),
//! resolved once and stored, so that a start never depends on the geocoder.
//!
//! At start the stored profile is used when it was resolved for the configured address. Else
//! one Nominatim search (with address details) resolves it, and the NINA region is looked up
//! for a German city. Until that succeeds the loops that need the point do not start: the
//! attempt is repeated (1 min, doubling to 10 min) and reported as failed. Every 30 days the
//! profile is resolved again; a moved point takes effect at the next start.

use std::time::Duration;

use mxbot_common::supervise::Alive;
use tracing::{info, warn};

use crate::{fetch::Http, geo::NOMINATIM_TIMEOUT, store::Db, url_encode};

/// Where the bot measures from.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HomeProfile {
    /// The configured address it was resolved for.
    pub(crate) address: String,
    pub(crate) lat: f64,
    pub(crate) lon: f64,
    pub(crate) city: Option<String>,
    /// Bezirk, or the city district / suburb where a city has no Bezirke.
    pub(crate) borough: Option<String>,
    pub(crate) postcode: Option<String>,
    pub(crate) country_code: Option<String>,
    /// NINA's region key (AGS); `None` outside Germany or where NINA does not know the city.
    pub(crate) nina_ags: Option<String>,
    pub(crate) resolved_at: i64,
}

impl HomeProfile {
    pub(crate) fn point(&self) -> (f64, f64) {
        (self.lat, self.lon)
    }
}

/// How the first resolution is repeated: the first wait, doubling up to the second.
pub(crate) const RETRY: (Duration, Duration) =
    (Duration::from_secs(60), Duration::from_secs(10 * 60));
/// How often the stored profile is resolved again.
pub(crate) const REFRESH_EVERY: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// The profile for `address`: the stored one when it was resolved for this address (no
/// request), else resolved now and stored.
pub(crate) async fn home_profile(
    db: &Db,
    http: &Http,
    address: &str,
) -> Result<HomeProfile, String> {
    match db.home_profile().await {
        Ok(Some(stored)) if stored.address == address => return Ok(stored),
        Ok(_) => {}
        Err(e) => warn!("Could not read the stored home profile: {e:#}"),
    }
    let profile = resolve(http, address).await?;
    if let Err(e) = db.store_home_profile(&profile).await {
        warn!("Could not store the home profile: {e:#}");
    }
    Ok(profile)
}

/// One Nominatim search for `address`, then the NINA region of a German city.
pub(crate) async fn resolve(http: &Http, address: &str) -> Result<HomeProfile, String> {
    let url = format!(
        "https://nominatim.openstreetmap.org/search?q={}&format=json&addressdetails=1&limit=1",
        url_encode(address)
    );
    let response = http
        .get(&url, &[("Accept-Language", "de")], NOMINATIM_TIMEOUT)
        .await
        .map_err(|e| format!("geocoder: {e}"))?;
    if !response.is_success() {
        return Err(format!("geocoder answered HTTP {}", response.status));
    }
    let json: serde_json::Value =
        serde_json::from_str(&response.body).map_err(|e| format!("geocoder answer: {e}"))?;
    let hit = json
        .as_array()
        .and_then(|hits| hits.first())
        .ok_or_else(|| format!("the geocoder does not know {address:?}"))?;
    let coordinate = |field: &str| {
        hit[field]
            .as_str()
            .and_then(|v| v.parse::<f64>().ok())
            .ok_or_else(|| format!("geocoder answer without {field}"))
    };
    let details = &hit["address"];
    let first = |fields: &[&str]| {
        fields
            .iter()
            .find_map(|field| details[*field].as_str())
            .map(str::to_owned)
    };
    let city = first(&["city", "town", "village", "municipality"]);
    let country_code = first(&["country_code"]);
    let nina_ags = match (country_code.as_deref(), city.as_deref()) {
        (Some("de"), Some(city)) => crate::alerts::nina_ags_for_city(http, city).await?,
        _ => None,
    };
    Ok(HomeProfile {
        address: address.to_owned(),
        lat: coordinate("lat")?,
        lon: coordinate("lon")?,
        borough: first(&["borough", "city_district", "suburb"]),
        postcode: first(&["postcode"]),
        city,
        country_code,
        nina_ags,
        resolved_at: chrono::Utc::now().timestamp(),
    })
}

/// Resolve the profile for `address` until it works (`retry`: the first wait and the longest),
/// reporting each attempt on `alive`, then `start` the loops with it. Afterwards the profile is
/// resolved again every [`REFRESH_EVERY`]; this runs for the bot's lifetime.
pub(crate) async fn keep_profile(
    db: Db,
    http: Http,
    address: String,
    alive: Alive,
    retry: (Duration, Duration),
    start: impl FnOnce(HomeProfile),
) {
    let (mut wait, longest) = retry;
    let profile = loop {
        match home_profile(&db, &http, &address).await {
            Ok(profile) => {
                alive.ok();
                break profile;
            }
            Err(e) => {
                warn!(
                    "Reference address not resolved ({e}); distances, weather and NINA wait, next try in {}s",
                    wait.as_secs()
                );
                alive.failed(&e);
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(longest);
            }
        }
    };
    log_profile(&profile);
    start(profile.clone());

    loop {
        tokio::time::sleep(REFRESH_EVERY).await;
        match resolve(&http, &address).await {
            Ok(fresh) => {
                if fresh.point() != profile.point() || fresh.nina_ags != profile.nina_ags {
                    info!("The home profile changed; it takes effect at the next start");
                }
                if let Err(e) = db.store_home_profile(&fresh).await {
                    warn!("Could not store the home profile: {e:#}");
                }
                alive.ok();
            }
            Err(e) => {
                warn!("Refreshing the home profile failed, keeping the stored one: {e}");
                alive.failed(&e);
            }
        }
    }
}

fn log_profile(profile: &HomeProfile) {
    info!(
        "Reference point: {:.5}, {:.5}; district {}; NINA region {}",
        profile.lat,
        profile.lon,
        profile.borough.as_deref().unwrap_or("unknown"),
        profile.nina_ags.as_deref().unwrap_or("none")
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::fetch::Recorded;
    use crate::testing::{fixture, test_db};

    const ADDRESS: &str = "Yourstreet 1, 10000 Yourcity";
    const SEARCH: &str = "https://nominatim.openstreetmap.org/search?q=Yourstreet+1";

    fn stored() -> HomeProfile {
        HomeProfile {
            address: ADDRESS.to_owned(),
            lat: 52.4736,
            lon: 13.4205,
            city: Some("Yourcity".to_owned()),
            borough: Some("YourDistrict".to_owned()),
            postcode: Some("10000".to_owned()),
            country_code: Some("xx".to_owned()),
            nina_ags: None,
            resolved_at: 1,
        }
    }

    #[tokio::test]
    async fn a_stored_profile_for_the_same_address_needs_no_request() {
        let (db, _dir) = test_db();
        db.store_home_profile(&stored()).await.unwrap();
        let recorded = Arc::new(Recorded::new());

        let profile = home_profile(&db, &Http::recorded(recorded.clone()), ADDRESS)
            .await
            .unwrap();

        assert_eq!(profile, stored());
        assert!(recorded.calls().is_empty(), "{:?}", recorded.calls());
    }

    #[tokio::test]
    async fn one_search_resolves_point_district_and_postcode() {
        let (db, _dir) = test_db();
        let mut old = stored();
        old.address = "Oldstreet 9, 10000 Yourcity".to_owned();
        db.store_home_profile(&old).await.unwrap();
        let recorded =
            Arc::new(Recorded::new().route(SEARCH, 200, &fixture("nominatim/reference.json")));

        let profile = home_profile(&db, &Http::recorded(recorded.clone()), ADDRESS)
            .await
            .unwrap();

        assert_eq!(profile.point(), (52.4736, 13.4205));
        assert_eq!(profile.borough.as_deref(), Some("YourDistrict"));
        assert_eq!(profile.postcode.as_deref(), Some("10000"));
        assert_eq!(profile.nina_ags, None, "outside Germany");
        assert_eq!(recorded.calls().len(), 1);
        assert_eq!(db.home_profile().await.unwrap().unwrap().address, ADDRESS);
    }

    #[tokio::test]
    async fn a_german_city_gets_its_nina_region() {
        let answer = fixture("nominatim/reference.json")
            .replace("\"country_code\":\"xx\"", "\"country_code\":\"de\"")
            .replace("Yourcity", "Berlin");
        let recorded = Arc::new(Recorded::new().route(SEARCH, 200, &answer));

        let profile = resolve(&Http::recorded(recorded.clone()), ADDRESS)
            .await
            .unwrap();

        // A city-state's key is built in: no request to NINA.
        assert_eq!(profile.nina_ags.as_deref(), Some("110000000000"));
        assert_eq!(recorded.calls().len(), 1);
    }

    #[tokio::test]
    async fn the_loops_start_once_a_later_attempt_succeeds() {
        let (db, _dir) = test_db();
        let recorded = Arc::new(Recorded::new().route_failing_first(
            SEARCH,
            2,
            200,
            &fixture("nominatim/reference.json"),
        ));
        let alive = mxbot_common::supervise::report("radar profile under test");
        let (started, mut was_started) = tokio::sync::mpsc::unbounded_channel();

        let task = tokio::spawn(keep_profile(
            db.clone(),
            Http::recorded(recorded.clone()),
            ADDRESS.to_owned(),
            alive,
            (Duration::from_millis(5), Duration::from_millis(20)),
            move |profile| started.send(profile).unwrap(),
        ));
        let profile = tokio::time::timeout(Duration::from_secs(5), was_started.recv())
            .await
            .expect("started")
            .unwrap();
        task.abort();

        assert_eq!(profile.borough.as_deref(), Some("YourDistrict"));
        assert_eq!(recorded.calls().len(), 3);
        let status = mxbot_common::supervise::snapshot()
            .into_iter()
            .find(|t| t.name == "radar profile under test")
            .unwrap();
        assert_eq!((status.failed_total, status.ok_total), (2, 1));
        assert!(was_started.try_recv().is_err(), "started once");
    }
}
